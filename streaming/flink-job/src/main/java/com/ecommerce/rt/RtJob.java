package com.ecommerce.rt;

import com.ecommerce.rt.config.JobConfig;
import com.ecommerce.rt.dedup.DedupFunction;
import com.ecommerce.rt.model.DailyResult;
import com.ecommerce.rt.model.Event;
import com.ecommerce.rt.model.InvalidRecord;
import com.ecommerce.rt.model.LateEvent;
import com.ecommerce.rt.model.RawRecord;
import com.ecommerce.rt.parse.ParseFunction;
import com.ecommerce.rt.sink.DlqRecordSerializer;
import com.ecommerce.rt.sink.LateJdbcSink;
import com.ecommerce.rt.sink.RtJdbcSink;
import com.ecommerce.rt.source.RawKafkaDeserializer;
import com.ecommerce.rt.window.DailyWindowFunction;
import com.ecommerce.rt.window.FinalizeFunction;
import com.ecommerce.rt.window.LateEventProcessFunction;
import com.ecommerce.rt.window.WindowUtils;
import java.time.Duration;
import java.util.Arrays;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase 9.3 realtime job: Kafka event_v1 → event-time UTC day windows → ecommerce_rt upsert.
 *
 * <p>Pipeline (docs/streaming_design.md §5–§7):
 *
 * <pre>
 * KafkaSource(raw) -> parse/validate --invalid--> DLQ topic
 *                              \--valid--> watermarks -> dedup(event_id) -> 1d windows
 *                                                              |--late--> rt_late_events
 *                                                              \--> finalize -> MySQL upsert
 * </pre>
 */
public final class RtJob {

    public static final String JOB_NAME = "ecommerce-rt-daily-metrics";

    private static final Logger LOG = LoggerFactory.getLogger(RtJob.class);

    private static final OutputTag<Event> LATE_TAG =
            new OutputTag<>("late-events", TypeInformation.of(Event.class));

    private RtJob() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && ("--help".equals(args[0]) || "-h".equals(args[0]))) {
            System.out.println(JobConfig.usage());
            return;
        }

        JobConfig config;
        try {
            config = JobConfig.parse(args);
        } catch (IllegalArgumentException exc) {
            System.err.println("[error] " + exc.getMessage());
            System.err.println(JobConfig.usage());
            System.exit(2);
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.parallelism);
        env.getConfig().setAutoWatermarkInterval(200L);
        // Explicit restart policy: up to 3 attempts, 10s apart, then fail the job.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));

        CheckpointConfig checkpoint = env.getCheckpointConfig();
        LOG.info(
                "inherited checkpoint config: interval={}ms mode={} minPause={}ms timeout={}ms "
                        + "externalized={}",
                checkpoint.getCheckpointInterval(),
                checkpoint.getCheckpointingMode(),
                checkpoint.getMinPauseBetweenCheckpoints(),
                checkpoint.getCheckpointTimeout(),
                checkpoint.getExternalizedCheckpointCleanup());
        if (checkpoint.getCheckpointingMode() != CheckpointingMode.EXACTLY_ONCE) {
            LOG.warn("cluster checkpoint mode is not EXACTLY_ONCE");
        }
        LOG.info(
                "job config: bootstrap={} sourceTopic={} dlqTopic={} group={} offsets={} "
                        + "parallelism={} sinkEnabled={}",
                config.bootstrapServers,
                config.sourceTopic,
                config.dlqTopic,
                config.groupId,
                config.startingOffsets,
                config.parallelism,
                config.sinkEnabled);

        KafkaSource<RawRecord> source =
                KafkaSource.<RawRecord>builder()
                        .setBootstrapServers(config.bootstrapServers)
                        .setTopics(config.sourceTopic)
                        .setGroupId(config.groupId)
                        .setStartingOffsets(config.offsetsInitializer())
                        .setDeserializer(new RawKafkaDeserializer())
                        .setProperty("commit.offsets.on.checkpoint", "true")
                        .setProperty("partition.discovery.interval.ms", "60000")
                        .build();

        DataStream<RawRecord> raw =
                env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "kafka-source-" + config.sourceTopic);

        SingleOutputStreamOperator<Event> parsed =
                raw.process(new ParseFunction()).name("parse-validate-event-v1").uid("parse-validate");

        DataStream<InvalidRecord> invalid = parsed.getSideOutput(ParseFunction.INVALID_TAG);
        KafkaSink<InvalidRecord> dlqSink =
                KafkaSink.<InvalidRecord>builder()
                        .setBootstrapServers(config.bootstrapServers)
                        .setRecordSerializer(new DlqRecordSerializer(config.dlqTopic))
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setProperty("acks", "all")
                        .build();
        invalid
                .sinkTo(dlqSink)
                .name("dlq-kafka-sink")
                .uid("dlq-kafka-sink")
                .setParallelism(config.parallelism);

        WatermarkStrategy<Event> watermarks =
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                        .withIdleness(Duration.ofMinutes(5))
                        .withTimestampAssigner((event, timestamp) -> event.eventTime);

        DataStream<Event> eventTimeStream =
                parsed.assignTimestampsAndWatermarks(watermarks)
                        .name("event-time-watermarks")
                        .uid("event-time-watermarks");

        DataStream<Event> deduped =
                eventTimeStream
                        .keyBy(event -> event.eventId)
                        .process(new DedupFunction())
                        .name("dedup-by-event-id")
                        .uid("dedup-by-event-id");

        SingleOutputStreamOperator<DailyResult> windowed =
                deduped.keyBy(event -> WindowUtils.dayKey(event.eventTime))
                        .window(TumblingEventTimeWindows.of(Duration.ofDays(1)))
                        .allowedLateness(Duration.ofHours(24))
                        .sideOutputLateData(LATE_TAG)
                        .process(new DailyWindowFunction())
                        .name("daily-window-aggregate")
                        .uid("daily-window-aggregate");

        DataStream<DailyResult> finalized =
                windowed.keyBy(result -> result.eventDate)
                        .process(new FinalizeFunction())
                        .name("finalize-window-results")
                        .uid("finalize-window-results");

        DataStream<LateEvent> lateEvents =
                windowed.getSideOutput(LATE_TAG)
                        .process(new LateEventProcessFunction())
                        .name("late-event-audit")
                        .uid("late-event-audit");

        if (config.sinkEnabled) {
            finalized
                    .addSink(new RtJdbcSink())
                    .name("mysql-rt-upsert")
                    .uid("mysql-rt-upsert")
                    .setParallelism(config.parallelism);
            lateEvents
                    .addSink(new LateJdbcSink())
                    .name("mysql-late-events")
                    .uid("mysql-late-events")
                    .setParallelism(config.parallelism);
        }

        LOG.info("submitting job {} args={}", JOB_NAME, Arrays.toString(args));
        env.execute(JOB_NAME);
    }
}
