package com.ecommerce.rt.parse;

import com.ecommerce.rt.model.Event;
import com.ecommerce.rt.model.InvalidRecord;
import com.ecommerce.rt.model.RawRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Validates raw Kafka values and splits them into the valid event stream / DLQ side output.
 *
 * <p>Malformed input can never fail the job: every parse failure increments
 * {@code invalid_records} and is emitted on {@link #INVALID_TAG}. The DLQ sink attaches
 * the source coordinates as Kafka headers.
 *
 * <p>Phase 9.4 adds one observability-only metric: for every valid event,
 * {@code ingest_latency_ms = max(0, processing_time - ingest_time)} is recorded in a
 * {@link DescriptiveStatisticsHistogram}. The histogram's p50/p95 are also exposed as
 * gauges so they can be read directly from the Flink REST metric API. This does not
 * change parsing validity, windows, dedup or sink semantics.
 */
public class ParseFunction extends ProcessFunction<RawRecord, Event> {

    private static final long serialVersionUID = 1L;

    public static final OutputTag<InvalidRecord> INVALID_TAG =
            new OutputTag<>("invalid-records", TypeInformation.of(InvalidRecord.class));

    /** Rolling window of the most recent latency samples exposed through Flink metrics. */
    static final int LATENCY_HISTOGRAM_WINDOW = 10_000;

    private transient Counter validCounter;
    private transient Counter invalidCounter;
    private transient Histogram ingestLatency;

    @Override
    public void open(OpenContext openContext) {
        validCounter = getRuntimeContext().getMetricGroup().counter("valid_records");
        invalidCounter = getRuntimeContext().getMetricGroup().counter("invalid_records");
        ingestLatency =
                getRuntimeContext()
                        .getMetricGroup()
                        .histogram(
                                "ingest_latency_ms",
                                new DescriptiveStatisticsHistogram(LATENCY_HISTOGRAM_WINDOW));
        getRuntimeContext()
                .getMetricGroup()
                .gauge("ingest_latency_p50_ms", (Gauge<Double>) () -> quantile(0.50));
        getRuntimeContext()
                .getMetricGroup()
                .gauge("ingest_latency_p95_ms", (Gauge<Double>) () -> quantile(0.95));
    }

    private double quantile(double q) {
        if (ingestLatency == null) {
            return Double.NaN;
        }
        HistogramStatistics statistics = ingestLatency.getStatistics();
        if (statistics == null || statistics.size() == 0) {
            return Double.NaN;
        }
        return statistics.getQuantile(q);
    }

    @Override
    public void processElement(RawRecord record, Context ctx, Collector<Event> out) {
        ParseOutcome outcome = EventParser.parse(record.value);
        if (outcome.isValid()) {
            validCounter.inc();
            long latencyMs = Math.max(0L, System.currentTimeMillis() - outcome.event.ingestTime);
            ingestLatency.update(latencyMs);
            out.collect(outcome.event);
            return;
        }
        invalidCounter.inc();
        ctx.output(
                INVALID_TAG,
                new InvalidRecord(
                        outcome.reason,
                        record.topic,
                        record.partition,
                        record.offset,
                        record.key,
                        record.value));
    }
}
