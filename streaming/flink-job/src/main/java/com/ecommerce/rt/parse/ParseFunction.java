package com.ecommerce.rt.parse;

import com.ecommerce.rt.model.Event;
import com.ecommerce.rt.model.InvalidRecord;
import com.ecommerce.rt.model.RawRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Validates raw Kafka values and splits them into the valid event stream / DLQ side output.
 *
 * <p>Malformed input can never fail the job: every parse failure increments
 * {@code invalid_records} and is emitted on {@link #INVALID_TAG}. The DLQ sink attaches
 * the source coordinates as Kafka headers.
 */
public class ParseFunction extends ProcessFunction<RawRecord, Event> {

    private static final long serialVersionUID = 1L;

    public static final OutputTag<InvalidRecord> INVALID_TAG =
            new OutputTag<>("invalid-records", TypeInformation.of(InvalidRecord.class));

    private transient Counter validCounter;
    private transient Counter invalidCounter;

    @Override
    public void open(OpenContext openContext) {
        validCounter = getRuntimeContext().getMetricGroup().counter("valid_records");
        invalidCounter = getRuntimeContext().getMetricGroup().counter("invalid_records");
    }

    @Override
    public void processElement(RawRecord record, Context ctx, Collector<Event> out) {
        ParseOutcome outcome = EventParser.parse(record.value);
        if (outcome.isValid()) {
            validCounter.inc();
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
