package com.ecommerce.rt.window;

import com.ecommerce.rt.model.Event;
import com.ecommerce.rt.model.LateEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Enriches the window's too-late side output with lateness/audit metadata and counts it. */
public class LateEventProcessFunction extends ProcessFunction<Event, LateEvent> {

    private static final long serialVersionUID = 1L;

    private transient Counter lateCounter;

    @Override
    public void open(OpenContext openContext) {
        lateCounter = getRuntimeContext().getMetricGroup().counter("late_events");
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<LateEvent> out) {
        long watermark = ctx.timerService().currentWatermark();
        long lateness = Math.max(0L, watermark - event.eventTime);
        lateCounter.inc();
        out.collect(
                new LateEvent(
                        event.eventId,
                        event.eventTime,
                        System.currentTimeMillis(),
                        lateness,
                        "beyond_allowed_lateness",
                        event.rawJson));
    }
}
