package com.ecommerce.rt.window;

import com.ecommerce.rt.model.DailyResult;
import com.ecommerce.rt.model.Event;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * UTC day-window aggregation: {@code [D 00:00, D+1 00:00)} with 24h allowed lateness.
 *
 * <p>The window operator buffers all raw events; each firing (initial fire, plus one per
 * late element within the allowed lateness) feeds {@link DailyAggregator} from scratch and
 * emits a cumulative {@link DailyResult}. Late elements beyond the lateness window are
 * routed by the job to {@code rt_late_events}.
 */
public class DailyWindowFunction
        extends ProcessWindowFunction<Event, DailyResult, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(
            String dayKey,
            Context context,
            Iterable<Event> elements,
            Collector<DailyResult> out) {
        DailyAggregator aggregator = new DailyAggregator();
        for (Event event : elements) {
            aggregator.add(event);
        }
        long watermark = context.currentWatermark();
        long windowEnd = context.window().getEnd();
        out.collect(
                aggregator.snapshot(
                        dayKey,
                        context.window().getStart(),
                        windowEnd,
                        watermark,
                        WindowUtils.isFinal(watermark, windowEnd)));
    }
}
