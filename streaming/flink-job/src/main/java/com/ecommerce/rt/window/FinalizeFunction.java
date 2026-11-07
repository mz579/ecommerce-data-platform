package com.ecommerce.rt.window;

import com.ecommerce.rt.model.DailyResult;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Emits the final, cumulative row when the watermark crosses {@code window_end + 24h}.
 *
 * <p>The window operator's cleanup timer fires at {@code window.maxTimestamp() +
 * allowedLateness} ({@code window_end + 24h - 1ms}); a separate event-time timer here at
 * exactly {@code window_end + 24h} makes the {@code is_final=1} boundary unambiguous and
 * independent of window cleanup ordering. The stored result is always the latest
 * cumulative snapshot.
 */
public class FinalizeFunction extends KeyedProcessFunction<String, DailyResult, DailyResult> {

    private static final long serialVersionUID = 1L;

    private static final String LATEST_STATE = "latest-daily-result";
    private static final String TIMER_STATE = "finalize-timer";

    private transient ValueState<DailyResult> latest;
    private transient ValueState<Long> registeredDeadline;

    @Override
    public void open(OpenContext openContext) {
        latest =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>(LATEST_STATE, DailyResult.class));
        registeredDeadline =
                getRuntimeContext().getState(new ValueStateDescriptor<>(TIMER_STATE, Long.class));
    }

    @Override
    public void processElement(DailyResult result, Context ctx, Collector<DailyResult> out)
            throws Exception {
        long watermark = ctx.timerService().currentWatermark();
        long deadline = result.windowEnd + WindowUtils.ALLOWED_LATENESS_MS;

        if (WindowUtils.isFinal(watermark, result.windowEnd)) {
            latest.clear();
            registeredDeadline.clear();
            out.collect(result.copyWith(watermark, true));
            return;
        }

        latest.update(result);
        Long existing = registeredDeadline.value();
        if (existing == null) {
            ctx.timerService().registerEventTimeTimer(deadline);
            registeredDeadline.update(deadline);
        }
        out.collect(result.copyWith(watermark, false));
    }

    @Override
    public void onTimer(
            long timestamp, OnTimerContext ctx, Collector<DailyResult> out) throws Exception {
        DailyResult result = latest.value();
        if (result == null) {
            return;
        }
        long watermark = ctx.timerService().currentWatermark();
        out.collect(result.copyWith(watermark, true));
        latest.clear();
        registeredDeadline.clear();
    }
}
