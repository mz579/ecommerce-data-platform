package com.ecommerce.rt.dedup;

import com.ecommerce.rt.model.Event;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Window-side dedup by deterministic {@code event_id} (Kafka key = visitor_id is not enough). */
public class DedupFunction extends KeyedProcessFunction<String, Event, Event> {

    private static final long serialVersionUID = 1L;

    public static final String STATE_NAME = "event-id-seen";

    private transient ValueState<Boolean> seenState;
    private transient DedupGate gate;
    private transient Counter seenCounter;
    private transient Counter dedupedCounter;

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>(STATE_NAME, Boolean.class);
        descriptor.enableTimeToLive(DedupGate.ttlConfig());
        seenState = getRuntimeContext().getState(descriptor);
        gate =
                new DedupGate(
                        new SeenState() {
                            @Override
                            public Boolean get(String key) throws Exception {
                                return seenState.value();
                            }

                            @Override
                            public void put(String key, Boolean value) throws Exception {
                                seenState.update(value);
                            }
                        });
        seenCounter = getRuntimeContext().getMetricGroup().counter("seen");
        dedupedCounter = getRuntimeContext().getMetricGroup().counter("deduped");
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
        seenCounter.inc();
        if (gate.isDuplicate(event.eventId)) {
            dedupedCounter.inc();
            return;
        }
        out.collect(event);
    }
}
