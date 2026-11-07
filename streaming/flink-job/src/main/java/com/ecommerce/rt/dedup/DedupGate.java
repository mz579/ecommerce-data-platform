package com.ecommerce.rt.dedup;

import java.time.Duration;
import org.apache.flink.api.common.state.StateTtlConfig;

/**
 * Pure dedup decision: first occurrence of an {@code event_id} wins, later ones are dropped.
 *
 * <p>Production state carries a 48h processing-time TTL (>= window + allowed lateness), so
 * a replay of the same DWD {@code dedup_key} is absorbed even across job restarts while
 * still bounding state growth.
 */
public final class DedupGate {

    public static final Duration TTL = Duration.ofHours(48);

    private final SeenState state;

    public DedupGate(SeenState state) {
        this.state = state;
    }

    /** @return true when this event_id was already seen (and must not enter the window). */
    public boolean isDuplicate(String eventId) throws Exception {
        if (state.get(eventId) != null) {
            return true;
        }
        state.put(eventId, Boolean.TRUE);
        return false;
    }

    /** TTL configuration shared by production and asserted by the self-test. */
    public static StateTtlConfig ttlConfig() {
        return StateTtlConfig.newBuilder(TTL)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
    }
}
