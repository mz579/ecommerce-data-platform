package com.ecommerce.rt.dedup;

/**
 * Minimal key/value view over the dedup store.
 *
 * <p>The production adapter wraps Flink {@code ValueState<Boolean>}; the self-test uses a
 * plain map, so both exercise the same {@link DedupGate} decision logic.
 */
public interface SeenState {

    Boolean get(String key) throws Exception;

    void put(String key, Boolean value) throws Exception;
}
