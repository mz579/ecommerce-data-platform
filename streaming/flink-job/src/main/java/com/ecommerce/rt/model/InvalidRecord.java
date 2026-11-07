package com.ecommerce.rt.model;

import java.io.Serializable;

/** A source record that failed event_v1 validation; routed to the DLQ topic. */
public class InvalidRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    public String reason;
    public String sourceTopic;
    public int partition;
    public long offset;
    public byte[] key;
    public byte[] value;

    public InvalidRecord() {}

    public InvalidRecord(
            String reason,
            String sourceTopic,
            int partition,
            long offset,
            byte[] key,
            byte[] value) {
        this.reason = reason;
        this.sourceTopic = sourceTopic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
    }
}
