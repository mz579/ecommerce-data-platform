package com.ecommerce.rt.model;

import java.io.Serializable;

/** Raw Kafka value plus source coordinates, before schema validation. */
public class RawRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    public byte[] value;
    public byte[] key;
    public String topic;
    public int partition;
    public long offset;
    public long kafkaTimestamp;

    public RawRecord() {}

    public RawRecord(
            byte[] value,
            byte[] key,
            String topic,
            int partition,
            long offset,
            long kafkaTimestamp) {
        this.value = value;
        this.key = key;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.kafkaTimestamp = kafkaTimestamp;
    }
}
