package com.ecommerce.rt.model;

import java.io.Serializable;

/**
 * A validated event_v1 message after JSON parsing.
 *
 * <p>Flink POJO: public fields + public no-arg constructor. {@code eventTime} is epoch
 * milliseconds in UTC (the only time field used for windows/watermarks). {@code rawJson}
 * keeps the original payload so the too-late side output can be persisted verbatim.
 */
public class Event implements Serializable {

    private static final long serialVersionUID = 1L;

    public String eventId;
    public long eventTime;
    public Long visitorId;
    public String eventType;
    public long itemId;
    public Long transactionId;
    public String rawJson;

    public Event() {}

    public Event(
            String eventId,
            long eventTime,
            Long visitorId,
            String eventType,
            long itemId,
            Long transactionId,
            String rawJson) {
        this.eventId = eventId;
        this.eventTime = eventTime;
        this.visitorId = visitorId;
        this.eventType = eventType;
        this.itemId = itemId;
        this.transactionId = transactionId;
        this.rawJson = rawJson;
    }

    public boolean isView() {
        return "view".equals(eventType);
    }

    public boolean isAddToCart() {
        return "add_to_cart".equals(eventType);
    }

    public boolean isTransaction() {
        return "transaction".equals(eventType);
    }
}
