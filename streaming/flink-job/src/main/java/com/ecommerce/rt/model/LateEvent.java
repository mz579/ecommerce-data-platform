package com.ecommerce.rt.model;

import java.io.Serializable;

/** An event that arrived after the window's allowed lateness; persisted to rt_late_events. */
public class LateEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    public String eventId;
    public long eventTime;
    public long detectedAt;
    public long latenessMs;
    public String reason;
    public String payload;

    public LateEvent() {}

    public LateEvent(
            String eventId,
            long eventTime,
            long detectedAt,
            long latenessMs,
            String reason,
            String payload) {
        this.eventId = eventId;
        this.eventTime = eventTime;
        this.detectedAt = detectedAt;
        this.latenessMs = latenessMs;
        this.reason = reason;
        this.payload = payload;
    }
}
