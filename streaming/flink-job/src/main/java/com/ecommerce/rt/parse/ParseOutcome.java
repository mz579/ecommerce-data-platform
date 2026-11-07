package com.ecommerce.rt.parse;

import com.ecommerce.rt.model.Event;

/** Result of parsing one Kafka value: exactly one of {@code event} / {@code reason} is set. */
public final class ParseOutcome {

    public final Event event;
    public final String reason;

    private ParseOutcome(Event event, String reason) {
        this.event = event;
        this.reason = reason;
    }

    public static ParseOutcome ok(Event event) {
        return new ParseOutcome(event, null);
    }

    public static ParseOutcome invalid(String reason) {
        return new ParseOutcome(null, reason);
    }

    public boolean isValid() {
        return event != null;
    }
}
