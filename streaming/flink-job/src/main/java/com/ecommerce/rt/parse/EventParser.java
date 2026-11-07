package com.ecommerce.rt.parse;

import com.ecommerce.rt.model.Event;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Pure event_v1 parser/validator (no Flink runtime dependency).
 *
 * <p>Uses Flink's shaded Jackson so the job jar needs no extra JSON library. Contract
 * reference: {@code streaming/schemas/event_v1.json} and docs/streaming_design.md §3.
 * Every failure returns a stable {@code reason} used as the DLQ {@code error_reason}
 * header; valid messages never throw.
 */
public final class EventParser {

    public static final int SUPPORTED_SCHEMA_VERSION = 1;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Set<String> EVENT_TYPES = Set.of("view", "add_to_cart", "transaction");
    private static final Pattern EVENT_ID_PATTERN = Pattern.compile("[A-Za-z0-9._:-]{1,128}");

    private EventParser() {}

    public static ParseOutcome parse(byte[] raw) {
        if (raw == null) {
            return ParseOutcome.invalid("invalid_payload: null value");
        }
        String text;
        try {
            text =
                    StandardCharsets.UTF_8
                            .newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .decode(ByteBuffer.wrap(raw))
                            .toString();
        } catch (CharacterCodingException exc) {
            return ParseOutcome.invalid("invalid_utf8: " + exc.getClass().getSimpleName());
        }

        JsonNode root;
        try {
            root = MAPPER.readTree(text);
        } catch (Exception exc) {
            return ParseOutcome.invalid("invalid_json: " + firstLine(exc.getMessage()));
        }
        if (root == null || !root.isObject()) {
            return ParseOutcome.invalid("invalid_payload: not a JSON object");
        }

        JsonNode version = root.get("schema_version");
        if (version == null || !version.isIntegralNumber()) {
            return ParseOutcome.invalid("unsupported_schema_version: missing");
        }
        if (version.asInt() != SUPPORTED_SCHEMA_VERSION) {
            return ParseOutcome.invalid("unsupported_schema_version: " + version.asInt());
        }

        JsonNode eventIdNode = root.get("event_id");
        if (eventIdNode == null || !eventIdNode.isTextual()) {
            return ParseOutcome.invalid("schema: event_id: not a string");
        }
        String eventId = eventIdNode.asText();
        if (!EVENT_ID_PATTERN.matcher(eventId).matches()) {
            return ParseOutcome.invalid("schema: event_id: invalid format");
        }

        JsonNode eventTimeNode = root.get("event_time");
        if (eventTimeNode == null || !eventTimeNode.isTextual()) {
            return ParseOutcome.invalid("schema: event_time: not a string");
        }
        String eventTimeText = eventTimeNode.asText();
        if (!eventTimeText.endsWith("Z")) {
            return ParseOutcome.invalid("schema: event_time: must be UTC (Z suffix)");
        }
        Instant eventTime;
        try {
            eventTime = Instant.parse(eventTimeText);
        } catch (DateTimeParseException exc) {
            return ParseOutcome.invalid("schema: event_time: not an ISO-8601 datetime");
        }

        // ingest_time is required by the event_v1 contract but never used for windows.
        // It only feeds the ingest_latency_ms monitoring histogram (docs §3.2, §9).
        JsonNode ingestTimeNode = root.get("ingest_time");
        if (ingestTimeNode == null || !ingestTimeNode.isTextual()) {
            return ParseOutcome.invalid("schema: ingest_time: not a string");
        }
        String ingestTimeText = ingestTimeNode.asText();
        if (!ingestTimeText.endsWith("Z")) {
            return ParseOutcome.invalid("schema: ingest_time: must be UTC (Z suffix)");
        }
        Instant ingestTime;
        try {
            ingestTime = Instant.parse(ingestTimeText);
        } catch (DateTimeParseException exc) {
            return ParseOutcome.invalid("schema: ingest_time: not an ISO-8601 datetime");
        }

        Long visitorId;
        JsonNode visitorNode = root.get("visitor_id");
        if (visitorNode == null || visitorNode.isNull()) {
            visitorId = null;
        } else {
            if (!visitorNode.canConvertToLong()) {
                return ParseOutcome.invalid("schema: visitor_id: not an integer");
            }
            long value = visitorNode.asLong();
            if (value < 1L) {
                return ParseOutcome.invalid("schema: visitor_id: must be >= 1");
            }
            visitorId = value;
        }

        JsonNode typeNode = root.get("event_type");
        if (typeNode == null || !typeNode.isTextual()) {
            return ParseOutcome.invalid("schema: event_type: not a string");
        }
        String eventType = typeNode.asText();
        if (!EVENT_TYPES.contains(eventType)) {
            return ParseOutcome.invalid("schema: event_type: unsupported value");
        }

        JsonNode itemNode = root.get("item_id");
        if (itemNode == null || !itemNode.canConvertToLong()) {
            return ParseOutcome.invalid("schema: item_id: not an integer");
        }
        long itemId = itemNode.asLong();
        if (itemId < 1L) {
            return ParseOutcome.invalid("schema: item_id: must be >= 1");
        }

        JsonNode transactionNode = root.get("transaction_id");
        Long transactionId;
        if (transactionNode == null || transactionNode.isNull()) {
            transactionId = null;
        } else {
            if (!transactionNode.canConvertToLong()) {
                return ParseOutcome.invalid("schema: transaction_id: not an integer");
            }
            transactionId = transactionNode.asLong();
            // 0 is a legitimate order id: the source dataset has exactly one
            // transactionid=0 row (events.csv line 271795) and offline ADS counts it.
            if (transactionId < 0L) {
                return ParseOutcome.invalid("schema: transaction_id: must be >= 0");
            }
        }
        if ("transaction".equals(eventType) && transactionId == null) {
            return ParseOutcome.invalid("schema: transaction_id: required for transaction");
        }
        if (!"transaction".equals(eventType) && transactionId != null) {
            return ParseOutcome.invalid("schema: transaction_id: must be null for non-transaction");
        }

        return ParseOutcome.ok(
                new Event(
                        eventId,
                        eventTime.toEpochMilli(),
                        ingestTime.toEpochMilli(),
                        visitorId,
                        eventType,
                        itemId,
                        transactionId,
                        text));
    }

    private static String firstLine(String message) {
        if (message == null || message.isEmpty()) {
            return "malformed JSON";
        }
        int newline = message.indexOf('\n');
        return newline >= 0 ? message.substring(0, newline) : message;
    }
}
