package com.ecommerce.rt;

import com.ecommerce.rt.dedup.DedupGate;
import com.ecommerce.rt.dedup.SeenState;
import com.ecommerce.rt.model.DailyResult;
import com.ecommerce.rt.model.Event;
import com.ecommerce.rt.parse.EventParser;
import com.ecommerce.rt.parse.ParseOutcome;
import com.ecommerce.rt.sink.RtJdbcSink;
import com.ecommerce.rt.window.DailyAggregator;
import com.ecommerce.rt.window.WindowUtils;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Executable self-check for the pure aggregation/dedup/window logic.
 *
 * <p>Run by {@code scripts/build_flink_job.sh}; exits non-zero on the first failed
 * assertion set. This is intentionally dependency-free (no JUnit download required).
 */
public final class SelfTest {

    private static int passed;
    private static int failed;

    private SelfTest() {}

    public static void main(String[] args) {
        testParserAndDedup();
        testNullVisitorExcludedFromUvDau();
        testOrdersUseDistinctTransactionId();
        testStrictIntersectionAndRates();
        testEmptyDenominatorYieldsNull();
        testWindowBoundaries();
        testFinalBoundary();
        testLateDuplicateDoesNotDoubleCount();
        testRateRounding();
        testItemDistinctCounts();
        testDedupTtl();
        testTransactionContract();
        testIngestTimeContract();
        testIsFinalMonotonic();

        System.out.printf("%nSELFTEST passed=%d failed=%d%n", passed, failed);
        if (failed > 0) {
            System.exit(1);
        }
        System.out.println("SELFTEST_OK");
    }

    private static void testParserAndDedup() {
        MapSeenState state = new MapSeenState();
        DedupGate gate = new DedupGate(state);
        try {
            ParseOutcome first = EventParser.parse(json("evt-1", "2015-07-14T10:00:00.000Z", 1, "view", 10, null));
            check("parse valid view", first.isValid());
            ParseOutcome second = EventParser.parse(json("evt-1", "2015-07-14T10:00:00.000Z", 1, "view", 10, null));
            check("duplicate event_id first occurrence accepted", !gate.isDuplicate(first.event.eventId));
            check("duplicate event_id second occurrence dropped", gate.isDuplicate(second.event.eventId));
            check("different event_id accepted", !gate.isDuplicate("evt-2"));
            check(
                    "invalid JSON rejected",
                    !EventParser.parse("{oops".getBytes(StandardCharsets.UTF_8)).isValid());
            check(
                    "unsupported schema_version rejected",
                    !EventParser.parse(json("evt-3", "2015-07-14T10:00:00.000Z", 1, "view", 10, null, 2))
                            .isValid());
        } catch (Exception exc) {
            check("dedup test threw " + exc, false);
        }
    }

    private static void testNullVisitorExcludedFromUvDau() {
        DailyAggregator agg = new DailyAggregator();
        agg.add(event("e1", "view", null, 10, null, "2015-07-14T10:00:00.000Z"));
        agg.add(event("e2", "view", 1L, 11, null, "2015-07-14T11:00:00.000Z"));
        DailyResult result = agg.snapshot("2015-07-14", 0L, WindowUtils.DAY_MS, 0L, false);
        check("pv includes null visitor", result.pv == 2);
        check("uv excludes null visitor", result.uv == 1);
        check("dau excludes null visitor", result.dau == 1);
    }

    private static void testOrdersUseDistinctTransactionId() {
        DailyAggregator agg = new DailyAggregator();
        agg.add(event("t1", "transaction", 1L, 10, 100L, "2015-07-14T10:00:00.000Z"));
        agg.add(event("t2", "transaction", 2L, 11, 100L, "2015-07-14T11:00:00.000Z"));
        agg.add(event("t3", "transaction", 2L, 12, 101L, "2015-07-14T12:00:00.000Z"));
        DailyResult result = agg.snapshot("2015-07-14", 0L, WindowUtils.DAY_MS, 0L, false);
        check("orders = distinct transaction_id", result.orders == 2);
        check("buyer_users = distinct visitor", result.buyerUsers == 2);
        check("dau counts transaction visitors", result.dau == 2);
    }

    private static void testStrictIntersectionAndRates() {
        DailyAggregator agg = new DailyAggregator();
        agg.add(event("s1", "view", 1L, 10, null, "2015-07-14T09:00:00.000Z"));
        agg.add(event("s2", "view", 2L, 11, null, "2015-07-14T10:00:00.000Z"));
        agg.add(event("s3", "transaction", 1L, 10, 500L, "2015-07-14T11:00:00.000Z"));
        agg.add(event("s4", "transaction", 3L, 12, 501L, "2015-07-14T12:00:00.000Z"));
        DailyResult result = agg.snapshot("2015-07-14", 0L, WindowUtils.DAY_MS, 0L, false);
        check("strict users = view ∩ transaction", result.strictUsers == 1);
        check("view_users = 2", result.viewUsers == 2);
        check("buyer_users = 2", result.buyerUsers == 2);
        check("conv_strict_rate = 1/2", equals(result.convStrictRate, "0.500000"));
        check("conv_global_rate = 2/2", equals(result.convGlobalRate, "1.000000"));
        check("add_to_cart_rate = 0/2", equals(result.addToCartRate, "0.000000"));
    }

    private static void testEmptyDenominatorYieldsNull() {
        DailyResult result =
                new DailyAggregator().snapshot("2015-07-14", 0L, WindowUtils.DAY_MS, 0L, false);
        check("empty pv", result.pv == 0);
        check("view_users = 0", result.viewUsers == 0);
        check("add_to_cart_rate NULL when denominator 0", result.addToCartRate == null);
        check("conv_strict_rate NULL when denominator 0", result.convStrictRate == null);
        check("conv_global_rate NULL when denominator 0", result.convGlobalRate == null);
    }

    private static void testWindowBoundaries() {
        long start = Instant.parse("2015-07-14T00:00:00.000Z").toEpochMilli();
        long end = start + WindowUtils.DAY_MS;
        check("dayStart floors to UTC midnight", WindowUtils.dayStart(start) == start);
        check(
                "23:59:59.999 belongs to [D, D+1)",
                WindowUtils.inWindow(end - 1L, start));
        check("D+1 00:00 not in [D, D+1)", !WindowUtils.inWindow(end, start));
        check("windowEnd = D+1 00:00", WindowUtils.windowEnd(start) == end);
        check("dayKey D", "2015-07-14".equals(WindowUtils.dayKey(end - 1L)));
        check("dayKey D+1", "2015-07-15".equals(WindowUtils.dayKey(end)));
    }

    private static void testFinalBoundary() {
        long windowEnd = Instant.parse("2015-07-16T00:00:00.000Z").toEpochMilli();
        long deadline = windowEnd + WindowUtils.ALLOWED_LATENESS_MS;
        check("is_final false 1ms before deadline", !WindowUtils.isFinal(deadline - 1L, windowEnd));
        check("is_final true exactly at deadline", WindowUtils.isFinal(deadline, windowEnd));
        check("is_final true after deadline", WindowUtils.isFinal(deadline + 1L, windowEnd));
    }

    private static void testLateDuplicateDoesNotDoubleCount() {
        MapSeenState state = new MapSeenState();
        DedupGate gate = new DedupGate(state);
        DailyAggregator agg = new DailyAggregator();
        Event first = event("dup-1", "view", 7L, 30, null, "2015-07-14T08:00:00.000Z");
        Event duplicate = event("dup-1", "view", 7L, 30, null, "2015-07-14T08:00:00.000Z");
        try {
            if (!gate.isDuplicate(first.eventId)) {
                agg.add(first);
            }
            if (!gate.isDuplicate(duplicate.eventId)) {
                agg.add(duplicate);
            }
            DailyResult result = agg.snapshot("2015-07-14", 0L, WindowUtils.DAY_MS, 0L, false);
            check("duplicate event_id counted once", result.pv == 1 && result.uv == 1);
        } catch (Exception exc) {
            check("late duplicate test threw " + exc, false);
        }
    }

    private static void testRateRounding() {
        DailyAggregator agg = new DailyAggregator();
        agg.add(event("r1", "view", 1L, 10, null, "2015-07-14T08:00:00.000Z"));
        agg.add(event("r2", "view", 2L, 11, null, "2015-07-14T08:01:00.000Z"));
        agg.add(event("r3", "view", 3L, 12, null, "2015-07-14T08:02:00.000Z"));
        agg.add(event("r4", "add_to_cart", 1L, 10, null, "2015-07-14T08:03:00.000Z"));
        DailyResult result = agg.snapshot("2015-07-14", 0L, WindowUtils.DAY_MS, 0L, false);
        check("1/3 rounds to 6 decimals", equals(result.addToCartRate, "0.333333"));
    }

    private static void testItemDistinctCounts() {
        DailyAggregator agg = new DailyAggregator();
        agg.add(event("i1", "view", 1L, 10, null, "2015-07-14T08:00:00.000Z"));
        agg.add(event("i2", "view", 2L, 10, null, "2015-07-14T08:01:00.000Z"));
        agg.add(event("i3", "add_to_cart", 1L, 10, null, "2015-07-14T08:02:00.000Z"));
        agg.add(event("i4", "transaction", 1L, 11, 900L, "2015-07-14T08:03:00.000Z"));
        DailyResult result = agg.snapshot("2015-07-14", 0L, WindowUtils.DAY_MS, 0L, false);
        check("items_viewed distinct", result.itemsViewed == 1);
        check("items_carted distinct", result.itemsCarted == 1);
        check("items_bought distinct", result.itemsBought == 1);
    }

    private static void testDedupTtl() {
        check("dedup TTL constant is 48h", DedupGate.TTL.equals(Duration.ofHours(48)));
        check(
                "dedup TTL config is 48h",
                DedupGate.ttlConfig().getTtl().toMilliseconds()
                        == Duration.ofHours(48).toMillis());
    }

    private static void testTransactionContract() {
        check(
                "transaction without transaction_id rejected",
                !EventParser.parse(json("x1", "2015-07-14T10:00:00.000Z", 1, "transaction", 10, null))
                        .isValid());
        check(
                "transaction_id=0 accepted (dataset has one transactionid=0 row)",
                EventParser.parse(
                                json(
                                        "x0",
                                        "2015-06-15T21:36:37.081Z",
                                        1,
                                        "transaction",
                                        10,
                                        0L))
                        .isValid());
        check(
                "negative transaction_id rejected",
                !EventParser.parse(json("x4", "2015-07-14T10:00:00.000Z", 1, "transaction", 10, -1L))
                        .isValid());
        check(
                "non-transaction with transaction_id rejected",
                !EventParser.parse(json("x2", "2015-07-14T10:00:00.000Z", 1, "view", 10, 5L))
                        .isValid());
        check(
                "non-UTC event_time rejected",
                !EventParser.parse(
                                ("{\"event_id\":\"x3\","
                                                + "\"event_time\":\"2015-07-14T10:00:00.000+08:00\","
                                                + "\"schema_version\":1,\"visitor_id\":1,"
                                                + "\"event_type\":\"view\",\"item_id\":10,"
                                                + "\"transaction_id\":null}")
                                        .getBytes(StandardCharsets.UTF_8))
                        .isValid());
    }

    private static void testIngestTimeContract() {
        ParseOutcome valid =
                EventParser.parse(json("ingest-ok", "2015-07-14T10:00:00.000Z", 1, "view", 10, null));
        check(
                "ingest_time parsed to epoch millis",
                valid.isValid()
                        && valid.event.ingestTime
                                == Instant.parse("2026-11-07T00:00:00.000Z").toEpochMilli());
        check(
                "missing ingest_time rejected",
                !EventParser.parse(
                                json(
                                        "ingest-missing",
                                        "2015-07-14T10:00:00.000Z",
                                        1,
                                        "view",
                                        10,
                                        null,
                                        1,
                                        null))
                        .isValid());
        check(
                "non-UTC ingest_time rejected",
                !EventParser.parse(
                                json(
                                        "ingest-offset",
                                        "2015-07-14T10:00:00.000Z",
                                        1,
                                        "view",
                                        10,
                                        null,
                                        1,
                                        "2026-11-07T08:00:00.000+08:00"))
                        .isValid());
        check(
                "malformed ingest_time rejected",
                !EventParser.parse(
                                json(
                                        "ingest-bad",
                                        "2015-07-14T10:00:00.000Z",
                                        1,
                                        "view",
                                        10,
                                        null,
                                        1,
                                        "2026-11-07T25:00:00.000Z"))
                        .isValid());
    }

    private static void testIsFinalMonotonic() {
        // In-memory guard: copyWith must never downgrade an object that is already final.
        DailyResult result = new DailyResult();
        result.eventDate = "2015-07-19";
        result.windowStart = 0L;
        result.windowEnd = WindowUtils.DAY_MS;
        result.sourceWatermark = 1000L;
        result.isFinal = true;

        DailyResult downgraded = result.copyWith(500L, false);
        check("copyWith keeps is_final=true when finalFlag=false", downgraded.isFinal);
        check("copyWith keeps the larger watermark", downgraded.sourceWatermark == 1000L);

        DailyResult upgraded = result.copyWith(2000L, true);
        check("copyWith stays final when finalFlag=true", upgraded.isFinal);

        // Cross-write guard: the upsert SQL must be monotonic for is_final, exactly like
        // source_watermark. Plain "is_final = VALUES(is_final)" lets an out-of-order write
        // for the same day downgrade a final row, leaving the contradictory state
        // "watermark past day_end+lateness but is_final=0" (observed on 2015-07-19 /
        // 2015-07-25 during the 9.4 demo replays).
        String[] statements = {
            RtJdbcSink.TRAFFIC_SQL, RtJdbcSink.FUNNEL_SQL, RtJdbcSink.ITEM_SQL
        };
        String[] names = {"traffic", "funnel", "item"};
        for (int i = 0; i < statements.length; i++) {
            String sql = statements[i];
            check(
                    names[i] + " upsert makes is_final monotonic",
                    sql.contains("is_final = GREATEST(is_final, VALUES(is_final))"));
            check(
                    names[i] + " upsert never assigns is_final from VALUES alone",
                    !sql.contains("is_final = VALUES(is_final)"));
        }
        check(
                "traffic upsert keeps source_watermark monotonic",
                RtJdbcSink.TRAFFIC_SQL.contains(
                        "source_watermark = GREATEST(COALESCE(source_watermark, '1970-01-01'), "
                                + "VALUES(source_watermark))"));
        check(
                "funnel upsert keeps source_watermark monotonic",
                RtJdbcSink.FUNNEL_SQL.contains(
                        "source_watermark = GREATEST(COALESCE(source_watermark, '1970-01-01'), "
                                + "VALUES(source_watermark))"));
    }

    private static byte[] json(
            String eventId,
            String eventTime,
            Integer visitorId,
            String eventType,
            long itemId,
            Long transactionId) {
        return json(eventId, eventTime, visitorId, eventType, itemId, transactionId, 1);
    }

    private static byte[] json(
            String eventId,
            String eventTime,
            Integer visitorId,
            String eventType,
            long itemId,
            Long transactionId,
            int schemaVersion) {
        return json(
                eventId,
                eventTime,
                visitorId,
                eventType,
                itemId,
                transactionId,
                schemaVersion,
                "2026-11-07T00:00:00.000Z");
    }

    private static byte[] json(
            String eventId,
            String eventTime,
            Integer visitorId,
            String eventType,
            long itemId,
            Long transactionId,
            int schemaVersion,
            String ingestTime) {
        String ingestField =
                ingestTime == null ? "" : "\"ingest_time\":\"" + ingestTime + "\",";
        String body =
                "{"
                        + "\"event_id\":\""
                        + eventId
                        + "\","
                        + "\"event_time\":\""
                        + eventTime
                        + "\","
                        + ingestField
                        + "\"schema_version\":"
                        + schemaVersion
                        + ","
                        + "\"visitor_id\":"
                        + (visitorId == null ? "null" : visitorId)
                        + ","
                        + "\"event_type\":\""
                        + eventType
                        + "\","
                        + "\"item_id\":"
                        + itemId
                        + ","
                        + "\"transaction_id\":"
                        + (transactionId == null ? "null" : transactionId)
                        + "}";
        return body.getBytes(StandardCharsets.UTF_8);
    }

    private static Event event(
            String id,
            String type,
            Long visitorId,
            long itemId,
            Long transactionId,
            String eventTime) {
        return new Event(
                id,
                Instant.parse(eventTime).toEpochMilli(),
                Instant.parse("2026-11-07T00:00:00.000Z").toEpochMilli(),
                visitorId,
                type,
                itemId,
                transactionId,
                "{}");
    }

    private static boolean equals(BigDecimal actual, String expected) {
        return actual != null && actual.compareTo(new BigDecimal(expected)) == 0;
    }

    private static void check(String name, boolean condition) {
        if (condition) {
            passed++;
            System.out.println("PASS " + name);
        } else {
            failed++;
            System.out.println("FAIL " + name);
        }
    }

    /** In-memory SeenState used only by this self-test. */
    private static final class MapSeenState implements SeenState {
        private final Map<String, Boolean> values = new HashMap<>();

        @Override
        public Boolean get(String key) {
            return values.get(key);
        }

        @Override
        public void put(String key, Boolean value) {
            values.put(key, value);
        }
    }
}
