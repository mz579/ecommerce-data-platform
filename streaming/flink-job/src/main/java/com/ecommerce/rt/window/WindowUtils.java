package com.ecommerce.rt.window;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** UTC day-window helpers shared by the job and the executable self-test. */
public final class WindowUtils {

    public static final long DAY_MS = 86_400_000L;
    public static final long ALLOWED_LATENESS_MS = 24L * 60L * 60L * 1000L;

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;

    private WindowUtils() {}

    /** Floor an epoch-millis timestamp to the UTC day boundary [D 00:00, D+1 00:00). */
    public static long dayStart(long timestamp) {
        return Math.floorDiv(timestamp, DAY_MS) * DAY_MS;
    }

    public static long windowEnd(long timestamp) {
        return dayStart(timestamp) + DAY_MS;
    }

    /** Window key: the UTC date, e.g. {@code 2015-07-14}. */
    public static String dayKey(long timestamp) {
        LocalDate date = Instant.ofEpochMilli(dayStart(timestamp)).atZone(ZoneOffset.UTC).toLocalDate();
        return DATE_FORMAT.format(date);
    }

    /** {@code is_final=1} iff the current watermark has crossed window_end + 24h. */
    public static boolean isFinal(long watermark, long windowEnd) {
        return watermark >= windowEnd + ALLOWED_LATENESS_MS;
    }

    /** {@code [D, D+1)} membership check used by tests and late-event classification. */
    public static boolean inWindow(long timestamp, long windowStart) {
        return timestamp >= windowStart && timestamp < windowStart + DAY_MS;
    }

    /** Ratio with 6 decimals (matching DECIMAL(20,6)); denominator 0 yields NULL. */
    public static BigDecimal ratio(long numerator, long denominator) {
        if (denominator == 0L) {
            return null;
        }
        return BigDecimal.valueOf(numerator)
                .divide(BigDecimal.valueOf(denominator), 6, RoundingMode.HALF_UP);
    }

    public static LocalDateTime utcDateTime(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
    }

    public static LocalDate utcDate(String date) {
        return LocalDate.parse(date, DATE_FORMAT);
    }

    /** Marker for an unset event-time watermark. */
    public static boolean hasWatermark(long watermark) {
        return watermark != Long.MIN_VALUE && watermark != Long.MAX_VALUE;
    }
}
