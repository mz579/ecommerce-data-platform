package com.ecommerce.rt.model;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * One day-window aggregate, written as a single JDBC transaction to the three RT tables.
 *
 * <p>All values are cumulative for the whole window (never incremental), so a late
 * re-fire or a checkpoint replay simply overwrites the previous row via upsert.
 */
public class DailyResult implements Serializable {

    private static final long serialVersionUID = 1L;

    public String eventDate;
    public long pv;
    public long uv;
    public long dau;
    public long viewUsers;
    public long cartUsers;
    public long buyerUsers;
    public long orders;
    public long strictUsers;
    public BigDecimal addToCartRate;
    public BigDecimal convStrictRate;
    public BigDecimal convGlobalRate;
    public long itemsViewed;
    public long itemsCarted;
    public long itemsBought;
    public long windowStart;
    public long windowEnd;
    public long sourceWatermark;
    public boolean isFinal;

    public DailyResult() {}

    /** Returns a shallow copy with a newer watermark / final flag (source record is immutable). */
    public DailyResult copyWith(long watermark, boolean finalFlag) {
        DailyResult copy = new DailyResult();
        copy.eventDate = eventDate;
        copy.pv = pv;
        copy.uv = uv;
        copy.dau = dau;
        copy.viewUsers = viewUsers;
        copy.cartUsers = cartUsers;
        copy.buyerUsers = buyerUsers;
        copy.orders = orders;
        copy.strictUsers = strictUsers;
        copy.addToCartRate = addToCartRate;
        copy.convStrictRate = convStrictRate;
        copy.convGlobalRate = convGlobalRate;
        copy.itemsViewed = itemsViewed;
        copy.itemsCarted = itemsCarted;
        copy.itemsBought = itemsBought;
        copy.windowStart = windowStart;
        copy.windowEnd = windowEnd;
        copy.sourceWatermark = Math.max(sourceWatermark, watermark);
        copy.isFinal = finalFlag || isFinal;
        return copy;
    }
}
