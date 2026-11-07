package com.ecommerce.rt.window;

import com.ecommerce.rt.model.DailyResult;
import com.ecommerce.rt.model.Event;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Pure, cumulative daily aggregation. There is no incremental update path: every emission
 * rebuilds the whole window from the buffered elements, which is exactly what the
 * late-arrival contract requires (late re-fires must publish cumulative values).
 */
public final class DailyAggregator implements Serializable {

    private static final long serialVersionUID = 1L;

    private long pv;
    private final Set<Long> viewUsers = new HashSet<>();
    private final Set<Long> activeUsers = new HashSet<>();
    private final Set<Long> cartUsers = new HashSet<>();
    private final Set<Long> buyerUsers = new HashSet<>();
    private final Set<Long> transactionIds = new HashSet<>();
    private final Set<Long> viewedItems = new HashSet<>();
    private final Set<Long> cartedItems = new HashSet<>();
    private final Set<Long> boughtItems = new HashSet<>();

    public void add(Event event) {
        if (event.visitorId != null) {
            activeUsers.add(event.visitorId);
        }
        if (event.isView()) {
            pv++;
            if (event.visitorId != null) {
                viewUsers.add(event.visitorId);
            }
            viewedItems.add(event.itemId);
        } else if (event.isAddToCart()) {
            if (event.visitorId != null) {
                cartUsers.add(event.visitorId);
            }
            cartedItems.add(event.itemId);
        } else if (event.isTransaction()) {
            if (event.visitorId != null) {
                buyerUsers.add(event.visitorId);
            }
            if (event.transactionId != null) {
                transactionIds.add(event.transactionId);
            }
            boughtItems.add(event.itemId);
        }
    }

    public long pv() {
        return pv;
    }

    public long viewUsers() {
        return viewUsers.size();
    }

    public long strictUsers() {
        long strict = 0L;
        for (Long visitor : viewUsers) {
            if (buyerUsers.contains(visitor)) {
                strict++;
            }
        }
        return strict;
    }

    public DailyResult snapshot(
            String eventDate, long windowStart, long windowEnd, long watermark, boolean isFinal) {
        DailyResult result = new DailyResult();
        result.eventDate = eventDate;
        result.pv = pv;
        result.uv = viewUsers.size();
        result.dau = activeUsers.size();
        result.viewUsers = viewUsers.size();
        result.cartUsers = cartUsers.size();
        result.buyerUsers = buyerUsers.size();
        result.orders = transactionIds.size();
        result.strictUsers = strictUsers();
        result.addToCartRate = WindowUtils.ratio(result.cartUsers, result.viewUsers);
        result.convStrictRate = WindowUtils.ratio(result.strictUsers, result.viewUsers);
        result.convGlobalRate = WindowUtils.ratio(result.buyerUsers, result.viewUsers);
        result.itemsViewed = viewedItems.size();
        result.itemsCarted = cartedItems.size();
        result.itemsBought = boughtItems.size();
        result.windowStart = windowStart;
        result.windowEnd = windowEnd;
        result.sourceWatermark = watermark;
        result.isFinal = isFinal;
        return result;
    }
}
