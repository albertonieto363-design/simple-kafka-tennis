package com.simplekafka.tennis;

import java.util.ArrayList;
import java.util.List;

/**
 * Decisions for the backfill: which time window to load for a match, how fine the price
 * steps can be, and how to drop readings that did not change anything.
 */
public final class BackfillPlan {
    private BackfillPlan() {
    }

    /** How long before the scheduled start we start loading prices. */
    public static final long BEFORE_START_SECONDS = 3 * 3600;
    /** How long after the scheduled start we keep loading prices (long matches and delays). */
    public static final long AFTER_START_SECONDS = 8 * 3600;

    private static final long DAY = 86_400;

    public static long windowStart(Match m) {
        return m.gameStart() - BEFORE_START_SECONDS;
    }

    public static long windowEnd(Match m, long nowEpoch) {
        return Math.min(m.gameStart() + AFTER_START_SECONDS, nowEpoch);
    }

    /**
     * The price step in seconds the API still keeps for a match that started at gameStart:
     * 1-minute data is kept for about a week, 5-minute data for about two months, and
     * older data is too coarse to be useful, so we return 0 (skip).
     */
    public static int bucketSeconds(long gameStart, long nowEpoch) {
        long age = nowEpoch - gameStart;
        if (age <= 6 * DAY) {
            return 60;
        }
        if (age <= 55 * DAY) {
            return 300;
        }
        return 0;
    }

    /**
     * Drops readings whose price equals the previous kept reading. The first and the last
     * reading are always kept. When replaying, a price holds until the next reading.
     */
    public static List<PricePoint> thin(List<PricePoint> points) {
        List<PricePoint> sorted = new ArrayList<>(points);
        sorted.sort((x, y) -> Long.compare(x.timestamp(), y.timestamp()));
        List<PricePoint> out = new ArrayList<>();
        for (int i = 0; i < sorted.size(); i++) {
            PricePoint p = sorted.get(i);
            boolean first = out.isEmpty();
            boolean last = i == sorted.size() - 1;
            if (first || last || p.price() != out.get(out.size() - 1).price()) {
                out.add(p);
            }
        }
        return out;
    }
}
