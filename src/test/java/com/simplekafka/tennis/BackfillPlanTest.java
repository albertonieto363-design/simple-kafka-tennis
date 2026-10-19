package com.simplekafka.tennis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class BackfillPlanTest {
    static final long DAY = 86_400;
    static final long NOW = 1_800_000_000L;

    static Match matchStartingAt(long start) {
        return new Match("s", "t", "A", "B", "ta", "tb", start, true, 'A');
    }

    @Test
    void bucketSizeDependsOnHowOldTheMatchIs() {
        assertEquals(60, BackfillPlan.bucketSeconds(NOW - 1 * DAY, NOW));
        assertEquals(60, BackfillPlan.bucketSeconds(NOW - 6 * DAY, NOW));
        assertEquals(300, BackfillPlan.bucketSeconds(NOW - 6 * DAY - 1, NOW));
        assertEquals(300, BackfillPlan.bucketSeconds(NOW - 55 * DAY, NOW));
        assertEquals(0, BackfillPlan.bucketSeconds(NOW - 55 * DAY - 1, NOW));
        assertEquals(60, BackfillPlan.bucketSeconds(NOW + DAY, NOW)); // not started yet
    }

    @Test
    void windowIsAroundTheScheduledStart() {
        Match m = matchStartingAt(NOW - 2 * DAY);
        assertEquals(m.gameStart() - 3 * 3600, BackfillPlan.windowStart(m));
        assertEquals(m.gameStart() + 8 * 3600, BackfillPlan.windowEnd(m, NOW));
    }

    @Test
    void windowEndNeverPassesNow() {
        Match m = matchStartingAt(NOW - 3600);
        assertEquals(NOW, BackfillPlan.windowEnd(m, NOW));
    }

    @Test
    void thinDropsRepeatedPricesButKeepsFirstAndLast() {
        List<PricePoint> in = List.of(
                new PricePoint(10, 0.5), new PricePoint(20, 0.5), new PricePoint(30, 0.5),
                new PricePoint(40, 0.6), new PricePoint(50, 0.6),
                new PricePoint(60, 0.5), new PricePoint(70, 0.5));

        List<PricePoint> out = BackfillPlan.thin(in);

        assertEquals(List.of(
                new PricePoint(10, 0.5), new PricePoint(40, 0.6),
                new PricePoint(60, 0.5), new PricePoint(70, 0.5)), out);
    }

    @Test
    void thinSortsByTimeAndHandlesTinyInputs() {
        assertEquals(0, BackfillPlan.thin(List.of()).size());
        assertEquals(1, BackfillPlan.thin(List.of(new PricePoint(5, 0.4))).size());
        List<PricePoint> out = BackfillPlan.thin(List.of(new PricePoint(20, 0.7), new PricePoint(10, 0.4)));
        assertEquals(10L, out.get(0).timestamp());
        assertEquals(20L, out.get(1).timestamp());
    }

    @Test
    void thinKeepsAllPointsOfAConstantSeriesOnlyAtTheEnds() {
        List<PricePoint> in = List.of(new PricePoint(1, 0.3), new PricePoint(2, 0.3),
                new PricePoint(3, 0.3), new PricePoint(4, 0.3));
        List<PricePoint> out = BackfillPlan.thin(in);
        assertEquals(2, out.size());
        assertEquals(1L, out.get(0).timestamp());
        assertEquals(4L, out.get(1).timestamp());
    }
}
