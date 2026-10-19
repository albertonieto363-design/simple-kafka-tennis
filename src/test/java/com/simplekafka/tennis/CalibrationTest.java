package com.simplekafka.tennis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class CalibrationTest {
    static final long START = 1_792_476_000L;

    static String price(String match, long ts, double p) {
        return Messages.price(match, ts, p);
    }

    static String result(String match, char winner) {
        return Messages.result(new Match(match, "Title " + match, "A", "B", "ta", "tb", START, true, winner));
    }

    static Calibration.Observation obs(double p, boolean won) {
        return new Calibration.Observation("m", "t", p, won);
    }

    @Test
    void priceAtUsesTheLastReadingAtOrBeforeTheTime() {
        List<PricePoint> pts = List.of(new PricePoint(100, 0.4), new PricePoint(200, 0.5), new PricePoint(300, 0.6));
        assertEquals(0.4, Calibration.priceAt(pts, 100), 0.0);
        assertEquals(0.4, Calibration.priceAt(pts, 199), 0.0);
        assertEquals(0.5, Calibration.priceAt(pts, 200), 0.0);
        assertEquals(0.6, Calibration.priceAt(pts, 10_000), 0.0);
    }

    @Test
    void priceAtAllowsASmallGapBeforeTheFirstReadingOnly() {
        List<PricePoint> pts = List.of(new PricePoint(1000, 0.7));
        assertEquals(0.7, Calibration.priceAt(pts, 1000 - 299), 0.0);
        assertEquals(0.7, Calibration.priceAt(pts, 1000 - 300), 0.0);
        assertNull(Calibration.priceAt(pts, 1000 - 301));
        assertNull(Calibration.priceAt(List.of(), 5));
        assertNull(Calibration.priceAt(null, 5));
    }

    @Test
    void observationIsSeenFromTheFavoritesSide() {
        List<String> prices = List.of(
                price("a-fav-wins", START - 3600, 0.70),
                price("a-fav-loses", START - 3600, 0.70),
                price("b-fav-wins", START - 3600, 0.30),
                price("b-fav-loses", START - 3600, 0.30));
        List<String> results = List.of(result("a-fav-wins", 'A'), result("a-fav-loses", 'B'),
                result("b-fav-wins", 'B'), result("b-fav-loses", 'A'));

        List<Calibration.Observation> o = Calibration.observe(Calibration.load(prices, results), 0);

        assertEquals(4, o.size());
        for (Calibration.Observation x : o) {
            assertEquals(0.7, x.favoritePrice(), 1e-12);
            assertEquals(x.match().endsWith("wins"), x.favoriteWon());
        }
    }

    @Test
    void usesThePriceInForceAtTheSnapshotTime() {
        List<String> prices = List.of(
                price("m", START - 10_800, 0.60),   // 3 hours before
                price("m", START - 1800, 0.90));    // 30 minutes before
        List<String> results = List.of(result("m", 'A'));
        Calibration.Dataset d = Calibration.load(prices, results);

        assertEquals(0.60, Calibration.observe(d, 180).get(0).favoritePrice(), 1e-12);
        assertEquals(0.60, Calibration.observe(d, 60).get(0).favoritePrice(), 1e-12);
        assertEquals(0.90, Calibration.observe(d, 0).get(0).favoritePrice(), 1e-12);
    }

    @Test
    void matchesWithoutAPriceAreSkippedAndCounted() {
        List<String> prices = List.of(price("has-price", START - 3600, 0.6));
        List<String> results = List.of(result("has-price", 'A'), result("no-price", 'A'));

        Calibration.Report r = Calibration.report(Calibration.load(prices, results), 60, 5);

        assertEquals(1, r.n());
        assertEquals(1, r.skipped());
    }

    @Test
    void brierScoreAndLogLossMatchHandCalculation() {
        // (0.8 won): (0.8-1)^2 = 0.04 ; (0.8 lost): (0.8-0)^2 = 0.64 ; mean 0.34
        Calibration.Report r = Calibration.report(List.of(obs(0.8, true), obs(0.8, false)), 0, 60, 5);
        assertEquals(0.34, r.brier(), 1e-12);
        assertEquals((-Math.log(0.8) - Math.log(0.2)) / 2, r.logLoss(), 1e-9);
        assertEquals(0.8, r.meanFavoritePrice(), 1e-12);
        assertEquals(0.5, r.favoriteWinRate(), 1e-12);
        assertEquals(1 - 0.34 / 0.25, r.brierSkill(), 1e-12);
    }

    @Test
    void aCoinFlipMarketScoresExactlyZeroSkill() {
        Calibration.Report r = Calibration.report(List.of(obs(0.5, true), obs(0.5, false)), 0, 60, 5);
        assertEquals(0.25, r.brier(), 1e-12);
        assertEquals(0.0, r.brierSkill(), 1e-12);
    }

    @Test
    void bucketEdgesLandInTheRightBucket() {
        List<Calibration.Observation> o = List.of(obs(0.5, true), obs(0.6, true), obs(0.7, true),
                obs(0.8, true), obs(0.9, true), obs(1.0, true), obs(0.59, true));
        Calibration.Report r = Calibration.report(o, 0, 0, 5);

        // buckets: 50-60 {0.5, 0.59}, 60-70 {0.6}, 70-80 {0.7}, 80-90 {0.8}, 90-100 {0.9, 1.0}
        assertEquals(5, r.buckets().size());
        int[] expected = {2, 1, 1, 1, 2};
        for (int i = 0; i < 5; i++) {
            assertEquals(expected[i], r.buckets().get(i).n());
        }
        assertEquals(0.5, r.buckets().get(0).low(), 1e-12);
        assertEquals(0.6, r.buckets().get(0).high(), 1e-12);
    }

    @Test
    void emptyBucketsAreLeftOutAndNoDataIsHandled() {
        Calibration.Report one = Calibration.report(List.of(obs(0.95, true)), 0, 0, 5);
        assertEquals(1, one.buckets().size());
        Calibration.Report none = Calibration.report(List.of(), 3, 60, 5);
        assertEquals(0, none.n());
        assertEquals(3, none.skipped());
        assertEquals(0, none.buckets().size());
        assertTrue(none.toText().contains("No data"));
        assertThrows(IllegalArgumentException.class, () -> Calibration.report(List.of(), 0, 0, 0));
    }

    @Test
    void wilsonIntervalMatchesKnownValues() {
        double[] half = Calibration.wilson(5, 10);
        assertEquals(0.2366, half[0], 0.0005);
        assertEquals(0.7634, half[1], 0.0005);
        double[] none = Calibration.wilson(0, 10);
        assertEquals(0.0, none[0], 1e-12);
        assertTrue(none[1] > 0.2 && none[1] < 0.35);
        double[] all = Calibration.wilson(10, 10);
        assertEquals(1.0, all[1], 1e-12);
        assertEquals(0, Calibration.wilson(0, 0)[0], 0.0);
        assertEquals(1, Calibration.wilson(0, 0)[1], 0.0);
    }

    @Test
    void loadSkipsBrokenMessagesAndKeepsTheLastResultOfAMatch() {
        List<String> prices = List.of(price("m", 100, 0.5), "not json", "{\"match\":\"m\",\"ts\":1,\"p\":1.5}",
                "{\"ts\":1,\"p\":0.5}", price("m", 50, 0.4));
        List<String> results = List.of(result("m", 'A'), result("m", 'B'),
                "{\"match\":\"x\",\"winner\":\"?\"}", "[]");

        Calibration.Dataset d = Calibration.load(prices, results);

        assertEquals(5, d.badMessages);
        assertEquals(1, d.results.size());
        assertEquals('B', d.results.get("m").winner());
        assertEquals(50L, d.prices.get("m").get(0).timestamp()); // sorted by time
        assertEquals(100L, d.prices.get("m").get(1).timestamp());
    }

    @Test
    void csvQuotesTitlesWithCommasAndQuotes() {
        List<Calibration.Observation> o = List.of(
                new Calibration.Observation("slug", "Open, Final: \"A\" vs B", 0.7, true));
        String csv = Calibration.toCsv(o, 60);
        String[] lines = csv.split("\n");
        assertEquals("match,title,minutes_before_start,favorite_price,favorite_won", lines[0]);
        assertEquals("slug,\"Open, Final: \"\"A\"\" vs B\",60,0.7,1", lines[1]);
    }

    /** Simulated market that is exactly right: the favorite wins with probability equal to its price. */
    @Test
    void aPerfectlyCalibratedMarketLandsOnTheDiagonal() {
        Random rnd = new Random(42);
        List<Calibration.Observation> o = new ArrayList<>();
        for (int i = 0; i < 40_000; i++) {
            double p = 0.5 + rnd.nextDouble() * 0.5;
            o.add(obs(p, rnd.nextDouble() < p));
        }
        Calibration.Report r = Calibration.report(o, 0, 0, 5);

        assertEquals(5, r.buckets().size());
        for (Calibration.Bucket b : r.buckets()) {
            assertEquals(b.meanPrice(), b.winRate(), 0.02);
            assertTrue(b.ciLow() <= b.meanPrice() && b.meanPrice() <= b.ciHigh());
        }
        // For uniform p in [0.5, 1], the expected Brier score is the average of p(1-p) = 1/6 = 0.1667.
        assertEquals(1.0 / 6, r.brier(), 0.004);
        assertTrue(r.brierSkill() > 0.30);
    }

    /** Simulated overconfident market: true chance is only half as far from 50% as the price says. */
    @Test
    void anOverconfidentMarketShowsUpAsDotsBelowTheDiagonal() {
        Random rnd = new Random(7);
        List<Calibration.Observation> o = new ArrayList<>();
        for (int i = 0; i < 40_000; i++) {
            double p = 0.5 + rnd.nextDouble() * 0.5;
            double truth = 0.5 + (p - 0.5) * 0.5;
            o.add(obs(p, rnd.nextDouble() < truth));
        }
        Calibration.Report r = Calibration.report(o, 0, 0, 5);

        Calibration.Bucket top = r.buckets().get(4);
        assertTrue(top.meanPrice() - top.winRate() > 0.15, "top bucket should be far below the diagonal");
        assertTrue(top.ciHigh() < top.meanPrice(), "even the 95% range should miss the diagonal");
        assertTrue(r.favoriteWinRate() < r.meanFavoritePrice() - 0.1);
        assertTrue(r.brier() > 1.0 / 6 + 0.02, "overconfidence should cost Brier score: " + r.brier());
    }

    @Test
    void textReportListsTheBuckets() {
        Calibration.Report r = Calibration.report(List.of(obs(0.65, true), obs(0.95, true)), 0, 60, 5);
        String text = r.toText();
        assertTrue(text.contains("60 min before"));
        assertTrue(text.contains("Brier score"));
        assertTrue(text.contains(" 60- 70%"));
        assertTrue(text.contains(" 90-100%"));
    }
}
