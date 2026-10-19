package com.simplekafka.tennis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class CalibrationStudyTest {
    static final long START = 1_792_476_000L;

    /** Builds matches whose first price is 3 hours before the start and holds until the start. */
    static List<String>[] simulate(int matches, long seed, double honesty) {
        Random rnd = new Random(seed);
        List<String> prices = new ArrayList<>();
        List<String> results = new ArrayList<>();
        for (int i = 0; i < matches; i++) {
            String slug = "match-" + i;
            double p = 0.5 + rnd.nextDouble() * 0.5;                  // price of A (A is always the favorite)
            double truth = 0.5 + (p - 0.5) * honesty;
            char winner = rnd.nextDouble() < truth ? 'A' : 'B';
            prices.add(Messages.price(slug, START - 3 * 3600, p));
            prices.add(Messages.price(slug, START + 3600, winner == 'A' ? 1.0 : 0.0));   // after the start
            results.add(Messages.result(new Match(slug, "Open: P" + i + " vs Q" + i, "P", "Q", "t1", "t2",
                    START, true, winner)));
        }
        @SuppressWarnings("unchecked")
        List<String>[] out = new List[]{prices, results};
        return out;
    }

    @Test
    void runProducesReportsHtmlAndCsvForEachSnapshotTime() {
        List<String>[] data = simulate(300, 1, 1.0);
        List<String> log = new ArrayList<>();

        CalibrationStudy.Output out = CalibrationStudy.run(data[0], data[1], List.of(180, 60, 0), 5, log::add);

        assertEquals(3, out.reports().size());
        assertEquals(300, out.reports().get(0).n());
        assertEquals(300, out.reports().get(1).n());
        assertEquals(300, out.reports().get(2).n());
        assertEquals(0, out.badMessages());
        assertEquals(300, out.matchesWithResult());
        assertTrue(log.stream().anyMatch(s -> s.contains("Brier score")));
        // header once, then 3 x 300 rows
        assertEquals(1 + 900, out.csv().split("\n").length);
        assertTrue(out.csv().startsWith("match,title,minutes_before_start"));
        assertEquals(1, out.csv().split("match,title,minutes_before_start").length - 1);
    }

    @Test
    void snapshotAtTheStartSeesAfterTheStartPricesNever() {
        // The post-start prices are 0 or 1 (the result); snapshots at or before the start must not use them.
        List<String>[] data = simulate(200, 2, 1.0);
        CalibrationStudy.Output out = CalibrationStudy.run(data[0], data[1], List.of(0), 5, s -> { });
        Calibration.Report r = out.reports().get(0);
        assertTrue(r.meanFavoritePrice() < 0.9, "snapshot must not leak the outcome: " + r.meanFavoritePrice());
        assertTrue(r.brier() > 0.12, "a leak would push the Brier score toward 0: " + r.brier());
    }

    @Test
    void honestAndOverconfidentMarketsGiveDifferentVerdicts() {
        List<String>[] honest = simulate(3000, 3, 1.0);
        List<String>[] bold = simulate(3000, 4, 0.4);

        Calibration.Report a = CalibrationStudy.run(honest[0], honest[1], List.of(60), 5, s -> { }).reports().get(0);
        Calibration.Report b = CalibrationStudy.run(bold[0], bold[1], List.of(60), 5, s -> { }).reports().get(0);

        assertEquals(a.meanFavoritePrice(), a.favoriteWinRate(), 0.03);
        assertTrue(b.meanFavoritePrice() - b.favoriteWinRate() > 0.1);
        assertTrue(b.brier() > a.brier());
    }

    @Test
    void htmlHasAPanelPerSnapshotWithChartAndTable() {
        List<String>[] data = simulate(300, 5, 1.0);
        String html = CalibrationStudy.run(data[0], data[1], List.of(180, 60, 0), 5, s -> { }).html();

        assertTrue(html.startsWith("<!doctype html>"));
        assertEquals(3, count(html, "<section class=\"panel\">"));
        assertEquals(3, count(html, "<svg "));
        assertEquals(3, count(html, "</svg>"));
        assertEquals(3, count(html, "<table>"));
        assertEquals(count(html, "<g class=\"pt\""), count(html, "</g>"));
        assertEquals(15, count(html, "<g class=\"pt\""));            // 5 buckets x 3 snapshots
        assertTrue(html.contains("180 minutes before the scheduled start"));
        assertTrue(html.contains("At the scheduled start"));
        assertTrue(html.contains("prefers-color-scheme: dark"));
        assertFalse(html.contains("NaN"));
        assertFalse(html.contains("Infinity"));
    }

    @Test
    void htmlEscapesTextAndHandlesAnEmptyStudy() {
        String html = CalibrationChart.html(
                List.of(Calibration.report(List.of(), 4, 60, 5)), "a <b>bold</b> & \"quoted\" subtitle");
        assertTrue(html.contains("a &lt;b&gt;bold&lt;/b&gt; &amp; &quot;quoted&quot; subtitle"));
        assertTrue(html.contains("No matches had a price at this time."));
        assertFalse(html.contains("<svg "));
        assertFalse(html.contains("NaN"));
    }

    @Test
    void emptyTopicsDoNotCrash() {
        CalibrationStudy.Output out = CalibrationStudy.run(List.of(), List.of(), List.of(60, 0), 5, s -> { });
        assertEquals(2, out.reports().size());
        assertEquals(0, out.reports().get(0).n());
        assertTrue(out.csv().isEmpty() || out.csv().startsWith("match,"));
    }

    private static int count(String text, String needle) {
        int n = 0;
        for (int i = text.indexOf(needle); i >= 0; i = text.indexOf(needle, i + needle.length())) {
            n++;
        }
        return n;
    }
}
