package com.simplekafka.tennis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TennisBackfillTest {
    static final long NOW = 1792360800L;            // 2026-10-18 22:00 UTC
    static final String START_2D_AGO = "2026-10-16 18:00:00+00";
    static final long START_2D_AGO_EPOCH = PolymarketParser.parseEpochSeconds(START_2D_AGO);

    static final String[] AB = {"Player A", "Player B"};

    static String ev(String slug, String[] prices, boolean closed, String start, String tokenA) {
        return PolymarketParserTest.event(slug, slug.toUpperCase(),
                PolymarketParserTest.market("moneyline", AB, new String[]{tokenA, tokenA + "x"},
                        prices, closed, start));
    }

    /** Fake network: serves events for any events/keyset call and history keyed by token. */
    static class FakeHttp implements PolymarketApi.HttpGet {
        final String eventsBody;
        final Map<String, String> historyByToken;
        final List<String> urls = new ArrayList<>();

        FakeHttp(String eventsBody, Map<String, String> historyByToken) {
            this.eventsBody = eventsBody;
            this.historyByToken = historyByToken;
        }

        @Override
        public String get(String url) {
            urls.add(url);
            if (url.contains("/events/keyset")) {
                return eventsBody;
            }
            for (Map.Entry<String, String> e : historyByToken.entrySet()) {
                if (url.contains("token_id=" + e.getKey() + "&")) {
                    return e.getValue();
                }
            }
            return "{\"data\":[]}";
        }
    }

    static String history(long start, double... prices) {
        StringBuilder sb = new StringBuilder("{\"data\":[");
        for (int i = 0; i < prices.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append("{\"timestamp\":").append(start + 60L * i).append(",\"price\":").append(prices[i])
                    .append(",\"resolution_seconds\":60}");
        }
        return sb.append("],\"pagination\":{\"has_more\":false}}").toString();
    }

    static class Capture implements TennisBackfill.MessageSink, TennisBackfill.DoneRecorder {
        final List<String> topics = new ArrayList<>();
        final List<String> messages = new ArrayList<>();
        final List<String> done = new ArrayList<>();

        @Override
        public void send(String topic, String message) {
            topics.add(topic);
            messages.add(message);
        }

        @Override
        public void markDone(String slug) {
            done.add(slug);
        }
    }

    TennisBackfill.Stats run(FakeHttp http, Capture cap, Set<String> done, int days, int max) throws IOException {
        return TennisBackfill.run(new PolymarketApi(http), cap, done, cap, List.of(PolymarketApi.ATP),
                NOW, days, max, s -> { }, 0);
    }

    @Test
    void loadsPricesThenResultAndSkipsUnresolvedMatches() throws IOException {
        String body = PolymarketParserTest.page(null,
                ev("won-by-a", new String[]{"1", "0"}, true, START_2D_AGO, "tok1"),
                ev("voided", new String[]{"0.5", "0.5"}, true, START_2D_AGO, "tok2"));
        FakeHttp http = new FakeHttp(body, Map.of(
                "tok1", history(START_2D_AGO_EPOCH, 0.6, 0.6, 0.7, 1.0)));
        Capture cap = new Capture();

        TennisBackfill.Stats stats = run(http, cap, new HashSet<>(), 7, 10);

        assertEquals(1, stats.matchesLoaded);
        assertEquals(1, stats.skippedNoWinner);
        assertEquals(3, stats.priceMessages);   // 0.6, 0.7, 1.0 after thinning
        assertEquals(4, cap.messages.size());   // 3 prices + 1 result
        assertEquals(TennisBackfill.PRICES_TOPIC, cap.topics.get(0));
        assertEquals(TennisBackfill.PRICES_TOPIC, cap.topics.get(2));
        assertEquals(TennisBackfill.RESULTS_TOPIC, cap.topics.get(3));
        assertEquals(Messages.price("won-by-a", START_2D_AGO_EPOCH, 0.6), cap.messages.get(0));
        assertEquals(Messages.price("won-by-a", START_2D_AGO_EPOCH + 120, 0.7), cap.messages.get(1));
        assertTrue(cap.messages.get(3).contains("\"winner\":\"A\""));
        assertEquals(List.of("won-by-a"), cap.done);
    }

    @Test
    void asksForTheWindowAroundTheStartAtOneMinuteSteps() throws IOException {
        String body = PolymarketParserTest.page(null,
                ev("m", new String[]{"0", "1"}, true, START_2D_AGO, "tok1"));
        FakeHttp http = new FakeHttp(body, Map.of("tok1", history(START_2D_AGO_EPOCH, 0.4, 0.3)));

        run(http, new Capture(), new HashSet<>(), 7, 10);

        String historyUrl = http.urls.stream().filter(u -> u.contains("prices-history")).findFirst().orElseThrow();
        assertTrue(historyUrl.contains("token_id=tok1&"));
        assertTrue(historyUrl.contains("start=" + (START_2D_AGO_EPOCH - 3 * 3600)));
        assertTrue(historyUrl.contains("end=" + (START_2D_AGO_EPOCH + 8 * 3600)));
        assertTrue(historyUrl.contains("bucket_seconds=60"));
        String eventsUrl = http.urls.get(0);
        assertTrue(eventsUrl.contains("series_id=" + PolymarketApi.ATP));
        assertTrue(eventsUrl.contains("closed=true"));
        assertTrue(eventsUrl.contains("end_date_min="));
    }

    @Test
    void recordsAreWrittenForLoserCase() throws IOException {
        String body = PolymarketParserTest.page(null,
                ev("m", new String[]{"0", "1"}, true, START_2D_AGO, "tok1"));
        FakeHttp http = new FakeHttp(body, Map.of("tok1", history(START_2D_AGO_EPOCH, 0.4, 0.3)));
        Capture cap = new Capture();

        run(http, cap, new HashSet<>(), 7, 10);

        assertTrue(cap.messages.get(cap.messages.size() - 1).contains("\"winner\":\"B\""));
    }

    @Test
    void skipsMatchesAlreadyLoaded() throws IOException {
        String body = PolymarketParserTest.page(null,
                ev("done-one", new String[]{"1", "0"}, true, START_2D_AGO, "tok1"));
        FakeHttp http = new FakeHttp(body, Map.of("tok1", history(START_2D_AGO_EPOCH, 0.6, 0.7)));
        Capture cap = new Capture();

        TennisBackfill.Stats stats = run(http, cap, new HashSet<>(Set.of("done-one")), 7, 10);

        assertEquals(0, stats.matchesLoaded);
        assertEquals(1, stats.skippedAlreadyDone);
        assertEquals(0, cap.messages.size());
    }

    @Test
    void skipsMatchesOutsideTheDayRangeOrWithoutAStartTime() throws IOException {
        String body = PolymarketParserTest.page(null,
                ev("old", new String[]{"1", "0"}, true, "2026-09-01 10:00:00+00", "tok1"),
                ev("nostart", new String[]{"1", "0"}, true, null, "tok2"));
        FakeHttp http = new FakeHttp(body, Map.of());

        TennisBackfill.Stats stats = run(http, new Capture(), new HashSet<>(), 7, 10);

        assertEquals(0, stats.matchesLoaded);
        assertEquals(2, stats.skippedTooOld);
    }

    @Test
    void matchWithoutPricesIsNotMarkedDone() throws IOException {
        String body = PolymarketParserTest.page(null,
                ev("empty", new String[]{"1", "0"}, true, START_2D_AGO, "tok1"));
        FakeHttp http = new FakeHttp(body, Map.of());
        Capture cap = new Capture();

        TennisBackfill.Stats stats = run(http, cap, new HashSet<>(), 7, 10);

        assertEquals(1, stats.skippedNoPrices);
        assertEquals(0, cap.messages.size());
        assertEquals(0, cap.done.size());
    }

    @Test
    void keepsOnlyTheMostRecentMatchesAndWritesThemOldestFirst() throws IOException {
        long t1 = START_2D_AGO_EPOCH - 86_400;      // 3 days ago
        long t2 = START_2D_AGO_EPOCH;               // 2 days ago
        long t3 = START_2D_AGO_EPOCH + 43_200;      // 1.5 days ago
        String body = PolymarketParserTest.page(null,
                ev("m2", new String[]{"1", "0"}, true, "2026-10-16 18:00:00+00", "tok2"),
                ev("m3", new String[]{"1", "0"}, true, "2026-10-17 06:00:00+00", "tok3"),
                ev("m1", new String[]{"1", "0"}, true, "2026-10-15 18:00:00+00", "tok1"));
        FakeHttp http = new FakeHttp(body, Map.of(
                "tok1", history(t1, 0.5, 0.6),
                "tok2", history(t2, 0.5, 0.6),
                "tok3", history(t3, 0.5, 0.6)));
        Capture cap = new Capture();

        TennisBackfill.Stats stats = run(http, cap, new HashSet<>(), 7, 2);

        assertEquals(2, stats.matchesLoaded);
        assertEquals(List.of("m2", "m3"), cap.done);   // m1 (oldest) dropped, then oldest first
    }

    @Test
    void networkErrorsStopTheRunInsteadOfWritingPartialMatches() {
        PolymarketApi.HttpGet failing = url -> { throw new IOException("HTTP 500"); };
        Capture cap = new Capture();

        assertThrows(IOException.class, () -> TennisBackfill.run(new PolymarketApi(failing), cap,
                new HashSet<>(), cap, List.of(PolymarketApi.ATP), NOW, 7, 10, s -> { }, 0));
        assertEquals(0, cap.messages.size());
    }

    @Test
    void messageFormatsAreValidJson() {
        Match m = new Match("slug-1", "Open: \"A\" vs B", "A", "B", "ta", "tb", 1792476000L, true, 'B');

        Map<?, ?> price = (Map<?, ?>) Json.parse(Messages.price("slug-1", 1792386000L, 0.785));
        assertEquals("slug-1", price.get("match"));
        assertEquals(1792386000.0, (Double) price.get("ts"), 0.0);
        assertEquals(0.785, (Double) price.get("p"), 1e-12);

        Map<?, ?> result = (Map<?, ?>) Json.parse(Messages.result(m));
        assertEquals("Open: \"A\" vs B", result.get("title"));
        assertEquals("B", result.get("winner"));
        assertEquals(1792476000.0, (Double) result.get("start"), 0.0);

        Match undecided = new Match("s", "t", "A", "B", "ta", "tb", 1, false, Match.UNKNOWN);
        assertThrows(IllegalArgumentException.class, () -> Messages.result(undecided));
    }
}
