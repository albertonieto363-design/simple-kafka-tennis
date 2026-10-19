package com.simplekafka.tennis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class PolymarketParserTest {
    static final long START_2026_10_20_0600 = 1792476000L;

    /** Gamma sends lists as JSON text inside a string. */
    static String stringList(String... items) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < items.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(Json.quote(items[i]));
        }
        return Json.quote(sb.append(']').toString());
    }

    static String market(String type, String[] outcomes, String[] tokens, String[] prices,
                         boolean closed, String gameStart) {
        return "{\"question\":\"q\",\"sportsMarketType\":" + Json.quote(type)
                + ",\"outcomes\":" + stringList(outcomes)
                + ",\"clobTokenIds\":" + stringList(tokens)
                + (prices == null ? "" : ",\"outcomePrices\":" + stringList(prices))
                + ",\"closed\":" + closed
                + (gameStart == null ? "" : ",\"gameStartTime\":" + Json.quote(gameStart)) + "}";
    }

    static String event(String slug, String title, String... markets) {
        return "{\"slug\":" + Json.quote(slug) + ",\"title\":" + Json.quote(title)
                + ",\"markets\":[" + String.join(",", markets) + "]}";
    }

    static String page(String cursor, String... events) {
        return "{\"events\":[" + String.join(",", events) + "]"
                + (cursor == null ? "" : ",\"next_cursor\":" + Json.quote(cursor)) + "}";
    }

    static final String[] PLAYERS = {"Stefanos Tsitsipas", "Aleksandar Kovacevic"};
    static final String[] TOKENS = {"2584891087843707750175650908393993337415", "3147357875453067527605638413127410868169"};

    @Test
    void picksTheMoneylineMarketOutOfSeveral() {
        String ev = event("atp-tsitsip-kovacev-2026-10-19", "Almaty Open: Stefanos Tsitsipas vs Aleksandar Kovacevic",
                market("tennis_set_totals", new String[]{"Over 2.5", "Under 2.5"}, new String[]{"1", "2"},
                        new String[]{"0.3", "0.7"}, false, "2026-10-20 06:00:00+00"),
                market("moneyline", PLAYERS, TOKENS, new String[]{"0.785", "0.215"}, false, "2026-10-20 06:00:00+00"),
                market("tennis_first_set_winner", PLAYERS, new String[]{"7", "8"},
                        new String[]{"0.7", "0.3"}, false, "2026-10-20 06:00:00+00"));

        List<Match> matches = PolymarketParser.parseEvents(page(null, ev));

        assertEquals(1, matches.size());
        Match m = matches.get(0);
        assertEquals("atp-tsitsip-kovacev-2026-10-19", m.slug());
        assertEquals("Almaty Open: Stefanos Tsitsipas vs Aleksandar Kovacevic", m.title());
        assertEquals("Stefanos Tsitsipas", m.playerA());
        assertEquals("Aleksandar Kovacevic", m.playerB());
        assertEquals(TOKENS[0], m.tokenA());
        assertEquals(TOKENS[1], m.tokenB());
        assertEquals(START_2026_10_20_0600, m.gameStart());
        assertFalse(m.closed());
        assertEquals(Match.UNKNOWN, m.winner());
    }

    @Test
    void resolvedMatchHasAWinner() {
        String won = event("a", "A vs B", market("moneyline", new String[]{"A", "B"}, new String[]{"1", "2"},
                new String[]{"1", "0"}, true, "2025-10-08 09:00:00+00"));
        String lost = event("b", "C vs D", market("moneyline", new String[]{"C", "D"}, new String[]{"3", "4"},
                new String[]{"0", "1"}, true, "2025-10-08 09:00:00+00"));

        List<Match> matches = PolymarketParser.parseEvents(page(null, won, lost));

        assertEquals('A', matches.get(0).winner());
        assertEquals('B', matches.get(1).winner());
        assertTrue(matches.get(0).closed());
    }

    @Test
    void unresolvedOrVoidedMatchHasNoWinner() {
        String voided = event("v", "V", market("moneyline", new String[]{"A", "B"}, new String[]{"1", "2"},
                new String[]{"0.5", "0.5"}, true, "2026-10-01 09:00:00+00"));
        String stillOpenLooksDecided = event("o", "O", market("moneyline", new String[]{"A", "B"},
                new String[]{"1", "2"}, new String[]{"1", "0"}, false, "2026-10-01 09:00:00+00"));
        String noPrices = event("n", "N", market("moneyline", new String[]{"A", "B"}, new String[]{"1", "2"},
                null, true, "2026-10-01 09:00:00+00"));

        List<Match> matches = PolymarketParser.parseEvents(page(null, voided, stillOpenLooksDecided, noPrices));

        assertEquals(3, matches.size());
        for (Match m : matches) {
            assertEquals(Match.UNKNOWN, m.winner());
        }
    }

    @Test
    void skipsEventsWithoutAUsableMoneylineMarket() {
        String futures = event("f", "Tournament winner",
                market("tennis_set_totals", new String[]{"Yes", "No"}, new String[]{"1", "2"}, null, false, null));
        String threeWay = event("t", "Three outcomes",
                market("moneyline", new String[]{"A", "B", "C"}, new String[]{"1", "2", "3"}, null, false, null));
        String noMarkets = "{\"slug\":\"x\",\"title\":\"x\"}";
        String noSlug = "{\"title\":\"x\",\"markets\":[]}";

        assertEquals(0, PolymarketParser.parseEvents(page(null, futures, threeWay, noMarkets, noSlug)).size());
    }

    @Test
    void acceptsRealArraysInsteadOfStringifiedLists() {
        String ev = "{\"slug\":\"s\",\"title\":\"T\",\"markets\":[{\"sportsMarketType\":\"moneyline\","
                + "\"outcomes\":[\"A\",\"B\"],\"clobTokenIds\":[\"1\",\"2\"],\"outcomePrices\":[0.6,0.4],"
                + "\"closed\":false,\"gameStartTime\":\"2026-10-20T06:00:00Z\"}]}";

        List<Match> matches = PolymarketParser.parseEvents("[" + ev + "]");

        assertEquals(1, matches.size());
        assertEquals("B", matches.get(0).playerB());
        assertEquals(START_2026_10_20_0600, matches.get(0).gameStart());
    }

    @Test
    void missingStartTimeGivesZero() {
        String ev = event("s", "T", market("moneyline", new String[]{"A", "B"}, new String[]{"1", "2"},
                null, false, null));
        assertEquals(0, PolymarketParser.parseEvents(page(null, ev)).get(0).gameStart());
    }

    @Test
    void readsTheNextEventsCursor() {
        assertEquals("abc", PolymarketParser.nextCursor(page("abc")));
        assertNull(PolymarketParser.nextCursor(page(null)));
        assertNull(PolymarketParser.nextCursor("[]"));
    }

    @Test
    void readsPriceHistory() {
        String body = "{\"data\":[{\"timestamp\":1792386000,\"price\":0.785,\"resolution_seconds\":300},"
                + "{\"timestamp\":1792387892,\"price\":0.79,\"resolution_seconds\":0}],"
                + "\"pagination\":{\"limit\":100,\"has_more\":false}}";

        List<PricePoint> points = PolymarketParser.parseHistory(body);

        assertEquals(2, points.size());
        assertEquals(1792386000L, points.get(0).timestamp());
        assertEquals(0.785, points.get(0).price(), 1e-12);
        assertEquals(1792387892L, points.get(1).timestamp());
        assertNull(PolymarketParser.historyNextCursor(body));
    }

    @Test
    void historyCursorOnlyWhenThereIsMore() {
        String more = "{\"data\":[],\"pagination\":{\"has_more\":true,\"next_cursor\":\"c1\"}}";
        String last = "{\"data\":[],\"pagination\":{\"has_more\":false,\"next_cursor\":\"c1\"}}";
        assertEquals("c1", PolymarketParser.historyNextCursor(more));
        assertNull(PolymarketParser.historyNextCursor(last));
        assertEquals(0, PolymarketParser.parseHistory("{\"data\":[]}").size());
        assertEquals(0, PolymarketParser.parseHistory("{}").size());
    }

    @Test
    void parsesTimesInTheFormsPolymarketUses() {
        assertEquals(START_2026_10_20_0600, PolymarketParser.parseEpochSeconds("2026-10-20 06:00:00+00"));
        assertEquals(START_2026_10_20_0600, PolymarketParser.parseEpochSeconds("2026-10-20T06:00:00Z"));
        assertEquals(START_2026_10_20_0600, PolymarketParser.parseEpochSeconds("2026-10-19T23:00:00-07:00"));
        assertEquals(START_2026_10_20_0600, PolymarketParser.parseEpochSeconds("2026-10-19 23:00:00-07"));
        assertEquals(START_2026_10_20_0600, PolymarketParser.parseEpochSeconds("2026-10-20T06:00:00"));
        assertEquals(0, PolymarketParser.parseEpochSeconds(null));
        assertEquals(0, PolymarketParser.parseEpochSeconds(""));
        assertEquals(0, PolymarketParser.parseEpochSeconds("not a time"));
    }
}
