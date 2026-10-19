package com.simplekafka.tennis;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Turns Polymarket API responses into Java objects. Pure functions, no network.
 */
public final class PolymarketParser {
    private PolymarketParser() {
    }

    /** A resolved side must be at least this high, the other side at most 1 minus this. */
    private static final double RESOLVED_THRESHOLD = 0.99;

    /**
     * Reads a Gamma "events/keyset" response and returns one Match per event that has a
     * moneyline market. Events without a usable moneyline market are skipped.
     */
    public static List<Match> parseEvents(String json) {
        Object root = Json.parse(json);
        List<Object> events;
        if (root instanceof Map<?, ?> map) {
            events = asList(map.get("events"));
        } else {
            events = asList(root);
        }

        List<Match> matches = new ArrayList<>();
        for (Object o : events) {
            Map<String, Object> event = asMap(o);
            if (event == null) {
                continue;
            }
            Match match = parseEvent(event);
            if (match != null) {
                matches.add(match);
            }
        }
        return matches;
    }

    /** The cursor for the next page of events, or null on the last page. */
    public static String nextCursor(String json) {
        Map<String, Object> root = asMap(Json.parse(json));
        if (root == null) {
            return null;
        }
        Object cursor = root.get("next_cursor");
        return cursor instanceof String s && !s.isEmpty() ? s : null;
    }

    /** Reads a prices-history response: {"data":[{"timestamp":..,"price":..}], ...}. */
    public static List<PricePoint> parseHistory(String json) {
        Map<String, Object> root = asMap(Json.parse(json));
        List<PricePoint> points = new ArrayList<>();
        if (root == null) {
            return points;
        }
        for (Object o : asList(root.get("data"))) {
            Map<String, Object> row = asMap(o);
            if (row == null) {
                continue;
            }
            Double ts = toDouble(row.get("timestamp"));
            Double price = toDouble(row.get("price"));
            if (ts != null && price != null) {
                points.add(new PricePoint(ts.longValue(), price));
            }
        }
        return points;
    }

    /** The cursor for the next page of history, or null if there is no more. */
    public static String historyNextCursor(String json) {
        Map<String, Object> root = asMap(Json.parse(json));
        if (root == null) {
            return null;
        }
        Map<String, Object> pagination = asMap(root.get("pagination"));
        if (pagination == null || !Boolean.TRUE.equals(pagination.get("has_more"))) {
            return null;
        }
        Object cursor = pagination.get("next_cursor");
        return cursor instanceof String s && !s.isEmpty() ? s : null;
    }

    /** Parses times like "2026-10-20 06:00:00+00" or "2026-10-17T17:05:44Z". Returns 0 if unreadable. */
    public static long parseEpochSeconds(String time) {
        if (time == null || time.isBlank()) {
            return 0;
        }
        String t = time.trim().replace(' ', 'T');
        if (t.matches(".*T.*[+-]\\d\\d$")) {
            t = t + ":00"; // "+00" -> "+00:00"
        }
        try {
            return OffsetDateTime.parse(t).toEpochSecond();
        } catch (DateTimeParseException e) {
            try {
                return LocalDateTime.parse(t).toEpochSecond(ZoneOffset.UTC);
            } catch (DateTimeParseException e2) {
                return 0;
            }
        }
    }

    private static Match parseEvent(Map<String, Object> event) {
        Object slug = event.get("slug");
        if (!(slug instanceof String)) {
            return null;
        }
        for (Object mo : asList(event.get("markets"))) {
            Map<String, Object> market = asMap(mo);
            if (market == null || !"moneyline".equals(market.get("sportsMarketType"))) {
                continue;
            }
            List<String> outcomes = stringList(market.get("outcomes"));
            List<String> tokens = stringList(market.get("clobTokenIds"));
            if (outcomes.size() != 2 || tokens.size() != 2) {
                continue;
            }

            boolean closed = Boolean.TRUE.equals(market.get("closed"));
            long start = parseEpochSeconds(asString(market.get("gameStartTime")));
            if (start == 0) {
                start = parseEpochSeconds(asString(event.get("gameStartTime")));
            }
            String title = event.get("title") instanceof String t ? t : (String) slug;

            return new Match((String) slug, title, outcomes.get(0), outcomes.get(1),
                    tokens.get(0), tokens.get(1), start, closed,
                    winnerOf(closed, stringList(market.get("outcomePrices"))));
        }
        return null;
    }

    private static char winnerOf(boolean closed, List<String> prices) {
        if (!closed || prices.size() != 2) {
            return Match.UNKNOWN;
        }
        double a, b;
        try {
            a = Double.parseDouble(prices.get(0));
            b = Double.parseDouble(prices.get(1));
        } catch (NumberFormatException e) {
            return Match.UNKNOWN;
        }
        if (a >= RESOLVED_THRESHOLD && b <= 1 - RESOLVED_THRESHOLD) {
            return 'A';
        }
        if (b >= RESOLVED_THRESHOLD && a <= 1 - RESOLVED_THRESHOLD) {
            return 'B';
        }
        return Match.UNKNOWN; // for example a voided match priced 0.5 / 0.5
    }

    /** Gamma sends lists as JSON text inside a string, e.g. "[\"Yes\",\"No\"]". Handle both forms. */
    private static List<String> stringList(Object value) {
        Object v = value;
        if (v instanceof String s) {
            if (s.isBlank()) {
                return List.of();
            }
            try {
                v = Json.parse(s);
            } catch (IllegalArgumentException e) {
                return List.of();
            }
        }
        List<String> out = new ArrayList<>();
        for (Object o : asList(v)) {
            if (o instanceof String s) {
                out.add(s);
            } else if (o instanceof Double d) {
                out.add(Double.toString(d));
            }
        }
        return out;
    }

    private static Double toDouble(Object o) {
        if (o instanceof Double d) {
            return d;
        }
        if (o instanceof String s) {
            try {
                return Double.valueOf(s);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private static String asString(Object o) {
        return o instanceof String s ? s : null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        return o instanceof Map<?, ?> ? (Map<String, Object>) o : null;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> asList(Object o) {
        return o instanceof List<?> ? (List<Object>) o : List.of();
    }
}
