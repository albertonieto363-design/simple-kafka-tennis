package com.simplekafka.tennis;

/**
 * The JSON text of the messages written to the tennis topics.
 *
 * tennis-prices: {"match":"<slug>","ts":<epoch seconds>,"p":<price of player A, 0 to 1>}
 * tennis-results: {"match":"<slug>","title":"..","a":"..","b":"..","start":<epoch seconds>,"winner":"A"|"B"}
 *
 * The price p is the market price of the token that pays 1 if player A wins. Player B's
 * price is 1 - p (the two tokens of a market are complements).
 */
public final class Messages {
    private Messages() {
    }

    public static String price(String matchSlug, long timestamp, double priceOfA) {
        return "{\"match\":" + Json.quote(matchSlug)
                + ",\"ts\":" + timestamp
                + ",\"p\":" + priceOfA + "}";
    }

    public static String result(Match m) {
        if (m.winner() != 'A' && m.winner() != 'B') {
            throw new IllegalArgumentException("Match has no winner yet: " + m.slug());
        }
        return "{\"match\":" + Json.quote(m.slug())
                + ",\"title\":" + Json.quote(m.title())
                + ",\"a\":" + Json.quote(m.playerA())
                + ",\"b\":" + Json.quote(m.playerB())
                + ",\"start\":" + m.gameStart()
                + ",\"winner\":\"" + m.winner() + "\"}";
    }
}
