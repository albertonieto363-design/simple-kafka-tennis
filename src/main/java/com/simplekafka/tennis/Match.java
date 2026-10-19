package com.simplekafka.tennis;

/**
 * One tennis match and its "who wins" (moneyline) market on Polymarket.
 *
 * Player A is the first outcome of the market, player B the second. tokenA is the
 * Polymarket token that pays 1 if player A wins, so its price is A's win probability.
 *
 * @param gameStart scheduled start in epoch seconds, or 0 if unknown
 * @param winner    'A' or 'B' once the market has resolved, otherwise '?'
 */
public record Match(
        String slug,
        String title,
        String playerA,
        String playerB,
        String tokenA,
        String tokenB,
        long gameStart,
        boolean closed,
        char winner) {

    public static final char UNKNOWN = '?';
}
