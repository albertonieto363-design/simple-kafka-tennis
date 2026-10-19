package com.simplekafka.tennis;

/** One price reading: epoch seconds and the price (0 to 1) of a token. */
public record PricePoint(long timestamp, double price) {
}
