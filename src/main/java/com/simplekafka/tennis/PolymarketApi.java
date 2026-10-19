package com.simplekafka.tennis;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads public Polymarket data. Only GET requests, no account or key needed.
 * The HTTP part is behind {@link HttpGet} so tests can run without a network.
 */
public final class PolymarketApi {

    /** Fetches a URL and returns the response body as text. */
    public interface HttpGet {
        String get(String url) throws IOException;
    }

    public static final String GAMMA = "https://gamma-api.polymarket.com";
    public static final String DATA = "https://data-api.polymarket.com";

    /** Series ids: ATP singles 10365, WTA singles 10366. */
    public static final long ATP = 10365;
    public static final long WTA = 10366;

    private static final int MAX_HISTORY_PAGES = 20;

    private final HttpGet http;

    public PolymarketApi(HttpGet http) {
        this.http = http;
    }

    /**
     * Finished matches of a series whose event ended on or after sinceEpoch.
     * Pages through the results (100 per page) up to maxPages pages.
     */
    public List<Match> closedMatches(long seriesId, long sinceEpoch, int maxPages) throws IOException {
        List<Match> all = new ArrayList<>();
        String cursor = null;
        for (int page = 0; page < maxPages; page++) {
            String url = GAMMA + "/events/keyset?series_id=" + seriesId
                    + "&closed=true&limit=100"
                    + "&end_date_min=" + enc(Instant.ofEpochSecond(sinceEpoch).toString());
            if (cursor != null) {
                url += "&after_cursor=" + enc(cursor);
            }
            String body = http.get(url);
            all.addAll(PolymarketParser.parseEvents(body));
            cursor = PolymarketParser.nextCursor(body);
            if (cursor == null) {
                break;
            }
        }
        return all;
    }

    /**
     * Price history of one token from startEpoch (included) to endEpoch (excluded),
     * in steps of bucketSeconds. The API allows a range of up to 15 days.
     */
    public List<PricePoint> history(String tokenId, long startEpoch, long endEpoch, int bucketSeconds)
            throws IOException {
        List<PricePoint> all = new ArrayList<>();
        String cursor = null;
        for (int page = 0; page < MAX_HISTORY_PAGES; page++) {
            String url = DATA + "/v2/prices-history?token_id=" + enc(tokenId)
                    + "&start=" + startEpoch + "&end=" + endEpoch
                    + "&bucket_seconds=" + bucketSeconds;
            if (cursor != null) {
                url += "&cursor=" + enc(cursor);
            }
            String body = http.get(url);
            all.addAll(PolymarketParser.parseHistory(body));
            cursor = PolymarketParser.historyNextCursor(body);
            if (cursor == null) {
                break;
            }
        }
        return all;
    }

    /** A real HTTP client. Retries a few times on rate limits (429) and server errors (5xx). */
    public static HttpGet javaHttp() {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        return url -> {
            IOException last = null;
            for (int attempt = 1; attempt <= 4; attempt++) {
                try {
                    HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                            .timeout(Duration.ofSeconds(30))
                            .header("Accept", "application/json")
                            .header("User-Agent", "simple-kafka-tennis/1.0")
                            .GET()
                            .build();
                    HttpResponse<String> response =
                            client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                    int status = response.statusCode();
                    if (status == 200) {
                        return response.body();
                    }
                    last = new IOException("HTTP " + status + " for " + url);
                    if (status != 429 && status < 500) {
                        throw last; // a client error will not fix itself
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while fetching " + url, e);
                }
                try {
                    Thread.sleep(2000L * attempt);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting to retry", e);
                }
            }
            throw last;
        };
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
