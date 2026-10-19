package com.simplekafka.tennis;

import com.simplekafka.client.SimpleKafkaClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Loads recent finished ATP/WTA matches from Polymarket into two topics:
 *
 *   tennis-prices   one message per price change of player A's win price
 *   tennis-results  one message per match with the winner
 *
 * Both topics have a single partition so that replaying from offset 0 gives every message
 * in the order it was written. For each match the prices are written first, then the result.
 *
 * Usage: TennisBackfill <broker> <port> <days> [maxMatches] [seriesIds]
 *   days        how far back to look for finished matches (the API only keeps fine detail
 *               for recent matches, so 1 to 30 makes sense)
 *   maxMatches  load at most this many matches, the most recent ones (default 50)
 *   seriesIds   comma separated, default 10365,10366 (ATP and WTA)
 */
public final class TennisBackfill {
    public static final String PRICES_TOPIC = "tennis-prices";
    public static final String RESULTS_TOPIC = "tennis-results";

    private static final Path STATE_FILE = Path.of("tennis-state", "backfill-done.txt");

    /** Where messages go. In production this is the broker; in tests, a list. */
    public interface MessageSink {
        void send(String topic, String message) throws IOException;
    }

    /** Remembers which matches are fully loaded so a second run does not load them twice. */
    public interface DoneRecorder {
        void markDone(String slug) throws IOException;
    }

    /** Counters printed at the end of a run. */
    public static final class Stats {
        public int matchesLoaded;
        public int skippedNoWinner;
        public int skippedAlreadyDone;
        public int skippedTooOld;
        public int skippedNoPrices;
        public long priceMessages;
    }

    private TennisBackfill() {
    }

    public static Stats run(PolymarketApi api, MessageSink sink, Set<String> done, DoneRecorder recorder,
                            List<Long> seriesIds, long nowEpoch, int days, int maxMatches,
                            Consumer<String> log, long pauseMillis) throws IOException {
        Stats stats = new Stats();
        long since = nowEpoch - days * 86_400L;

        // 1. Find finished matches in the time range.
        Map<String, Match> candidates = new LinkedHashMap<>();
        for (long series : seriesIds) {
            List<Match> found = api.closedMatches(series, since, 10);
            log.accept("Series " + series + ": " + found.size() + " finished events found");
            for (Match m : found) {
                if (m.gameStart() == 0 || m.gameStart() < since) {
                    stats.skippedTooOld++;
                } else if (m.winner() == Match.UNKNOWN) {
                    stats.skippedNoWinner++;
                } else if (done.contains(m.slug())) {
                    stats.skippedAlreadyDone++;
                } else {
                    candidates.putIfAbsent(m.slug(), m);
                }
            }
        }

        // 2. Keep the most recent maxMatches, then load them oldest first.
        List<Match> chosen = new ArrayList<>(candidates.values());
        chosen.sort(Comparator.comparingLong(Match::gameStart).reversed());
        if (chosen.size() > maxMatches) {
            chosen = new ArrayList<>(chosen.subList(0, maxMatches));
        }
        chosen.sort(Comparator.comparingLong(Match::gameStart));
        log.accept("Loading " + chosen.size() + " matches");

        // 3. Load prices and the result of each match.
        for (Match m : chosen) {
            int bucket = BackfillPlan.bucketSeconds(m.gameStart(), nowEpoch);
            if (bucket == 0) {
                stats.skippedTooOld++;
                continue;
            }
            List<PricePoint> points = BackfillPlan.thin(
                    api.history(m.tokenA(), BackfillPlan.windowStart(m), BackfillPlan.windowEnd(m, nowEpoch), bucket));
            pause(pauseMillis);
            if (points.isEmpty()) {
                stats.skippedNoPrices++;
                log.accept("  no prices for " + m.title());
                continue;
            }
            for (PricePoint p : points) {
                sink.send(PRICES_TOPIC, Messages.price(m.slug(), p.timestamp(), p.price()));
            }
            sink.send(RESULTS_TOPIC, Messages.result(m));
            recorder.markDone(m.slug());
            stats.matchesLoaded++;
            stats.priceMessages += points.size();
            log.accept("  " + m.title() + ": " + points.size() + " prices, winner "
                    + (m.winner() == 'A' ? m.playerA() : m.playerB()));
        }
        return stats;
    }

    private static void pause(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: TennisBackfill <broker> <port> <days> [maxMatches] [seriesIds]");
            System.out.println("Example: TennisBackfill localhost 9092 7 20");
            System.exit(1);
        }
        String broker = args[0];
        int port = Integer.parseInt(args[1]);
        int days = Integer.parseInt(args[2]);
        int maxMatches = args.length > 3 ? Integer.parseInt(args[3]) : 50;
        List<Long> series = new ArrayList<>();
        for (String s : (args.length > 4 ? args[4] : PolymarketApi.ATP + "," + PolymarketApi.WTA).split(",")) {
            series.add(Long.parseLong(s.trim()));
        }

        SimpleKafkaClient client = new SimpleKafkaClient(broker, port);
        client.initialize();
        ensureTopic(client, PRICES_TOPIC);
        ensureTopic(client, RESULTS_TOPIC);

        Files.createDirectories(STATE_FILE.getParent());
        Set<String> done = new HashSet<>();
        if (Files.exists(STATE_FILE)) {
            done.addAll(Files.readAllLines(STATE_FILE, StandardCharsets.UTF_8));
        }

        MessageSink sink = (topic, message) ->
                client.send(topic, 0, message.getBytes(StandardCharsets.UTF_8));
        DoneRecorder recorder = slug -> {
            Files.writeString(STATE_FILE, slug + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            done.add(slug);
        };

        Stats stats = run(new PolymarketApi(PolymarketApi.javaHttp()), sink, done, recorder, series,
                Instant.now().getEpochSecond(), days, maxMatches, System.out::println, 200);

        System.out.println();
        System.out.println("Matches loaded:        " + stats.matchesLoaded);
        System.out.println("Price messages:        " + stats.priceMessages);
        System.out.println("Skipped, no winner:    " + stats.skippedNoWinner);
        System.out.println("Skipped, already done: " + stats.skippedAlreadyDone);
        System.out.println("Skipped, too old:      " + stats.skippedTooOld);
        System.out.println("Skipped, no prices:    " + stats.skippedNoPrices);
    }

    private static void ensureTopic(SimpleKafkaClient client, String topic) throws IOException {
        if (client.getTopicMetadata(topic) != null) {
            return;
        }
        // One partition, replication 1: keeps one ordered log and works with a single broker.
        if (!client.createTopic(topic, 1, (short) 1)) {
            throw new IOException("Could not create topic " + topic);
        }
    }
}
