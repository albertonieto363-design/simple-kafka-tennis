package com.simplekafka.tennis;

import com.simplekafka.client.SimpleKafkaConsumer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Replays tennis-prices and tennis-results from offset 0 and checks how well the market's
 * prices matched the outcomes.
 *
 * Usage: CalibrationStudy <broker> <port> [minutesBeforeStart] [buckets]
 *   minutesBeforeStart  comma separated snapshot times, default 180,60,0
 *   buckets             number of price buckets between 50% and 100%, default 5
 *
 * Writes calibration.html (the reliability charts) and calibration-observations.csv.
 */
public final class CalibrationStudy {
    private CalibrationStudy() {
    }

    /** The finished study: reports plus the files' text. */
    public record Output(List<Calibration.Report> reports, String html, String csv, int badMessages,
                         int matchesWithResult) {
    }

    public static Output run(List<String> priceMessages, List<String> resultMessages,
                             List<Integer> minutesBeforeStart, int buckets, Consumer<String> log) {
        Calibration.Dataset data = Calibration.load(priceMessages, resultMessages);
        log.accept(priceMessages.size() + " price messages for " + data.prices.size() + " matches, "
                + data.results.size() + " results");
        if (data.badMessages > 0) {
            log.accept("Warning: " + data.badMessages + " messages could not be read and were skipped");
        }

        List<Calibration.Report> reports = new ArrayList<>();
        StringBuilder csv = new StringBuilder();
        for (int minutes : minutesBeforeStart) {
            Calibration.Report report = Calibration.report(data, minutes, buckets);
            reports.add(report);
            log.accept("");
            log.accept(report.toText().stripTrailing());
            String part = Calibration.toCsv(Calibration.observe(data, minutes), minutes);
            csv.append(csv.length() == 0 ? part : part.substring(part.indexOf('\n') + 1));
        }

        String subtitle = data.results.size() + " finished matches replayed from the broker on " + LocalDate.now()
                + ". Small samples are noisy, so read the bars before the dots.";
        return new Output(reports, CalibrationChart.html(reports, subtitle), csv.toString(), data.badMessages,
                data.results.size());
    }

    /** Reads a whole single-partition topic from offset 0 to the end of the log. */
    static List<String> readTopic(String broker, int port, String topic) throws IOException {
        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(broker, port, topic, 0, 0L);
        consumer.initialize();
        List<String> messages = new ArrayList<>();
        while (true) {
            List<byte[]> batch = consumer.poll();
            if (batch.isEmpty()) {
                break;
            }
            for (byte[] b : batch) {
                messages.add(new String(b, StandardCharsets.UTF_8));
            }
        }
        consumer.close();
        return messages;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: CalibrationStudy <broker> <port> [minutesBeforeStart] [buckets]");
            System.out.println("Example: CalibrationStudy localhost 9092 180,60,0 5");
            System.exit(1);
        }
        String broker = args[0];
        int port = Integer.parseInt(args[1]);
        List<Integer> offsets = new ArrayList<>();
        for (String s : (args.length > 2 ? args[2] : "180,60,0").split(",")) {
            offsets.add(Integer.parseInt(s.trim()));
        }
        int buckets = args.length > 3 ? Integer.parseInt(args[3]) : 5;

        List<String> results = readTopic(broker, port, TennisBackfill.RESULTS_TOPIC);
        List<String> prices = readTopic(broker, port, TennisBackfill.PRICES_TOPIC);
        Output out = run(prices, results, offsets, buckets, System.out::println);

        Files.writeString(Path.of("calibration.html"), out.html(), StandardCharsets.UTF_8);
        Files.writeString(Path.of("calibration-observations.csv"), out.csv(), StandardCharsets.UTF_8);
        System.out.println();
        System.out.println("Wrote calibration.html (open it in your browser) and calibration-observations.csv");
    }
}
