package com.simplekafka.tennis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Calibration statistics: when the market priced the favorite at 70%, did the favorite win
 * about 70% of the time?
 *
 * Method, in short:
 *  - Replay the price and result messages (from the broker topics) into a Dataset.
 *  - For each match, take the price of player A at a chosen time before the scheduled start
 *    (the last reading at or before that time; a price holds until the next reading).
 *  - Look at the market's favorite (the side priced 50% or more). One observation per match.
 *  - Group observations into price buckets and compare the average price with how often the
 *    favorite really won. Add a Brier score and log loss.
 */
public final class Calibration {
    private Calibration() {
    }

    /** If the first reading is at most this many seconds after the wanted time, use it anyway. */
    static final long TOLERANCE_SECONDS = 300;

    public record Result(String match, String title, long start, char winner) {
    }

    /** One match seen from the favorite's side. */
    public record Observation(String match, String title, double favoritePrice, boolean favoriteWon) {
    }

    public record Bucket(double low, double high, int n, double meanPrice, int wins,
                         double winRate, double ciLow, double ciHigh) {
    }

    /**
     * @param minutesBeforeStart the snapshot time, relative to the scheduled start
     * @param n                  matches with a usable price at that time
     * @param skipped            matches with a result but no price at that time
     */
    public record Report(int minutesBeforeStart, int n, int skipped, double brier, double logLoss,
                         double meanFavoritePrice, double favoriteWinRate, List<Bucket> buckets) {

        /** 1 means perfect, 0 means no better than always saying 50/50 (Brier 0.25). */
        public double brierSkill() {
            return n == 0 ? Double.NaN : 1 - brier / 0.25;
        }

        public String toText() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format(Locale.ROOT, "Snapshot %d min before the scheduled start: %d matches",
                    minutesBeforeStart, n));
            if (skipped > 0) {
                sb.append(String.format(Locale.ROOT, " (%d skipped, no price yet)", skipped));
            }
            sb.append('\n');
            if (n == 0) {
                return sb.append("  No data.\n").toString();
            }
            sb.append(String.format(Locale.ROOT,
                    "  Brier score %.4f (always 50/50 = 0.2500, skill %+.1f%%), log loss %.4f%n",
                    brier, brierSkill() * 100, logLoss));
            sb.append(String.format(Locale.ROOT,
                    "  Favorites were priced %.1f%% on average and won %.1f%%%n",
                    meanFavoritePrice * 100, favoriteWinRate * 100));
            sb.append("  price bucket    matches   avg price   favorite won   95% range\n");
            for (Bucket b : buckets) {
                sb.append(String.format(Locale.ROOT, "  %3.0f-%3.0f%%       %7d   %8.1f%%   %11.1f%%   %3.0f-%3.0f%%%n",
                        b.low() * 100, b.high() * 100, b.n(), b.meanPrice() * 100,
                        b.winRate() * 100, b.ciLow() * 100, b.ciHigh() * 100));
            }
            return sb.toString();
        }
    }

    /** Everything replayed from the two topics. */
    public static final class Dataset {
        public final Map<String, List<PricePoint>> prices = new HashMap<>();
        public final Map<String, Result> results = new HashMap<>();
        public int badMessages;
    }

    /** Builds a Dataset from the raw message texts. Unreadable messages are counted and skipped. */
    public static Dataset load(Collection<String> priceMessages, Collection<String> resultMessages) {
        Dataset d = new Dataset();
        for (String msg : priceMessages) {
            try {
                Map<?, ?> m = (Map<?, ?>) Json.parse(msg);
                String match = (String) m.get("match");
                double ts = (Double) m.get("ts");
                double p = (Double) m.get("p");
                if (match == null || p < 0 || p > 1) {
                    d.badMessages++;
                    continue;
                }
                d.prices.computeIfAbsent(match, k -> new ArrayList<>()).add(new PricePoint((long) ts, p));
            } catch (RuntimeException e) {
                d.badMessages++;
            }
        }
        for (List<PricePoint> list : d.prices.values()) {
            list.sort((x, y) -> Long.compare(x.timestamp(), y.timestamp()));
        }
        for (String msg : resultMessages) {
            try {
                Map<?, ?> m = (Map<?, ?>) Json.parse(msg);
                String match = (String) m.get("match");
                String winner = (String) m.get("winner");
                if (match == null || !("A".equals(winner) || "B".equals(winner))) {
                    d.badMessages++;
                    continue;
                }
                String title = m.get("title") instanceof String s ? s : match;
                long start = m.get("start") instanceof Double st ? st.longValue() : 0;
                // a match loaded twice keeps its last result
                d.results.put(match, new Result(match, title, start, winner.charAt(0)));
            } catch (RuntimeException e) {
                d.badMessages++;
            }
        }
        return d;
    }

    /**
     * The price in force at time t: the last reading at or before t. If every reading is later
     * than t but the first is within the tolerance, the first one is used. Otherwise null.
     * The list must be sorted by time.
     */
    public static Double priceAt(List<PricePoint> sorted, long t) {
        if (sorted == null || sorted.isEmpty()) {
            return null;
        }
        Double found = null;
        for (PricePoint p : sorted) {
            if (p.timestamp() <= t) {
                found = p.price();
            } else {
                break;
            }
        }
        if (found == null && sorted.get(0).timestamp() <= t + TOLERANCE_SECONDS) {
            found = sorted.get(0).price();
        }
        return found;
    }

    /** One observation per match that has a result and a price at the snapshot time. */
    public static List<Observation> observe(Dataset d, int minutesBeforeStart) {
        List<Observation> out = new ArrayList<>();
        for (Result r : d.results.values()) {
            Double p = priceAt(d.prices.get(r.match()), r.start() - minutesBeforeStart * 60L);
            if (p == null) {
                continue;
            }
            boolean aIsFavorite = p >= 0.5;
            double favoritePrice = aIsFavorite ? p : 1 - p;
            boolean favoriteWon = aIsFavorite == (r.winner() == 'A');
            out.add(new Observation(r.match(), r.title(), favoritePrice, favoriteWon));
        }
        out.sort((x, y) -> x.match().compareTo(y.match()));
        return out;
    }

    public static Report report(Dataset d, int minutesBeforeStart, int bucketCount) {
        List<Observation> obs = observe(d, minutesBeforeStart);
        return report(obs, d.results.size() - obs.size(), minutesBeforeStart, bucketCount);
    }

    public static Report report(List<Observation> obs, int skipped, int minutesBeforeStart, int bucketCount) {
        if (bucketCount < 1) {
            throw new IllegalArgumentException("bucketCount must be at least 1");
        }
        int n = obs.size();
        if (n == 0) {
            return new Report(minutesBeforeStart, 0, skipped, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
                    List.of());
        }

        double width = 0.5 / bucketCount;
        int[] count = new int[bucketCount];
        int[] wins = new int[bucketCount];
        double[] priceSum = new double[bucketCount];
        double brierSum = 0;
        double logSum = 0;
        double priceTotal = 0;
        int winTotal = 0;
        for (Observation o : obs) {
            double p = o.favoritePrice();
            double y = o.favoriteWon() ? 1 : 0;
            brierSum += (p - y) * (p - y);
            double clipped = Math.min(1 - 1e-6, Math.max(1e-6, p));
            logSum += -Math.log(o.favoriteWon() ? clipped : 1 - clipped);
            priceTotal += p;
            winTotal += o.favoriteWon() ? 1 : 0;

            int i = (int) Math.floor((p - 0.5) / width + 1e-9);
            i = Math.max(0, Math.min(bucketCount - 1, i));
            count[i]++;
            wins[i] += o.favoriteWon() ? 1 : 0;
            priceSum[i] += p;
        }

        List<Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < bucketCount; i++) {
            if (count[i] == 0) {
                continue;
            }
            double[] ci = wilson(wins[i], count[i]);
            buckets.add(new Bucket(0.5 + i * width, 0.5 + (i + 1) * width, count[i], priceSum[i] / count[i],
                    wins[i], (double) wins[i] / count[i], ci[0], ci[1]));
        }
        return new Report(minutesBeforeStart, n, skipped, brierSum / n, logSum / n, priceTotal / n,
                (double) winTotal / n, buckets);
    }

    /** 95% Wilson score interval for a win rate: {low, high}. */
    public static double[] wilson(int wins, int n) {
        if (n <= 0) {
            return new double[]{0, 1};
        }
        double z = 1.96;
        double phat = (double) wins / n;
        double denom = 1 + z * z / n;
        double center = (phat + z * z / (2.0 * n)) / denom;
        double half = z * Math.sqrt(phat * (1 - phat) / n + z * z / (4.0 * n * n)) / denom;
        return new double[]{Math.max(0, center - half), Math.min(1, center + half)};
    }

    /** One row per observation, for spreadsheets and your own analysis. */
    public static String toCsv(List<Observation> obs, int minutesBeforeStart) {
        StringBuilder sb = new StringBuilder("match,title,minutes_before_start,favorite_price,favorite_won\n");
        for (Observation o : obs) {
            sb.append(csv(o.match())).append(',')
                    .append(csv(o.title())).append(',')
                    .append(minutesBeforeStart).append(',')
                    .append(o.favoritePrice()).append(',')
                    .append(o.favoriteWon() ? 1 : 0).append('\n');
        }
        return sb.toString();
    }

    private static String csv(String s) {
        if (s.contains(",") || s.contains("\"") || s.contains("\n")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }
}
