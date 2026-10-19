package com.simplekafka.tennis;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

/**
 * Replays the stored tennis prices through simple trading rules and reports honest results.
 *
 * Honesty rules built in:
 * 1. Matches are split by date. The rule is chosen on the older part (train) and judged ONLY on
 *    the newer part (test), so the reported result was never used to pick the rule.
 * 2. Costs are paid on every trade (half the spread, plus an optional fee), at several sizes.
 * 3. Baselines (buy every favorite, buy every player A) are shown so the cost model can be checked.
 * 4. The number of rules tried is printed, because the best of many rules always looks good on train.
 */
public final class BacktestStudy {
    private BacktestStudy() {
    }

    public static final int[] MINUTES = {120, 60, 30};
    public static final double[][] BANDS = {{0.5, 1.0}, {0.5, 0.6}, {0.6, 0.7}, {0.7, 0.8}, {0.8, 1.0}};
    public static final int MIN_TRAIN_TRADES = 30;

    public record Output(String text, int trainMatches, int testMatches, int strategiesTried,
                         Backtest.Strategy chosen, Backtest.Stats chosenTrain, Backtest.Stats chosenTest) {
    }

    /** Every rule on the grid. */
    public static List<Backtest.Strategy> grid() {
        List<Backtest.Strategy> all = new ArrayList<>();
        for (int m : MINUTES) {
            for (Backtest.Side side : new Backtest.Side[]{Backtest.Side.FAVORITE, Backtest.Side.UNDERDOG}) {
                for (double[] band : BANDS) {
                    all.add(new Backtest.Strategy(m, side, band[0], band[1]));
                }
            }
        }
        return all;
    }

    /** Matches that have a result, oldest first. */
    static List<Calibration.Result> ordered(Calibration.Dataset d) {
        List<Calibration.Result> list = new ArrayList<>();
        for (Calibration.Result r : d.results.values()) {
            if (d.prices.containsKey(r.match())) {
                list.add(r);
            }
        }
        list.sort((a, b) -> a.start() != b.start() ? Long.compare(a.start(), b.start())
                : a.match().compareTo(b.match()));
        return list;
    }

    public static Output run(List<String> priceMessages, List<String> resultMessages,
                             double fullSpreadCents, double feePercent, Consumer<String> log) {
        Calibration.Dataset d = Calibration.load(priceMessages, resultMessages);
        List<Calibration.Result> all = ordered(d);
        int split = (int) Math.round(all.size() * 2.0 / 3.0);
        List<Calibration.Result> train = all.subList(0, split);
        List<Calibration.Result> test = all.subList(split, all.size());
        Backtest.Costs costs = Backtest.Costs.withSpreadCents(fullSpreadCents, feePercent);
        List<Backtest.Strategy> grid = grid();

        StringBuilder sb = new StringBuilder();
        sb.append(String.format(Locale.ROOT, "%d matches: %d oldest for choosing a rule (train), %d newest for judging it (test)%n",
                all.size(), train.size(), test.size()));
        sb.append(String.format(Locale.ROOT, "Costs: spread %.1f cents (half paid on entry), fee %.1f%% of stake. Each trade stakes $1.%n%n",
                fullSpreadCents, feePercent));

        // 1. Baselines on the test period, at several spreads.
        sb.append("Baselines on the test period (profit per $1 trade; a fair market loses about the spread):\n");
        sb.append(String.format(Locale.ROOT, "  %-34s %6s %8s %8s%n", "rule", "trades", "spread 0", "spread 2"));
        Backtest.Strategy buyA = new Backtest.Strategy(60, Backtest.Side.PLAYER_A, 0.0, 1.0);
        Backtest.Strategy buyFav = new Backtest.Strategy(60, Backtest.Side.FAVORITE, 0.5, 1.0);
        for (Backtest.Strategy s : List.of(buyA, buyFav)) {
            Backtest.Stats z = Backtest.stats(Backtest.run(d, test, s, Backtest.Costs.withSpreadCents(0, feePercent)));
            Backtest.Stats t = Backtest.stats(Backtest.run(d, test, s, Backtest.Costs.withSpreadCents(2, feePercent)));
            sb.append(String.format(Locale.ROOT, "  %-34s %6d %s %s%n", s.label(), z.n(), money(z.mean()), money(t.mean())));
        }
        sb.append('\n');

        // 2. Choose on train.
        Backtest.Strategy best = null;
        Backtest.Stats bestTrain = null;
        for (Backtest.Strategy s : grid) {
            Backtest.Stats st = Backtest.stats(Backtest.run(d, train, s, costs));
            if (st.n() >= MIN_TRAIN_TRADES && (bestTrain == null || st.mean() > bestTrain.mean())) {
                best = s;
                bestTrain = st;
            }
        }
        sb.append(String.format(Locale.ROOT, "Tried %d rules on the train period (only rules with at least %d trades can win).%n",
                grid.size(), MIN_TRAIN_TRADES));
        if (best == null) {
            sb.append("No rule had enough trades on the train period, so nothing was chosen.\n");
            String text = sb.toString();
            log.accept(text);
            return new Output(text, train.size(), test.size(), grid.size(), null, null, null);
        }
        Backtest.Stats bestTest = Backtest.stats(Backtest.run(d, test, best, costs));
        sb.append("Best on train:  ").append(best.label()).append('\n');
        sb.append(describe("  train", bestTrain));
        sb.append(describe("  TEST ", bestTest));
        sb.append(verdict(bestTrain, bestTest));

        // 3. The chosen rule at other spreads, test period only.
        sb.append("\nChosen rule on the test period at other spreads:\n");
        sb.append(String.format(Locale.ROOT, "  %-8s %6s %9s %9s %9s%n", "spread", "trades", "mean", "Sharpe", "drawdown"));
        for (double cents : new double[]{0, 1, 2, 4}) {
            Backtest.Stats st = Backtest.stats(Backtest.run(d, test, best, Backtest.Costs.withSpreadCents(cents, feePercent)));
            sb.append(String.format(Locale.ROOT, "  %-8s %6d %s %9s %9s%n", cents + "c", st.n(), money(st.mean()),
                    num(st.sharpePerTrade()), num(st.maxDrawdown())));
        }

        String text = sb.toString();
        log.accept(text);
        return new Output(text, train.size(), test.size(), grid.size(), best, bestTrain, bestTest);
    }

    static String verdict(Backtest.Stats train, Backtest.Stats test) {
        StringBuilder v = new StringBuilder("  Verdict: ");
        if (test.n() < 2 || Double.isNaN(test.tStat())) {
            return v.append("too few test trades to say anything.\n").toString();
        }
        if (test.ciLow() > 0) {
            v.append("the test 95% range for profit per trade is above zero. That is rare for one rule picked from many,")
                    .append(" so check it again on new data before believing it.\n");
        } else if (test.ciHigh() < 0) {
            v.append("the rule LOSES money on unseen matches. The train result did not hold up.\n");
        } else {
            v.append("the test 95% range includes zero, so there is no evidence of an edge after costs.\n");
        }
        if (train.mean() > 0 && test.mean() < train.mean()) {
            v.append(String.format(Locale.ROOT,
                    "  Train profit per trade was %+.4f, test was %+.4f: the usual shrinkage from picking the best of many.%n",
                    train.mean(), test.mean()));
        }
        return v.toString();
    }

    static String describe(String name, Backtest.Stats s) {
        return String.format(Locale.ROOT,
                "%s: %d trades, win rate %.1f%%, mean %s per $1 (95%% range %s to %s), Sharpe/trade %s, total %+.2f, max drawdown %.2f%n",
                name, s.n(), s.winRate() * 100, money(s.mean()).trim(), money(s.ciLow()).trim(), money(s.ciHigh()).trim(),
                num(s.sharpePerTrade()), s.totalPnl(), s.maxDrawdown());
    }

    static String money(double x) {
        return Double.isNaN(x) ? "     n/a" : String.format(Locale.ROOT, "%+8.4f", x);
    }

    static String num(double x) {
        return Double.isNaN(x) ? "n/a" : String.format(Locale.ROOT, "%.3f", x);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: BacktestStudy <broker> <port> [spreadCents=2] [feePercent=0]");
            System.out.println("Example: BacktestStudy localhost 9092 2 0");
            System.exit(1);
        }
        String broker = args[0];
        int port = Integer.parseInt(args[1]);
        double spread = args.length > 2 ? Double.parseDouble(args[2]) : 2.0;
        double fee = args.length > 3 ? Double.parseDouble(args[3]) : 0.0;

        List<String> results = CalibrationStudy.readTopic(broker, port, TennisBackfill.RESULTS_TOPIC);
        List<String> prices = CalibrationStudy.readTopic(broker, port, TennisBackfill.PRICES_TOPIC);
        Output out = run(prices, results, spread, fee, System.out::println);
        try {
            Files.writeString(Path.of("backtest.txt"), out.text(), StandardCharsets.UTF_8);
            System.out.println("Wrote backtest.txt");
        } catch (IOException e) {
            System.out.println("Could not write backtest.txt: " + e.getMessage());
        }
    }
}
