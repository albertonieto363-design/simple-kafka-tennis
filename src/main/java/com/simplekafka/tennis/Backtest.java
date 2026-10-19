package com.simplekafka.tennis;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Backtest of simple "buy one side at a set time before the match and hold to the result" rules.
 *
 * Every trade stakes $1 and is held until the match resolves, so there is no exit cost: the
 * token pays 1 per share if the player wins and 0 otherwise. The entry price is the price
 * in the replayed data (a midpoint-like price) plus an assumed half spread, optional extra
 * slippage, and an optional proportional fee.
 *
 * Trades overlap in time and capital is treated as unlimited. Nothing here models order
 * size, so results hold only for stakes small enough not to move the price.
 */
public final class Backtest {
    private Backtest() {
    }

    public enum Side { FAVORITE, UNDERDOG, PLAYER_A }

    /**
     * A rule: at minutesBeforeStart, if the favorite's price is in [minFavoritePrice, maxFavoritePrice),
     * buy the given side. The upper bound is inclusive when it is 1.0 or more.
     */
    public record Strategy(int minutesBeforeStart, Side side, double minFavoritePrice, double maxFavoritePrice) {
        public String label() {
            String who = switch (side) {
                case FAVORITE -> "favorite";
                case UNDERDOG -> "underdog";
                case PLAYER_A -> "player A";
            };
            boolean all = minFavoritePrice <= 0.5 && maxFavoritePrice >= 1.0;
            String band = all ? "any price"
                    : String.format(Locale.ROOT, "fav %.0f-%.0f%%", minFavoritePrice * 100, maxFavoritePrice * 100);
            return String.format(Locale.ROOT, "buy %s %dm before, %s", who, minutesBeforeStart, band);
        }
    }

    /**
     * Trading costs in price units (0.01 = one cent per share).
     * @param halfSpread half of the bid-ask spread, paid when buying at the ask
     * @param slippage   extra price paid on top of the ask
     * @param feeRate    fee as a fraction of the $1 stake
     */
    public record Costs(double halfSpread, double slippage, double feeRate) {
        /** Costs for a given full bid-ask spread in cents, split evenly around the midpoint. */
        public static Costs withSpreadCents(double fullSpreadCents, double feePercent) {
            return new Costs(fullSpreadCents / 200.0, 0, feePercent / 100.0);
        }
    }

    public record Trade(String match, long start, char side, double midPrice, double entryPrice,
                        boolean won, double pnl) {
    }

    /**
     * @param sd         standard deviation of per-trade profit
     * @param tStat      mean divided by its standard error (about 2 or more is notable for ONE strategy)
     * @param maxDrawdown largest peak-to-trough fall of cumulative profit, in dollars of stake
     */
    public record Stats(int n, double mean, double sd, double tStat, double ciLow, double ciHigh,
                        double winRate, double totalPnl, double maxDrawdown) {
        /** Mean over standard deviation, per trade. Not annualized on purpose. */
        public double sharpePerTrade() {
            return sd > 0 ? mean / sd : Double.NaN;
        }
    }

    /** The trade this rule makes on one match, or null if it does not trade. */
    public static Trade tradeFor(Calibration.Result r, List<PricePoint> prices, Strategy s, Costs c) {
        Double pa = Calibration.priceAt(prices, r.start() - s.minutesBeforeStart() * 60L);
        if (pa == null) {
            return null;
        }
        boolean aIsFavorite = pa >= 0.5;
        double favoritePrice = aIsFavorite ? pa : 1 - pa;
        boolean inBand = favoritePrice >= s.minFavoritePrice()
                && (favoritePrice < s.maxFavoritePrice() || s.maxFavoritePrice() >= 1.0);
        if (!inBand) {
            return null;
        }

        char side;
        double mid;
        switch (s.side()) {
            case FAVORITE -> {
                side = aIsFavorite ? 'A' : 'B';
                mid = favoritePrice;
            }
            case UNDERDOG -> {
                side = aIsFavorite ? 'B' : 'A';
                mid = 1 - favoritePrice;
            }
            default -> {
                side = 'A';
                mid = pa;
            }
        }
        double entry = Math.min(0.999, mid + c.halfSpread() + c.slippage());
        boolean won = r.winner() == side;
        double pnl = (won ? 1.0 / entry : 0.0) - 1.0 - c.feeRate();
        return new Trade(r.match(), r.start(), side, mid, entry, won, pnl);
    }

    /** All trades of a rule over the given matches, in order of match start. */
    public static List<Trade> run(Calibration.Dataset d, List<Calibration.Result> matches, Strategy s, Costs c) {
        List<Calibration.Result> ordered = new ArrayList<>(matches);
        ordered.sort((x, y) -> Long.compare(x.start(), y.start()));
        List<Trade> trades = new ArrayList<>();
        for (Calibration.Result r : ordered) {
            Trade t = tradeFor(r, d.prices.get(r.match()), s, c);
            if (t != null) {
                trades.add(t);
            }
        }
        return trades;
    }

    /** Summary statistics. The trades must already be in time order (run() does that). */
    public static Stats stats(List<Trade> trades) {
        int n = trades.size();
        if (n == 0) {
            return new Stats(0, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 0, 0);
        }
        double sum = 0;
        int wins = 0;
        double cumulative = 0;
        double peak = 0;
        double maxDrawdown = 0;
        for (Trade t : trades) {
            sum += t.pnl();
            wins += t.won() ? 1 : 0;
            cumulative += t.pnl();
            peak = Math.max(peak, cumulative);
            maxDrawdown = Math.max(maxDrawdown, peak - cumulative);
        }
        double mean = sum / n;
        if (n < 2) {
            return new Stats(n, mean, Double.NaN, Double.NaN, Double.NaN, Double.NaN, (double) wins / n, sum,
                    maxDrawdown);
        }
        double squares = 0;
        for (Trade t : trades) {
            squares += (t.pnl() - mean) * (t.pnl() - mean);
        }
        double sd = Math.sqrt(squares / (n - 1));
        double se = sd / Math.sqrt(n);
        double t = se > 0 ? mean / se : Double.NaN;
        return new Stats(n, mean, sd, t, mean - 1.96 * se, mean + 1.96 * se, (double) wins / n, sum, maxDrawdown);
    }
}
