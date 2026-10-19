package com.simplekafka.tennis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class BacktestTest {
    static final long BASE = 1_790_000_000L;

    static Calibration.Result result(String slug, long start, char winner) {
        return new Calibration.Result(slug, "T " + slug, start, winner);
    }

    static List<PricePoint> priceFor(long start, double pa) {
        return List.of(new PricePoint(start - 3 * 3600, pa));
    }

    static Backtest.Strategy favorite60() {
        return new Backtest.Strategy(60, Backtest.Side.FAVORITE, 0.5, 1.0);
    }

    static Backtest.Trade trade(Calibration.Result r, double pa, Backtest.Strategy s, Backtest.Costs c) {
        return Backtest.tradeFor(r, priceFor(r.start(), pa), s, c);
    }

    @Test
    void winningFavoriteTradeHasTheHandCalculatedProfit() {
        // mid 0.60, full spread 2 cents -> pay 0.61 -> $1 buys 1/0.61 shares -> pays 1.6393 -> profit 0.6393
        Backtest.Trade t = trade(result("m", BASE, 'A'), 0.60, favorite60(), Backtest.Costs.withSpreadCents(2, 0));
        assertEquals('A', t.side());
        assertEquals(0.61, t.entryPrice(), 1e-12);
        assertTrue(t.won());
        assertEquals(1 / 0.61 - 1, t.pnl(), 1e-12);
    }

    @Test
    void losingTradeLosesTheWholeStakeAndFeeIsAdded() {
        Backtest.Trade t = trade(result("m", BASE, 'B'), 0.60, favorite60(), Backtest.Costs.withSpreadCents(2, 1));
        assertEquals(-1.0 - 0.01, t.pnl(), 1e-12);
        Backtest.Trade w = trade(result("m", BASE, 'A'), 0.60, favorite60(), Backtest.Costs.withSpreadCents(0, 1));
        assertEquals(1 / 0.60 - 1 - 0.01, w.pnl(), 1e-12);
    }

    @Test
    void favoriteAndUnderdogPickTheRightPlayerWhenAIsTheUnderdog() {
        Backtest.Costs free = Backtest.Costs.withSpreadCents(0, 0);
        Calibration.Result bWins = result("m", BASE, 'B');
        Backtest.Trade fav = trade(bWins, 0.30, favorite60(), free);       // B is the favorite at 0.70
        assertEquals('B', fav.side());
        assertEquals(0.70, fav.midPrice(), 1e-12);
        assertTrue(fav.won());

        Backtest.Trade dog = trade(bWins, 0.30, new Backtest.Strategy(60, Backtest.Side.UNDERDOG, 0.5, 1.0), free);
        assertEquals('A', dog.side());
        assertEquals(0.30, dog.midPrice(), 1e-12);
        assertEquals(false, dog.won());
        assertEquals(-1.0, dog.pnl(), 1e-12);

        Backtest.Trade a = trade(bWins, 0.30, new Backtest.Strategy(60, Backtest.Side.PLAYER_A, 0.0, 1.0), free);
        assertEquals('A', a.side());
    }

    @Test
    void priceBandUsesTheFavoritesPriceAndIsHalfOpen() {
        Backtest.Strategy band = new Backtest.Strategy(60, Backtest.Side.FAVORITE, 0.6, 0.7);
        Backtest.Costs free = Backtest.Costs.withSpreadCents(0, 0);
        Calibration.Result r = result("m", BASE, 'A');
        assertNotNull(trade(r, 0.60, band, free));
        assertNotNull(trade(r, 0.65, band, free));
        assertNull(trade(r, 0.70, band, free));
        assertNull(trade(r, 0.55, band, free));
        assertNotNull(trade(r, 0.35, band, free));   // B is the favorite at 0.65
        Backtest.Strategy top = new Backtest.Strategy(60, Backtest.Side.FAVORITE, 0.8, 1.0);
        assertNotNull(trade(r, 1.0, top, free));      // upper edge 1.0 is included
    }

    @Test
    void noPriceMeansNoTradeAndEntryIsCapped() {
        Calibration.Result r = result("m", BASE, 'A');
        assertNull(Backtest.tradeFor(r, List.of(new PricePoint(BASE + 10, 0.6)), favorite60(),
                Backtest.Costs.withSpreadCents(0, 0)));
        assertNull(Backtest.tradeFor(r, null, favorite60(), Backtest.Costs.withSpreadCents(0, 0)));
        Backtest.Trade t = trade(r, 0.995, favorite60(), Backtest.Costs.withSpreadCents(4, 0));
        assertEquals(0.999, t.entryPrice(), 1e-12);
    }

    @Test
    void tradeUsesThePriceInForceAtTheEntryTime() {
        long start = BASE;
        List<PricePoint> prices = List.of(new PricePoint(start - 7200, 0.60), new PricePoint(start - 1800, 0.90));
        Calibration.Result r = result("m", start, 'A');
        Backtest.Costs free = Backtest.Costs.withSpreadCents(0, 0);
        assertEquals(0.60, Backtest.tradeFor(r, prices, new Backtest.Strategy(60, Backtest.Side.FAVORITE, 0.5, 1.0), free).midPrice(), 1e-12);
        assertEquals(0.90, Backtest.tradeFor(r, prices, new Backtest.Strategy(15, Backtest.Side.FAVORITE, 0.5, 1.0), free).midPrice(), 1e-12);
    }

    @Test
    void statsMatchHandCalculation() {
        // profits: +1, -1, +1, -1, -1  -> mean -0.2, cumulative 1,0,1,0,-1, peak 1, drawdown 2
        List<Backtest.Trade> trades = new ArrayList<>();
        double[] pnl = {1, -1, 1, -1, -1};
        for (int i = 0; i < pnl.length; i++) {
            trades.add(new Backtest.Trade("m" + i, i, 'A', 0.5, 0.5, pnl[i] > 0, pnl[i]));
        }
        Backtest.Stats s = Backtest.stats(trades);
        assertEquals(5, s.n());
        assertEquals(-0.2, s.mean(), 1e-12);
        assertEquals(Math.sqrt(4.8 / 4), s.sd(), 1e-12);            // sum of squares of deviations = 4.8
        assertEquals(-0.2 / (s.sd() / Math.sqrt(5)), s.tStat(), 1e-12);
        assertEquals(0.4, s.winRate(), 1e-12);
        assertEquals(-1.0, s.totalPnl(), 1e-12);
        assertEquals(2.0, s.maxDrawdown(), 1e-12);
        assertEquals(-0.2 / s.sd(), s.sharpePerTrade(), 1e-12);
        assertEquals(s.mean() - 1.96 * s.sd() / Math.sqrt(5), s.ciLow(), 1e-12);
    }

    @Test
    void statsHandleEmptyAndSingleTradeLists() {
        assertEquals(0, Backtest.stats(List.of()).n());
        assertTrue(Double.isNaN(Backtest.stats(List.of()).mean()));
        Backtest.Stats one = Backtest.stats(List.of(new Backtest.Trade("m", 0, 'A', 0.5, 0.5, true, 1.0)));
        assertEquals(1, one.n());
        assertEquals(1.0, one.mean(), 1e-12);
        assertTrue(Double.isNaN(one.tStat()));
    }

    @Test
    void drawdownOfAnAlwaysWinningRunIsZero() {
        List<Backtest.Trade> trades = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            trades.add(new Backtest.Trade("m" + i, i, 'A', 0.5, 0.5, true, 0.5));
        }
        assertEquals(0.0, Backtest.stats(trades).maxDrawdown(), 1e-12);
    }

    // ---- simulated markets ----

    /**
     * Matches in time order. Player A is always the favorite at price p. The true chance that the UNDERDOG wins is
     * (1 - p) + underdogEdge while the match index is below edgeUntil, and exactly (1 - p) afterwards.
     */
    static List<String>[] simulate(int matches, long seed, double underdogEdge, int edgeUntil) {
        Random rnd = new Random(seed);
        List<String> prices = new ArrayList<>();
        List<String> results = new ArrayList<>();
        for (int i = 0; i < matches; i++) {
            long start = BASE + i * 600L;
            String slug = "m-" + i;
            double p = 0.5 + rnd.nextDouble() * 0.45;
            double edge = i < edgeUntil ? underdogEdge : 0.0;
            double underdogWins = Math.min(0.99, (1 - p) + edge);
            char winner = rnd.nextDouble() < underdogWins ? 'B' : 'A';
            prices.add(Messages.price(slug, start - 3 * 3600, p));
            results.add(Messages.result(new Match(slug, "T " + slug, "P", "Q", "t1", "t2", start, true, winner)));
        }
        @SuppressWarnings("unchecked")
        List<String>[] out = new List[]{prices, results};
        return out;
    }

    @Test
    void anHonestMarketGivesZeroProfitAtZeroSpreadAndLosesTheSpreadOtherwise() {
        List<String>[] data = simulate(8000, 11, 0.0, 0);
        Calibration.Dataset d = Calibration.load(data[0], data[1]);
        List<Calibration.Result> all = BacktestStudy.ordered(d);

        Backtest.Stats free = Backtest.stats(Backtest.run(d, all, favorite60(), Backtest.Costs.withSpreadCents(0, 0)));
        Backtest.Stats paid = Backtest.stats(Backtest.run(d, all, favorite60(), Backtest.Costs.withSpreadCents(4, 0)));

        assertEquals(8000, free.n());
        assertEquals(0.0, free.mean(), 0.03);
        assertTrue(free.ciLow() < 0.0 && free.ciHigh() > 0.0, "no edge should include zero");
        assertTrue(paid.mean() < free.mean() - 0.02, "paying the spread must cost money");
        assertTrue(paid.ciHigh() < 0.0 || paid.mean() < 0.0);
    }

    @Test
    void aRealUnderdogEdgeIsFoundByTheRuleGrid() {
        List<String>[] data = simulate(6000, 12, 0.08, 6000);
        Calibration.Dataset d = Calibration.load(data[0], data[1]);
        List<Calibration.Result> all = BacktestStudy.ordered(d);

        Backtest.Stats dog = Backtest.stats(Backtest.run(d, all,
                new Backtest.Strategy(60, Backtest.Side.UNDERDOG, 0.5, 1.0), Backtest.Costs.withSpreadCents(2, 0)));
        Backtest.Stats fav = Backtest.stats(Backtest.run(d, all, favorite60(), Backtest.Costs.withSpreadCents(2, 0)));

        assertTrue(dog.ciLow() > 0, "an 8 point underdog edge should survive a 2 cent spread: " + dog.mean());
        assertTrue(fav.mean() < 0, "the favorite side pays for the edge");
    }

    @Test
    void studyUsesAChronologicalSplitAndNeverTunesOnTheTestPeriod() {
        List<String>[] data = simulate(300, 13, 0.0, 0);
        Calibration.Dataset d = Calibration.load(data[0], data[1]);
        List<Calibration.Result> all = BacktestStudy.ordered(d);

        for (int i = 1; i < all.size(); i++) {
            assertTrue(all.get(i - 1).start() <= all.get(i).start());
        }
        BacktestStudy.Output out = BacktestStudy.run(data[0], data[1], 2, 0, s -> { });
        assertEquals(200, out.trainMatches());
        assertEquals(100, out.testMatches());
        assertEquals(BacktestStudy.grid().size(), out.strategiesTried());
        assertEquals(30, BacktestStudy.grid().size());
    }

    @Test
    void anEdgeThatExistsOnlyInTheTrainPeriodDoesNotSurviveTheTestPeriod() {
        // Matches 0..1999 are the train period (underdog edge); 2000..2999 are the test period (honest).
        List<String>[] data = simulate(3000, 14, 0.10, 2000);

        BacktestStudy.Output out = BacktestStudy.run(data[0], data[1], 2, 0, s -> { });

        assertNotNull(out.chosen());
        assertEquals(Backtest.Side.UNDERDOG, out.chosen().side());
        assertTrue(out.chosenTrain().mean() > 0, "train should look profitable: " + out.chosenTrain().mean());
        assertTrue(out.chosenTest().ciLow() <= 0, "the test must not confirm a train-only edge");
        assertTrue(out.chosenTest().mean() < out.chosenTrain().mean());
        assertTrue(out.text().contains("TEST"));
    }

    @Test
    void anEdgeThroughoutIsConfirmedOnTheTestPeriod() {
        List<String>[] data = simulate(4500, 15, 0.10, 4500);

        BacktestStudy.Output out = BacktestStudy.run(data[0], data[1], 2, 0, s -> { });

        assertNotNull(out.chosen());
        assertTrue(out.chosenTest().mean() > 0);
        assertTrue(out.chosenTest().ciLow() > 0, "a real edge should show on the test period: " + out.chosenTest());
        assertTrue(out.text().contains("above zero"));
    }

    @Test
    void tooFewMatchesMeansNothingIsChosen() {
        List<String>[] data = simulate(20, 16, 0.0, 0);
        List<String> log = new ArrayList<>();

        BacktestStudy.Output out = BacktestStudy.run(data[0], data[1], 2, 0, log::add);

        assertNull(out.chosen());
        assertTrue(out.text().contains("nothing was chosen"));
        assertEquals(1, log.size());
    }

    @Test
    void emptyTopicsDoNotCrash() {
        BacktestStudy.Output out = BacktestStudy.run(List.of(), List.of(), 2, 0, s -> { });
        assertNull(out.chosen());
        assertEquals(0, out.trainMatches());
        assertEquals(0, out.testMatches());
    }
}
