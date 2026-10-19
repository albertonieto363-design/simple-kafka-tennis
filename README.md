# SimpleKafka: a Kafka-style broker, used to study tennis prediction markets

A small message broker in Java modeled on Apache Kafka (topics, partitions, a log on disk, replay by offset), plus a study built on top of it: are individual ATP tennis match prices on Polymarket well calibrated, and can a simple rule beat them after costs?

**Short answer:** prices are well calibrated, and the one rule that looked profitable did not survive a held-out test. Details are under "Findings".

> **Credit:** the broker follows Raghav's "Build Your Own Kafka" tutorial on Medium ([link: ADD URL]). The architecture is from that tutorial. What I added is listed under "What I built and changed".

## How the pieces fit

```
Polymarket API --> TennisBackfill --> [tennis-prices] [tennis-results]  (topics in the broker)
                                               |
                          replay from offset 0 |
                                               v
                          CalibrationStudy      BacktestStudy
                          (chart + CSV)         (train/test split, costs)
```

The broker is used for what Kafka is for: an append-only log that several readers can replay from the start. Both studies read the full history from offset 0 every time they run.

## What I built and changed

**On the broker (from the tutorial)**
1. **Reads across log segments.** A read that crossed into the next segment file kept using the old closed channel and returned bad data. I rewrote the read loop (`Partition.readMessages`) and made lookups past the end of the index return "not found". 8 tests use 100-byte segments to force many rollovers.
2. **Large fetch responses.** The client read replies with one read into a 4096-byte buffer, so big or fragmented replies crashed the consumer. It now reads piece by piece. 5 tests run against a fake broker, including one that sends the reply 7 bytes at a time. Three failed on the original code.
3. **Persisted consumer offsets.** `FileOffsetStore` saves the next offset per (group, topic, partition) in `./offsets`, using a temp file and an atomic move so a crash cannot leave a half-written number. A consumer given a group id resumes where it stopped. 18 tests.

**The tennis study (new package `com.simplekafka.tennis`)**
- `PolymarketApi`, `PolymarketParser`, `Json`: a small client for the Gamma and price-history APIs and a hand-written JSON reader (no library).
- `TennisBackfill`: finds closed ATP match markets, works out the winner, loads each match's price history and result into the two topics. It records finished matches so a re-run skips them.
- `Calibration`, `CalibrationChart`, `CalibrationStudy`: reliability tables and chart, Brier score, log loss, Wilson 95% intervals.
- `Backtest`, `BacktestStudy`: trading rules with a spread and fee, train/test split, return, per-trade Sharpe ratio, maximum drawdown.

## Findings

Data: 326 ATP matches (mostly Challenger, ITF and qualifying events plus some Masters matches), 27,224 price messages, from recent matches. Polymarket only keeps fine-grained history for recent matches, so older ones cannot be loaded.

**Calibration.** Each match is seen from the favorite's side: what was its price at a set time before the scheduled start, and did it win?

| Time before start | Favorites priced | Favorites won | Brier score | vs. always 50/50 |
|---|---|---|---|---|
| 120 min | 66.3% | 64.6% | 0.2114 | +15.4% |
| 60 min | 66.2% | 64.9% | 0.2128 | +14.9% |
| 30 min | 66.3% | 63.1% | 0.2177 | +12.9% |
| 2 min | 66.4% | 64.1% | 0.2121 | +15.2% |
| 0 min | 66.9% | 59.8% | 0.2318 | +7.3% |

From 2 hours to 2 minutes before the start, prices are close to the observed win rates and every bucket's 95% range includes the diagonal. The reading at exactly the scheduled start is off, but readings 2 minutes before and 10 minutes after look normal, so I treat it as a data artifact at that instant, not a real inefficiency. I have not verified the cause.

**Backtest.** I tried 30 rules (favorite or underdog, 120, 60 or 30 minutes before the start, five price bands) with a 2 cent spread. The rule is chosen on the oldest 2/3 of matches (217) and judged only on the newest 1/3 (109).
- Buying every favorite lost 3.0 cents per dollar at zero spread and 4.5 cents at a 2 cent spread, so the cost model behaves sensibly.
- The best rule on the train period was "buy the underdog 30 minutes before when the favorite is at 70-80%": +22.6 cents per dollar on train.
- On the test period it made +55.4 cents per dollar, but over only 19 trades, with a 95% range of -30 to +140 cents. That includes zero, so there is no evidence of an edge. Picking the best of 30 rules produces results like this by chance.
- Detecting an edge of 5 cents per dollar on trades this noisy would take roughly 5,500 trades.

## Limitations

- Small sample, and mostly thin Challenger and ITF markets. Results may not hold for tour-level matches.
- The price history has one midpoint-like price per minute. There is no real bid and ask, so the spread is an assumption (adjustable: 0, 1, 2 and 4 cents are shown).
- Trades are $1 each with unlimited capital and no market impact.
- `gameStartTime` can be wrong for rescheduled matches.
- Re-running a backfill after a partial failure could duplicate that match's prices.
- Broker: the request buffer size is unverified, metadata, produce and create-topic replies still use a single read, and replication across several brokers is untested.

## Run it

You need three terminal windows: ZooKeeper, the broker, and one for the tools. Keep the first two open.

1. Start ZooKeeper (from the ZooKeeper folder, after copying `conf/zoo_sample.cfg` to `conf/zoo.cfg`):
   ```
   bin\zkServer.cmd
   ```
2. Build and test (99 tests):
   ```
   mvn clean package
   ```
3. Start the broker (arguments: id, host, port, ZooKeeper port):
   ```
   java -jar target\simple-kafka-1.0-SNAPSHOT.jar 1 localhost 9092 2181
   ```
4. Load matches (arguments: broker, port, days back, max matches, series ids; 10365 is ATP):
   ```
   java -cp target\classes com.simplekafka.tennis.TennisBackfill localhost 9092 7 100 10365
   ```
5. Run the calibration study (writes `calibration.html` and a CSV):
   ```
   java -cp target\classes com.simplekafka.tennis.CalibrationStudy localhost 9092 180,60,30,2 5
   ```
6. Run the backtest (arguments: broker, port, spread in cents, fee in percent):
   ```
   java -cp target\classes com.simplekafka.tennis.BacktestStudy localhost 9092 2 0
   ```

While the broker runs, `mvn clean package` fails because the jar is open. Use `mvn test` instead.

## Project layout

```
src/main/java/com/simplekafka/
  broker/   SimpleKafkaBroker, Partition, ZookeeperClient, Protocol, BrokerInfo
  client/   SimpleKafkaClient, SimpleKafkaProducer, SimpleKafkaConsumer, FileOffsetStore
  tennis/   Json, PolymarketApi, PolymarketParser, Match, PricePoint, BackfillPlan, Messages,
            TennisBackfill, Calibration, CalibrationChart, CalibrationStudy, Backtest, BacktestStudy
src/test/java/...   99 tests (JUnit 5)
```

Java 21, Maven, JUnit 5, Apache ZooKeeper 3.8 client library.

## What I learned

[Add 3 or 4 sentences in your own words: what offsets and partitions are, why Kafka keeps a log on disk, and what you learned about testing a trading idea.]

## Next steps

- Live collector: record real bid, ask and midpoint for upcoming matches, so the spread is measured and the sample grows past the history limit.
- Investigate the reading at the exact scheduled start.
