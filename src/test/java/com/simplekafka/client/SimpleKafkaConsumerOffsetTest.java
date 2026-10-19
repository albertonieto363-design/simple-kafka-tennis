package com.simplekafka.client;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that a consumer saves and restores its position.
 * No broker is needed: creating a consumer does not connect, and commit/seek are local.
 */
class SimpleKafkaConsumerOffsetTest {

    private static SimpleKafkaConsumer consumer(FileOffsetStore store, String group, int partition) {
        return new SimpleKafkaConsumer("localhost", 9092, "tennis-prices", partition, group, store);
    }

    @Test
    void aNewConsumerStartsAtZero(@TempDir Path dir) {
        SimpleKafkaConsumer c = consumer(new FileOffsetStore(dir), "g", 0);

        assertEquals(0, c.getCurrentOffset());
    }

    @Test
    void aRestartedConsumerResumesFromTheCommittedOffset(@TempDir Path dir) throws Exception {
        FileOffsetStore store = new FileOffsetStore(dir);
        SimpleKafkaConsumer first = consumer(store, "g", 0);
        first.seek(42);
        first.commit();

        SimpleKafkaConsumer second = consumer(store, "g", 0);

        assertEquals(42, second.getCurrentOffset());
    }

    @Test
    void positionThatWasNotCommittedIsNotRemembered(@TempDir Path dir) throws Exception {
        FileOffsetStore store = new FileOffsetStore(dir);
        SimpleKafkaConsumer first = consumer(store, "g", 0);
        first.seek(10);
        first.commit();
        first.seek(50); // moved on but never committed

        SimpleKafkaConsumer second = consumer(store, "g", 0);

        assertEquals(10, second.getCurrentOffset());
    }

    @Test
    void groupsAndPartitionsKeepTheirOwnPositions(@TempDir Path dir) throws Exception {
        FileOffsetStore store = new FileOffsetStore(dir);
        SimpleKafkaConsumer a0 = consumer(store, "a", 0);
        SimpleKafkaConsumer a1 = consumer(store, "a", 1);
        SimpleKafkaConsumer b0 = consumer(store, "b", 0);
        a0.seek(5);  a0.commit();
        a1.seek(6);  a1.commit();
        b0.seek(7);  b0.commit();

        assertEquals(5, consumer(store, "a", 0).getCurrentOffset());
        assertEquals(6, consumer(store, "a", 1).getCurrentOffset());
        assertEquals(7, consumer(store, "b", 0).getCurrentOffset());
    }

    @Test
    void replayFromTheStartStillWorksAfterCommitting(@TempDir Path dir) throws Exception {
        FileOffsetStore store = new FileOffsetStore(dir);
        SimpleKafkaConsumer c = consumer(store, "g", 0);
        c.seek(30);
        c.commit();

        c.seek(0); // replay from the beginning

        assertEquals(0, c.getCurrentOffset());
    }

    @Test
    void aConsumerWithoutAStoreCommitsHarmlessly() throws Exception {
        SimpleKafkaConsumer c = new SimpleKafkaConsumer("localhost", 9092, "t", 0);
        c.seek(3);
        c.commit(); // nothing to save to, must not fail

        assertEquals(3, c.getCurrentOffset());
    }

    @Test
    void groupAndStoreMustBeGivenTogether(@TempDir Path dir) {
        assertThrows(IllegalArgumentException.class,
                () -> new SimpleKafkaConsumer("localhost", 9092, "t", 0, null, new FileOffsetStore(dir)));
    }
}