package com.simplekafka.broker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionTest {

    // Tiny segments so a handful of messages forces several rollovers
    private static final int SMALL_SEGMENT = 100;
    private static final int BIG_FETCH = 1_000_000;

    private Partition newPartition(Path dir) {
        return new Partition(0, 1, new ArrayList<>(), dir.toString(), SMALL_SEGMENT);
    }

    private static byte[] msg(int i) {
        return ("msg-" + i).getBytes(StandardCharsets.UTF_8);
    }

    private static long segmentCount(Path dir) {
        File[] logs = dir.toFile().listFiles((d, name) -> name.endsWith(".log"));
        return logs == null ? 0 : logs.length;
    }

    @Test
    void appendReturnsSequentialOffsets(@TempDir Path dir) {
        Partition p = newPartition(dir);
        for (int i = 0; i < 25; i++) {
            assertEquals(i, p.append(msg(i)));
        }
        assertEquals(25, p.getLogEndOffset());
        p.close();
    }

    @Test
    void readsEverythingAcrossSegments(@TempDir Path dir) {
        Partition p = newPartition(dir);
        for (int i = 0; i < 40; i++) p.append(msg(i));
        assertTrue(segmentCount(dir) >= 3, "test needs several segments to be meaningful");

        List<byte[]> got = p.readMessages(0, BIG_FETCH);

        assertEquals(40, got.size());
        for (int i = 0; i < 40; i++) assertArrayEquals(msg(i), got.get(i));
        p.close();
    }

    @Test
    void readsFromTheMiddleAcrossASegmentBoundary(@TempDir Path dir) {
        Partition p = newPartition(dir);
        for (int i = 0; i < 40; i++) p.append(msg(i));

        List<byte[]> got = p.readMessages(7, BIG_FETCH);

        assertEquals(33, got.size());
        assertArrayEquals(msg(7), got.get(0));
        assertArrayEquals(msg(39), got.get(32));
        p.close();
    }

    @Test
    void readsFromTheStartOfALaterSegment(@TempDir Path dir) {
        Partition p = newPartition(dir);
        for (int i = 0; i < 40; i++) p.append(msg(i));

        // Segment file names are the base offset; use the second one as the start point
        File[] logs = dir.toFile().listFiles((d, name) -> name.endsWith(".log"));
        java.util.Arrays.sort(logs);
        long secondBase = Long.parseLong(logs[1].getName().replace(".log", ""));

        List<byte[]> got = p.readMessages(secondBase, BIG_FETCH);

        assertEquals(40 - secondBase, got.size());
        assertArrayEquals(msg((int) secondBase), got.get(0));
        p.close();
    }

    @Test
    void respectsMaxBytesWhileCrossingSegments(@TempDir Path dir) {
        Partition p = newPartition(dir);
        for (int i = 0; i < 40; i++) p.append(msg(i));

        // Each "msg-N" is 5 or 6 bytes + 4 bytes of length prefix; ask for roughly 15 messages
        List<byte[]> got = p.readMessages(0, 150);

        assertTrue(got.size() > 10 && got.size() < 20, "got " + got.size());
        assertArrayEquals(msg(0), got.get(0));
        // Messages that were returned are in order with no gaps
        for (int i = 0; i < got.size(); i++) assertArrayEquals(msg(i), got.get(i));
        p.close();
    }

    @Test
    void readingAtOrPastTheEndReturnsNothing(@TempDir Path dir) {
        Partition p = newPartition(dir);
        for (int i = 0; i < 10; i++) p.append(msg(i));

        assertTrue(p.readMessages(10, BIG_FETCH).isEmpty());
        assertTrue(p.readMessages(500, BIG_FETCH).isEmpty());
        p.close();
    }

    @Test
    void readingAnEmptyPartitionReturnsNothing(@TempDir Path dir) {
        Partition p = newPartition(dir);
        assertTrue(p.readMessages(0, BIG_FETCH).isEmpty());
        p.close();
    }

    @Test
    void dataSurvivesReopeningThePartition(@TempDir Path dir) {
        Partition p = newPartition(dir);
        for (int i = 0; i < 30; i++) p.append(msg(i));
        p.close();

        Partition reopened = newPartition(dir);
        assertEquals(30, reopened.getLogEndOffset());
        assertEquals(30, reopened.readMessages(0, BIG_FETCH).size());
        assertEquals(30, reopened.append(msg(30)));
        reopened.close();
    }
}