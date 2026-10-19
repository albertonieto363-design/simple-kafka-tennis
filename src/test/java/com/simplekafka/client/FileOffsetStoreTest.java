package com.simplekafka.client;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileOffsetStoreTest {

    @Test
    void nothingSavedYetGivesEmpty(@TempDir Path dir) throws Exception {
        FileOffsetStore store = new FileOffsetStore(dir);
        assertTrue(store.load("g", "tennis-prices", 0).isEmpty());
    }

    @Test
    void savedOffsetComesBack(@TempDir Path dir) throws Exception {
        FileOffsetStore store = new FileOffsetStore(dir);
        store.save("g", "tennis-prices", 0, 42);

        OptionalLong loaded = store.load("g", "tennis-prices", 0);

        assertTrue(loaded.isPresent());
        assertEquals(42, loaded.getAsLong());
    }

    @Test
    void savingAgainReplacesTheOldOffset(@TempDir Path dir) throws Exception {
        FileOffsetStore store = new FileOffsetStore(dir);
        store.save("g", "t", 0, 10);
        store.save("g", "t", 0, 25);

        assertEquals(25, store.load("g", "t", 0).getAsLong());
    }

    @Test
    void offsetsSurviveANewStoreObject(@TempDir Path dir) throws Exception {
        new FileOffsetStore(dir).save("g", "t", 2, 99);

        // A brand new object pointing at the same folder, like a restarted program
        assertEquals(99, new FileOffsetStore(dir).load("g", "t", 2).getAsLong());
    }

    @Test
    void groupsTopicsAndPartitionsAreKeptApart(@TempDir Path dir) throws Exception {
        FileOffsetStore store = new FileOffsetStore(dir);
        store.save("alice", "t1", 0, 1);
        store.save("bob", "t1", 0, 2);
        store.save("alice", "t2", 0, 3);
        store.save("alice", "t1", 1, 4);

        assertEquals(1, store.load("alice", "t1", 0).getAsLong());
        assertEquals(2, store.load("bob", "t1", 0).getAsLong());
        assertEquals(3, store.load("alice", "t2", 0).getAsLong());
        assertEquals(4, store.load("alice", "t1", 1).getAsLong());
    }

    @Test
    void zeroIsAValidOffset(@TempDir Path dir) throws Exception {
        FileOffsetStore store = new FileOffsetStore(dir);
        store.save("g", "t", 0, 0);

        OptionalLong loaded = store.load("g", "t", 0);

        assertTrue(loaded.isPresent());
        assertEquals(0, loaded.getAsLong());
    }

    @Test
    void createsTheFolderIfItDoesNotExist(@TempDir Path dir) throws Exception {
        Path nested = dir.resolve("a").resolve("b");
        FileOffsetStore store = new FileOffsetStore(nested);
        store.save("g", "t", 0, 5);

        assertEquals(5, store.load("g", "t", 0).getAsLong());
    }

    @Test
    void leavesNoTemporaryFilesBehind(@TempDir Path dir) throws Exception {
        FileOffsetStore store = new FileOffsetStore(dir);
        store.save("g", "t", 0, 1);
        store.save("g", "t", 0, 2);

        File[] files = dir.toFile().listFiles();
        assertEquals(1, files.length);
        assertTrue(files[0].getName().endsWith(".offset"), files[0].getName());
    }

    @Test
    void oddNamesAreMadeSafeForTheFileSystem(@TempDir Path dir) throws Exception {
        FileOffsetStore store = new FileOffsetStore(dir);
        store.save("my group/../x", "tennis:prices*", 0, 7);

        assertEquals(7, store.load("my group/../x", "tennis:prices*", 0).getAsLong());
        assertEquals(1, dir.toFile().listFiles().length);
    }

    @Test
    void aCorruptFileIsReportedNotIgnored(@TempDir Path dir) throws Exception {
        FileOffsetStore store = new FileOffsetStore(dir);
        store.save("g", "t", 0, 5);
        File file = dir.toFile().listFiles()[0];
        Files.writeString(file.toPath(), "not a number");

        assertThrows(IOException.class, () -> store.load("g", "t", 0));
    }

    @Test
    void negativeOffsetsAreRejected(@TempDir Path dir) {
        FileOffsetStore store = new FileOffsetStore(dir);

        assertThrows(IllegalArgumentException.class, () -> store.save("g", "t", 0, -1));
    }
}