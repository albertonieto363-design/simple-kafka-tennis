package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.OptionalLong;

/**
 * Remembers how far a consumer has read, so it can resume after a restart.
 *
 * One small file per (group, topic, partition) holds the NEXT offset to read.
 * Saves are atomic: the number is written to a temporary file and then moved over the
 * old file, so a crash in the middle of a save can never leave a half-written offset.
 */
public class FileOffsetStore {
    private final Path directory;

    /**
     * @param directory folder where the offset files are kept (created on first save)
     */
    public FileOffsetStore(Path directory) {
        if (directory == null) {
            throw new IllegalArgumentException("directory must not be null");
        }
        this.directory = directory;
    }

    /**
     * Load the saved offset.
     * @return the saved next-offset, or empty if nothing has been saved yet
     * @throws IOException if the file exists but does not contain a valid offset
     */
    public OptionalLong load(String group, String topic, int partition) throws IOException {
        Path file = fileFor(group, topic, partition);
        if (!Files.exists(file)) {
            return OptionalLong.empty();
        }
        String text = Files.readString(file, StandardCharsets.UTF_8).trim();
        long offset;
        try {
            offset = Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new IOException("Corrupt offset file " + file + ": '" + text + "'", e);
        }
        if (offset < 0) {
            throw new IOException("Corrupt offset file " + file + ": negative offset " + offset);
        }
        return OptionalLong.of(offset);
    }

    /**
     * Save the next offset to read, replacing any earlier value.
     */
    public void save(String group, String topic, int partition, long nextOffset) throws IOException {
        if (nextOffset < 0) {
            throw new IllegalArgumentException("offset must not be negative: " + nextOffset);
        }
        Path file = fileFor(group, topic, partition);
        Files.createDirectories(directory);

        Path temp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.writeString(temp, Long.toString(nextOffset), StandardCharsets.UTF_8);
        try {
            Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private Path fileFor(String group, String topic, int partition) {
        if (group == null || group.isEmpty()) {
            throw new IllegalArgumentException("group must not be empty");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic must not be empty");
        }
        return directory.resolve(safe(group) + "__" + safe(topic) + "__" + partition + ".offset");
    }

    /** Keep file names safe: anything other than letters, digits, dot, dash and underscore becomes _ */
    private static String safe(String name) {
        return name.replaceAll("[^A-Za-z0-9._-]", "_");
    }
}

