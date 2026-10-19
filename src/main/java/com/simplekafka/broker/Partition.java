package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents a partition in Build Your Own Kafka, containing segments of message logs
 */
public class Partition {
    private static final Logger LOGGER = Logger.getLogger(Partition.class.getName());
    private static final int DEFAULT_SEGMENT_SIZE = 1024 * 1024; // 1MB segment size
    private static final String LOG_SUFFIX = ".log";
    private static final String INDEX_SUFFIX = ".index";
    
    private final int id;
    private int leader;
    private List<Integer> followers;
    private final String baseDir;
    private final AtomicLong nextOffset;
    private final ReadWriteLock lock;
    private RandomAccessFile activeLogFile;
    private FileChannel activeLogChannel;
    private final List<SegmentInfo> segments;
    private final int segmentSize;

    public Partition(int id, int leader, List<Integer> followers, String baseDir) {
        this(id, leader, followers, baseDir, DEFAULT_SEGMENT_SIZE);
    }

    /**
     * Same as above, but with a configurable segment size so tests can force
     * segment rollover with a few small messages instead of 1MB of data.
     */
    public Partition(int id, int leader, List<Integer> followers, String baseDir, int segmentSize) {
        this.id = id;
        this.leader = leader;
        this.followers = followers;
        this.baseDir = baseDir;
        this.segmentSize = segmentSize;
        this.nextOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ArrayList<>();
        
        initialize();
    }
    
    /**
     * Initialize partition and load existing segments
     */
    private void initialize() {
        try {
            // Create directory if it doesn't exist
            File dir = new File(baseDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            
            // Load existing segments
            File[] files = dir.listFiles((dir1, name) -> name.endsWith(LOG_SUFFIX));
            if (files != null && files.length > 0) {
                for (File file : files) {
                    String baseName = file.getName().substring(0, file.getName().length() - LOG_SUFFIX.length());
                    long baseOffset = Long.parseLong(baseName);
                    
                    File indexFile = new File(baseDir, baseName + INDEX_SUFFIX);
                    if (indexFile.exists()) {
                        SegmentInfo segment = new SegmentInfo(baseOffset, file.getAbsolutePath(), indexFile.getAbsolutePath());
                        segments.add(segment);
                    }
                }
                
                // Sort segments by base offset
                segments.sort((s1, s2) -> Long.compare(s1.getBaseOffset(), s2.getBaseOffset()));
                
                // Determine next offset from the last segment
                if (!segments.isEmpty()) {
                    SegmentInfo lastSegment = segments.get(segments.size() - 1);
                    nextOffset.set(lastSegment.getBaseOffset() + countMessagesInSegment(lastSegment));
                }
            }
            
            // Create a new segment if none exists
            if (segments.isEmpty()) {
                createNewSegment(0);
            } else {
                // Open the last segment as active
                SegmentInfo lastSegment = segments.get(segments.size() - 1);
                openSegmentForAppend(lastSegment);
            }
            
            LOGGER.info("Initialized partition " + id + " with " + segments.size() + 
                       " segments, next offset: " + nextOffset.get());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize partition " + id, e);
        }
    }
    
    /**
     * Count the number of messages in a segment
     */
    private long countMessagesInSegment(SegmentInfo segment) throws IOException {
        long count = 0;
        try (RandomAccessFile logFile = new RandomAccessFile(segment.getLogPath(), "r");
             FileChannel logChannel = logFile.getChannel()) {
            
            ByteBuffer buffer = ByteBuffer.allocate(4); // Size field is 4 bytes
            
            while (logChannel.position() < logChannel.size()) {
                buffer.clear();
                int bytesRead = logChannel.read(buffer);
                if (bytesRead < 4) break;
                
                buffer.flip();
                int messageSize = buffer.getInt();
                
                // Skip the message
                logChannel.position(logChannel.position() + messageSize);
                count++;
            }
        }
        
        return count;
    }
    
    /**
     * Create a new segment for this partition
     */
    private void createNewSegment(long baseOffset) throws IOException {
        String baseName = String.format("%020d", baseOffset);
        String logPath = baseDir + File.separator + baseName + LOG_SUFFIX;
        String indexPath = baseDir + File.separator + baseName + INDEX_SUFFIX;
        
        // Create log file
        File logFile = new File(logPath);
        logFile.createNewFile();
        
        // Create index file
        File indexFile = new File(indexPath);
        indexFile.createNewFile();
        
        // Add to segments list
        SegmentInfo segment = new SegmentInfo(baseOffset, logPath, indexPath);
        segments.add(segment);
        
        // Open for append
        openSegmentForAppend(segment);
        
        LOGGER.info("Created new segment for partition " + id + ", base offset: " + baseOffset);
    }
    
    /**
     * Open a segment for append operations
     */
    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        // Close the currently active segment if any
        if (activeLogChannel != null && activeLogChannel.isOpen()) {
            activeLogChannel.close();
        }
        
        if (activeLogFile != null) {
            activeLogFile.close();
        }
        
        // Open the segment
        activeLogFile = new RandomAccessFile(segment.getLogPath(), "rw");
        activeLogChannel = activeLogFile.getChannel();
        
        // Move to the end of the file for appending
        activeLogChannel.position(activeLogChannel.size());
    }
    
    /**
     * Append a message to the log
     * @return the offset where the message was appended
     */
    public long append(byte[] message) {
        lock.writeLock().lock();
        try {
            long currentOffset = nextOffset.get();
            
            // Check if we need to roll over to a new segment
            if (activeLogChannel.position() >= segmentSize) {
                activeLogChannel.close();
                activeLogFile.close();
                createNewSegment(currentOffset);
            }
            
            // Write message size and data
            ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
            buffer.putInt(message.length);
            buffer.put(message);
            buffer.flip();
            
            // Write to file
            long position = activeLogChannel.position();
            activeLogChannel.write(buffer);
            
            // Force write to disk
            activeLogChannel.force(true);
            
            // Update index
            updateIndex(currentOffset, position);
            
            // Update offset
            nextOffset.incrementAndGet();
            
            return currentOffset;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to append message to partition " + id, e);
            return -1;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Update the index file with new offset
     */
    private void updateIndex(long offset, long position) {
        try {
            // Find the current segment
            if (segments.isEmpty()) return;
            
            SegmentInfo currentSegment = segments.get(segments.size() - 1);
            
            try (RandomAccessFile indexFile = new RandomAccessFile(currentSegment.getIndexPath(), "rw");
                 FileChannel indexChannel = indexFile.getChannel()) {
                
                // Position at the end
                indexChannel.position(indexChannel.size());
                
                // Write offset and position
                ByteBuffer buffer = ByteBuffer.allocate(16);
                buffer.putLong(offset);
                buffer.putLong(position);
                buffer.flip();
                
                indexChannel.write(buffer);
                indexChannel.force(true);
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to update index for partition " + id, e);
        }
    }
    
    /**
     * Read messages from the log starting at offset.
     * Keeps reading across segment boundaries until maxBytes is reached
     * or the end of the log is hit.
     */
    public List<byte[]> readMessages(long offset, int maxBytes) {
        lock.readLock().lock();
        List<byte[]> messages = new ArrayList<>();
        int bytesRead = 0;
        
        try {
            // Find the segment containing the offset
            SegmentInfo segment = findSegmentForOffset(offset);
            if (segment == null) {
                return messages;
            }
            
            // Find the file position for the offset using the index
            long position = findPositionForOffset(segment, offset);
            if (position < 0) {
                return messages;
            }
            
            int segmentIndex = segments.indexOf(segment);
            long currentOffset = offset;
            boolean stop = false;
            
            // Read one segment per pass; each file is opened and closed exactly once
            while (!stop && segment != null) {
                try (RandomAccessFile logFile = new RandomAccessFile(segment.getLogPath(), "r");
                     FileChannel logChannel = logFile.getChannel()) {
                    
                    logChannel.position(position);
                    ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                    
                    while (logChannel.position() < logChannel.size()) {
                        // Read message size
                        sizeBuffer.clear();
                        int sizeRead = logChannel.read(sizeBuffer);
                        if (sizeRead < 4) {
                            stop = true;
                            break;
                        }
                        
                        sizeBuffer.flip();
                        int messageSize = sizeBuffer.getInt();
                        
                        // Stop if adding this message would exceed maxBytes
                        if (bytesRead + messageSize > maxBytes) {
                            stop = true;
                            break;
                        }
                        
                        // Read message data
                        ByteBuffer messageBuffer = ByteBuffer.allocate(messageSize);
                        int messageRead = logChannel.read(messageBuffer);
                        if (messageRead < messageSize) {
                            LOGGER.warning("Incomplete message read at offset " + currentOffset);
                            stop = true;
                            break;
                        }
                        
                        messageBuffer.flip();
                        byte[] message = new byte[messageSize];
                        messageBuffer.get(message);
                        messages.add(message);
                        
                        bytesRead += messageSize + 4; // message size + 4 bytes for size field
                        currentOffset++;
                    }
                }
                
                // Finished this segment. Continue with the next one if there is more log.
                if (stop || currentOffset >= nextOffset.get()) {
                    break;
                }
                segmentIndex++;
                if (segmentIndex < segments.size()) {
                    segment = segments.get(segmentIndex);
                    position = 0; // next segment is read from its beginning
                } else {
                    segment = null;
                }
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to read messages from partition " + id, e);
        } finally {
            lock.readLock().unlock();
        }
        
        return messages;
    }
    

    /**
     * Find the segment containing the given offset
     */
    private SegmentInfo findSegmentForOffset(long offset) {
        if (segments.isEmpty() || offset >= nextOffset.get()) {
            return null;
        }
        
        // Binary search to find the segment
        int low = 0;
        int high = segments.size() - 1;
        
        while (low <= high) {
            int mid = (low + high) / 2;
            SegmentInfo segment = segments.get(mid);
            
            if (mid < segments.size() - 1) {
                SegmentInfo nextSegment = segments.get(mid + 1);
                if (offset >= segment.getBaseOffset() && offset < nextSegment.getBaseOffset()) {
                    return segment;
                }
            } else {
                // Last segment
                if (offset >= segment.getBaseOffset()) {
                    return segment;
                }
            }
            
            if (offset < segment.getBaseOffset()) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        
        return null;
    }
    
    /**
     * Find the file position for the given offset using the index
     */
    private long findPositionForOffset(SegmentInfo segment, long offset) {
        try (RandomAccessFile indexFile = new RandomAccessFile(segment.getIndexPath(), "r");
             FileChannel indexChannel = indexFile.getChannel()) {
            
            if (indexChannel.size() == 0) {
                // Empty index, start from beginning of log
                return 0;
            }
            
            // Relative offset within the segment
            long relativeOffset = offset - segment.getBaseOffset();
            
            // Each index entry is 16 bytes (8 for offset, 8 for position)
            long entryCount = indexChannel.size() / 16;
            
            if (relativeOffset < 0 || relativeOffset >= entryCount) {
                // The offset is not in this segment's index: report "not found"
                // instead of silently returning the last entry's position.
                return -1;
            }
            
            // Read the specific index entry
            indexChannel.position(relativeOffset * 16);
            ByteBuffer buffer = ByteBuffer.allocate(16);
            indexChannel.read(buffer);
            buffer.flip();
            
            // Skip offset
            buffer.getLong();
            // Return position
            return buffer.getLong();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to find position for offset " + offset, e);
            return -1;
        }
    }
    
    /**
     * Get partition ID
     */
    public int getId() {
        return id;
    }
    
    /**
     * Get leader broker ID
     */
    public int getLeader() {
        return leader;
    }
    
    /**
     * Set leader broker ID
     */
    public void setLeader(int leader) {
        this.leader = leader;
    }
    
    /**
     * Get follower broker IDs
     */
    public List<Integer> getFollowers() {
        return new ArrayList<>(followers);
    }
    
    /**
     * Set follower broker IDs
     */
    public void setFollowers(List<Integer> followers) {
        this.followers = new ArrayList<>(followers);
    }
    
    /**
     * Get the current log end offset
     */
    public long getLogEndOffset() {
        return nextOffset.get();
    }
    
    /**
     * Close partition resources
     */
    public void close() {
        lock.writeLock().lock();
        try {
            if (activeLogChannel != null && activeLogChannel.isOpen()) {
                activeLogChannel.close();
            }
            
            if (activeLogFile != null) {
                activeLogFile.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to close partition resources", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Class to represent segment information
     */
    private static class SegmentInfo {
        private final long baseOffset;
        private final String logPath;
        private final String indexPath;
        
        public SegmentInfo(long baseOffset, String logPath, String indexPath) {
            this.baseOffset = baseOffset;
            this.logPath = logPath;
            this.indexPath = indexPath;
        }
        
        public long getBaseOffset() {
            return baseOffset;
        }
        
        public String getLogPath() {
            return logPath;
        }
        
        public String getIndexPath() {
            return indexPath;
        }
    }
}