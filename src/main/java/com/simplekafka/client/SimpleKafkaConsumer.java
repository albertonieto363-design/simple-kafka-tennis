package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Example consumer for Build Your Own Kafka
 */
public class SimpleKafkaConsumer {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaConsumer.class.getName());
    private static final int MAX_BYTES = 1024 * 1024; // 1MB max fetch size
    private static final int POLL_INTERVAL_MS = 100;
    
    private final SimpleKafkaClient client;
    private final String topic;
    private final int partition;
    private final String groupId;
    private final FileOffsetStore offsetStore;
    private long currentOffset;
    private final AtomicBoolean running;
    private Thread consumerThread;
    
    /**
     * Create a SimpleKafka consumer
     * @param bootstrapBroker Hostname of a broker to connect to
     * @param bootstrapPort Port of the broker to connect to
     * @param topic Topic to consume from
     * @param partition Partition to consume from
     */
    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition) {
        this(bootstrapBroker, bootstrapPort, topic, partition, 0);
    }
    
    /**
     * Create a SimpleKafka consumer with a specific starting offset
     * @param bootstrapBroker Hostname of a broker to connect to
     * @param bootstrapPort Port of the broker to connect to
     * @param topic Topic to consume from
     * @param partition Partition to consume from
     * @param startOffset Offset to start consuming from
     */
    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition, long startOffset) {
        this(bootstrapBroker, bootstrapPort, topic, partition, startOffset, null, null);
    }
    
    /**
     * Create a consumer that remembers its position.
     * If the store already has an offset for this group, topic and partition, the consumer
     * resumes from it. Otherwise it starts at offset 0.
     * @param groupId Name that identifies this consumer's saved position
     * @param offsetStore Where the position is saved
     */
    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition,
                               String groupId, FileOffsetStore offsetStore) {
        this(bootstrapBroker, bootstrapPort, topic, partition, 0, groupId, offsetStore);
    }
    
    private SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition,
                                long startOffset, String groupId, FileOffsetStore offsetStore) {
        if ((groupId == null) != (offsetStore == null)) {
            throw new IllegalArgumentException("groupId and offsetStore must be given together");
        }
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
        this.partition = partition;
        this.groupId = groupId;
        this.offsetStore = offsetStore;
        this.currentOffset = startOffset;
        this.running = new AtomicBoolean(false);
        
        if (offsetStore != null) {
            try {
                OptionalLong saved = offsetStore.load(groupId, topic, partition);
                if (saved.isPresent()) {
                    this.currentOffset = saved.getAsLong();
                }
            } catch (IOException e) {
                throw new IllegalStateException("Could not read saved offset for group " + groupId, e);
            }
        }
    }
    
    /**
     * Initialize the consumer
     */
    public void initialize() throws IOException {
        client.initialize();
        
        // Check if topic exists
        if (client.getTopicMetadata(topic) == null) {
            throw new IOException("Topic does not exist: " + topic);
        }
    }
    
    /**
     * Set the offset to start consuming from
     */
    public void seek(long offset) {
        this.currentOffset = offset;
    }
    
    /**
     * Save the current position so a restarted consumer resumes from here.
     * Does nothing if this consumer was created without an offset store.
     */
    public void commit() throws IOException {
        if (offsetStore != null) {
            offsetStore.save(groupId, topic, partition, currentOffset);
        }
    }
    
    /**
     * Poll for new messages (single poll)
     */
    public List<byte[]> poll() throws IOException {
        List<byte[]> messages = client.fetch(topic, partition, currentOffset, MAX_BYTES);
        if (!messages.isEmpty()) {
            currentOffset += messages.size();
        }
        return messages;
    }
    
    /**
     * Start consuming messages in a loop.
     * With an offset store, the position is saved after each batch has been handled,
     * so a crash can cause a batch to be delivered again, but never skipped.
     */
    public void startConsuming(MessageHandler handler) {
        if (running.compareAndSet(false, true)) {
            consumerThread = new Thread(() -> {
                try {
                    while (running.get()) {
                        List<byte[]> messages = poll();
                        long firstOffset = currentOffset - messages.size();
                        
                        for (int i = 0; i < messages.size(); i++) {
                            handler.handle(messages.get(i), firstOffset + i);
                        }
                        
                        if (!messages.isEmpty()) {
                            try {
                                commit();
                            } catch (IOException e) {
                                LOGGER.log(Level.WARNING, "Could not save offset " + currentOffset, e);
                            }
                        }
                        
                        // If no messages, wait a bit before polling again
                        if (messages.isEmpty()) {
                            Thread.sleep(POLL_INTERVAL_MS);
                        }
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        LOGGER.log(Level.SEVERE, "Error in consumer loop", e);
                    }
                    running.set(false);
                }
            });
            
            consumerThread.setDaemon(true);
            consumerThread.start();
            
            LOGGER.info("Started consuming from topic: " + topic + ", partition: " + partition
                    + ", offset: " + currentOffset);
        }
    }
    
    /**
     * Stop consuming messages
     */
    public void stopConsuming() {
        if (running.compareAndSet(true, false)) {
            if (consumerThread != null) {
                try {
                    consumerThread.interrupt();
                    consumerThread.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            
            LOGGER.info("Stopped consuming from topic: " + topic + ", partition: " + partition);
        }
    }
    
    /**
     * Get the current offset
     */
    public long getCurrentOffset() {
        return currentOffset;
    }
    
    /**
     * Check if consumer is running
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * Close the consumer
     */
    public void close() {
        stopConsuming();
    }
    
    /**
     * Interface for handling consumed messages
     */
    public interface MessageHandler {
        void handle(byte[] message, long offset);
    }
    
    /**
     * Main method for demonstration
     */
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: SimpleKafkaConsumer <broker> <port> <topic> <partition> [groupId]");
            System.out.println("With a groupId, the consumer saves its position in the ./offsets folder and resumes from it.");
            System.exit(1);
        }
        
        String broker = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];
        int partition = Integer.parseInt(args[3]);
        
        try {
            SimpleKafkaConsumer consumer;
            if (args.length >= 5) {
                consumer = new SimpleKafkaConsumer(broker, port, topic, partition,
                        args[4], new FileOffsetStore(Paths.get("offsets")));
                System.out.println("Group '" + args[4] + "' resuming from offset " + consumer.getCurrentOffset());
            } else {
                consumer = new SimpleKafkaConsumer(broker, port, topic, partition);
            }
            consumer.initialize();
            
            System.out.println("Consumer initialized. Starting consumption...");
            
            // Consume messages and print them
            consumer.startConsuming((message, offset) -> {
                String messageStr = new String(message, StandardCharsets.UTF_8);
                System.out.println("Received message at offset " + offset + ": " + messageStr);
            });
            
            System.out.println("Consumer started. Press enter to stop.");
            System.in.read();
            
            consumer.close();
            System.out.println("Consumer stopped");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Consumer error", e);
        }
    }
}