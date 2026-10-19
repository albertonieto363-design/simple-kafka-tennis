package com.simplekafka.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.simplekafka.broker.Protocol;

/**
 * Tests SimpleKafkaClient.fetch against a tiny fake broker running inside the test,
 * so no ZooKeeper or real broker is needed.
 */
class SimpleKafkaClientFetchTest {

    private static final String TOPIC = "tennis-prices";
    private static final int BIG_FETCH = 1_000_000;

    @Test
    void fetchesMoreDataThanASingleReadBuffer() throws Exception {
        // 200 messages x (12 + 40) bytes is about 10 KB, well past the old 4096-byte buffer
        List<byte[]> sent = messages(200, 40);
        try (FakeBroker broker = new FakeBroker(fetchResponse(sent), Integer.MAX_VALUE, 0)) {
            SimpleKafkaClient client = new SimpleKafkaClient("127.0.0.1", broker.port());
            client.initialize();

            List<byte[]> got = client.fetch(TOPIC, 0, 0, BIG_FETCH);

            assertEquals(200, got.size());
            for (int i = 0; i < sent.size(); i++) assertArrayEquals(sent.get(i), got.get(i));
        }
    }

    @Test
    void fetchesAResponseThatArrivesInSmallPieces() throws Exception {
        List<byte[]> sent = messages(5, 30);
        // The response is written 7 bytes at a time with a pause between pieces, like a slow network
        try (FakeBroker broker = new FakeBroker(fetchResponse(sent), 7, 15)) {
            SimpleKafkaClient client = new SimpleKafkaClient("127.0.0.1", broker.port());
            client.initialize();

            List<byte[]> got = client.fetch(TOPIC, 0, 0, BIG_FETCH);

            assertEquals(5, got.size());
            for (int i = 0; i < sent.size(); i++) assertArrayEquals(sent.get(i), got.get(i));
        }
    }

    @Test
    void fetchesAnEmptyResponse() throws Exception {
        try (FakeBroker broker = new FakeBroker(fetchResponse(new ArrayList<>()), Integer.MAX_VALUE, 0)) {
            SimpleKafkaClient client = new SimpleKafkaClient("127.0.0.1", broker.port());
            client.initialize();

            assertTrue(client.fetch(TOPIC, 0, 0, BIG_FETCH).isEmpty());
        }
    }

    @Test
    void reportsABrokerErrorAsAnIOException() throws Exception {
        try (FakeBroker broker = new FakeBroker(errorResponse("Offset out of range"), Integer.MAX_VALUE, 0)) {
            SimpleKafkaClient client = new SimpleKafkaClient("127.0.0.1", broker.port());
            client.initialize();

            IOException e = assertThrows(IOException.class, () -> client.fetch(TOPIC, 0, 0, BIG_FETCH));

            assertTrue(e.getMessage().contains("Offset out of range"), e.getMessage());
        }
    }

    @Test
    void aResponseCutOffMidwayFailsWithAnIOException() throws Exception {
        byte[] whole = fetchResponse(messages(10, 40));
        byte[] cutOff = java.util.Arrays.copyOf(whole, whole.length / 2);
        try (FakeBroker broker = new FakeBroker(cutOff, Integer.MAX_VALUE, 0)) {
            SimpleKafkaClient client = new SimpleKafkaClient("127.0.0.1", broker.port());
            client.initialize();

            assertThrows(IOException.class, () -> client.fetch(TOPIC, 0, 0, BIG_FETCH));
        }
    }

    // ---- helpers: build the exact bytes Protocol.decodeFetchResponse expects ----

    private static List<byte[]> messages(int count, int size) {
        List<byte[]> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            StringBuilder sb = new StringBuilder("price-" + i + "-");
            while (sb.length() < size) sb.append('x');
            list.add(sb.substring(0, size).getBytes(StandardCharsets.UTF_8));
        }
        return list;
    }

    private static byte[] fetchResponse(List<byte[]> messages) {
        int total = 1 + 4;
        for (byte[] m : messages) total += 8 + 4 + m.length;
        ByteBuffer buf = ByteBuffer.allocate(total);
        buf.put(Protocol.FETCH_RESPONSE);
        buf.putInt(messages.size());
        long offset = 0;
        for (byte[] m : messages) {
            buf.putLong(offset++);
            buf.putInt(m.length);
            buf.put(m);
        }
        return buf.array();
    }

    private static byte[] errorResponse(String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(3 + bytes.length);
        buf.put(Protocol.ERROR_RESPONSE);
        buf.putShort((short) bytes.length);
        buf.put(bytes);
        return buf.array();
    }

    private static byte[] metadataResponse(int port) {
        byte[] host = "127.0.0.1".getBytes(StandardCharsets.UTF_8);
        byte[] topic = TOPIC.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.put(Protocol.METADATA_RESPONSE);
        buf.putInt(1);                       // one broker
        buf.putInt(1);                       //   id
        buf.putShort((short) host.length);   //   host
        buf.put(host);
        buf.putInt(port);                    //   port
        buf.putInt(1);                       // one topic
        buf.putShort((short) topic.length);  //   name
        buf.put(topic);
        buf.putInt(1);                       //   one partition
        buf.putInt(0);                       //     partition id
        buf.putInt(1);                       //     leader = broker 1
        buf.putInt(0);                       //     no replicas
        return java.util.Arrays.copyOf(buf.array(), buf.position());
    }

    /** A minimal one-broker server: answers metadata requests, and answers any fetch with fixed bytes. */
    private static final class FakeBroker implements AutoCloseable {
        private final ServerSocketChannel server;
        private final byte[] fetchResponse;
        private final int chunkSize;
        private final int delayMillis;

        FakeBroker(byte[] fetchResponse, int chunkSize, int delayMillis) throws IOException {
            this.fetchResponse = fetchResponse;
            this.chunkSize = chunkSize;
            this.delayMillis = delayMillis;
            this.server = ServerSocketChannel.open();
            this.server.bind(new InetSocketAddress("127.0.0.1", 0));
            Thread thread = new Thread(this::serve);
            thread.setDaemon(true);
            thread.start();
        }

        int port() throws IOException {
            return ((InetSocketAddress) server.getLocalAddress()).getPort();
        }

        private void serve() {
            while (server.isOpen()) {
                try (SocketChannel client = server.accept()) {
                    client.setOption(StandardSocketOptions.TCP_NODELAY, true);
                    ByteBuffer request = ByteBuffer.allocate(256);
                    if (client.read(request) <= 0) continue;
                    byte type = request.get(0);
                    if (type == Protocol.METADATA) {
                        write(client, metadataResponse(port()), Integer.MAX_VALUE, 0);
                    } else if (type == Protocol.FETCH) {
                        write(client, fetchResponse, chunkSize, delayMillis);
                    }
                } catch (IOException | InterruptedException e) {
                    // the server was closed at the end of the test
                }
            }
        }

        private static void write(SocketChannel client, byte[] data, int chunk, int delay)
                throws IOException, InterruptedException {
            for (int start = 0; start < data.length; start += chunk) {
                int len = (int) Math.min((long) chunk, (long) data.length - start);
                ByteBuffer piece = ByteBuffer.wrap(data, start, len);
                while (piece.hasRemaining()) client.write(piece);
                if (delay > 0) Thread.sleep(delay);
            }
        }

        @Override
        public void close() throws IOException {
            server.close();
        }
    }
}