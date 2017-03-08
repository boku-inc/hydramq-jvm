package io.hydramq.network;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.hydramq.CursorInfo;
import io.hydramq.Message;
import io.hydramq.PartitionId;
import io.hydramq.PersistenceTestsBase;
import io.hydramq.Topic;
import io.hydramq.TopicManager;
import io.hydramq.TopicManagers;
import io.hydramq.TopicWriter;
import io.hydramq.TopicWriters;
import io.hydramq.readers.PartitionReader;
import io.hydramq.subscriptions.LockState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static io.hydramq.listeners.Listen.CONTINUOUSLY;

/**
 * @author jfulton
 */
@Test(groups = "IT")
public class NetworkTopicIT extends PersistenceTestsBase {

    private static final Logger logger = LoggerFactory.getLogger(NetworkTopicIT.class);
    private static final MetricRegistry registry = new MetricRegistry();
    private final InetSocketAddress endpoint = new InetSocketAddress("localhost", 7070);
    private Timer timer = registry.timer("TxRx");

    static {
        final JmxReporter reporter =
                JmxReporter.forRegistry(NetworkTopicIT.registry).inDomain("io.hydramq").build();
        reporter.start();
    }

    @Test
    public void testSendLotOfMessages() throws Exception {
        NetworkTopic topic = new NetworkTopic("Example.Foo");
        SimpleConnectionManager connectionManager = new SimpleConnectionManager(endpoint);
        connectionManager.manage(topic);

        TopicWriter writer = TopicWriters.simple(topic);

        sendMessages(writer, 1_000_000, 1000);
        logger.info("Done sending...");
    }


    @Test
    public void testAcquireLocksWithCursors() throws Exception {
//        TopicManager topicManager = TopicManagers.network(endpoint);
//        NetworkTopic topic = (NetworkTopic)topicManager.topic("Example.Foo");
//
        NetworkTopic topic = new NetworkTopic("Example.Foo");
        SimpleConnectionManager connectionManager = new SimpleConnectionManager(endpoint);
        connectionManager.manage(topic);
        AtomicInteger leases = new AtomicInteger();

        Map<PartitionId, PartitionReader> readers = new ConcurrentHashMap<>();

        String clientGroup = "bar";
        topic.acquirePartitionLocks(clientGroup, (partitionId, state) -> {
            logger.info("{}: {}", state, partitionId);
            CompletableFuture<Void> future = new CompletableFuture<>();
            if (state == LockState.LOCKED) {
                readers.computeIfAbsent(partitionId, pId -> {
                    PartitionReader reader = null;
                    CursorInfo cursorInfo = topic.cursor(partitionId, clientGroup).join();
                    logger.info("CURSOR: {} - {}", partitionId, cursorInfo.getOffset());
                    return new PartitionReader(topic, pId, cursorInfo.getOffset(), (partitionId1, messageSet) -> {
//                        logger.info("{}, {} containing {} messages", partitionId1, messageSet.startOffset(), messageSet.size());
                        topic.cursor(partitionId1, clientGroup, messageSet.nextOffset()).join();
                    });

                }).start().whenComplete((aVoid, throwable) -> {
                    future.complete(null);
                    leases.incrementAndGet();
                });
            } else {
                PartitionReader partitionReader = readers.remove(partitionId);
                if (partitionReader != null) {
                    partitionReader.stop().whenComplete((aVoid, throwable) -> {
                        future.complete(null);
                        leases.decrementAndGet();
                    });
                } else {
                    future.complete(null);
                }
            }
            return future;
        });

        for (int i = 0; i < 10_000; i++) {
            Thread.sleep(1000);
            logger.info("{} leases, {} partitions, {} acquired, {} released", leases, readers.size(), topic.getAcquired().get(), topic.getReleased().get());
        }
    }

    @Test
    public void testAcquireLocks() throws Exception {
//        TopicManager topicManager = TopicManagers.network(endpoint);
//        NetworkTopic topic = (NetworkTopic)topicManager.topic("Example.Foo");
//
        AtomicInteger leases = new AtomicInteger();
        NetworkTopic topic = new NetworkTopic("Example.Foo");
        SimpleConnectionManager connectionManager = new SimpleConnectionManager(endpoint);
        connectionManager.manage(topic);

        topic.acquirePartitionLocks("foo", (partitionId, state) -> {
            logger.info("{}: {}", state, partitionId);

            if (state == LockState.LOCKED) {
                leases.incrementAndGet();
            } else {
                leases.decrementAndGet();
            }
            return CompletableFuture.completedFuture(null);
        });

        for (int i = 0; i < 10_000; i++) {
            Thread.sleep(1000);
            logger.info("Leases: {}, Acquired: {}, Released: {}", leases, topic.getAcquired(), topic.getReleased());
        }
    }

    @Test
    public void testManagedConnectDisconnect() throws Exception {
        ConnectionManager connectionManager = new SimpleConnectionManager(endpoint);
        NetworkTopic producer = new NetworkTopic("Example.Blah");

        connectionManager.manage(producer);

        producer.disconnect().join();
        Thread.sleep(1000);
    }

    @Test
    public void testWriteAndRead() throws Exception {
        NetworkTopic topic = new NetworkTopic("Example.Messages");
        topic.connect(endpoint).join();

        topic.discoverPartitions((partitionId, partitionState) -> {
            topic.write(partitionId, Message.empty().build());
        }, CONTINUOUSLY);

        TopicWriter writer = TopicWriters.simple(topic);

        for (int i = 0; i < 10_000_000; i++) {
            writer.write(message(i)).join();
        }
    }


    @Test
    public void testReadFromBeginning() throws Exception {
        NetworkTopic topic = new NetworkTopic("Example.Messages");
        topic.connect(endpoint).join();

        topic.discoverPartitions((partitionId, flags) -> {
            logger.info("{}: {}", partitionId, flags);

            new PartitionReader(topic, partitionId, 0, (partitionId1, messageSet) -> {
//                logger.info("{}, {} containing {} messages", partitionId1, messageSet.startOffset(), messageSet.size());
            }).start();
        });

        for (int i = 0; i < 10_000; i++) {
            Thread.sleep(1000);
        }
    }


    @Test
    public void testName2() throws Exception {
        TopicManager tm = TopicManagers.network(endpoint);
        Topic topic = tm.topic("Example.Messages");

        TopicWriter simple = TopicWriters.simple(topic);
        for (int i = 0; i < 10_000; i++) {
            simple.write(Message.empty().build()).join();
        }

        AtomicInteger messagesConsumed = new AtomicInteger();
        Map<PartitionId, PartitionReader> readers = new ConcurrentHashMap<>();

        Executors.newSingleThreadExecutor().execute(() -> {
            topic.acquirePartitionLocks("foo", (partitionId, state) -> {
                logger.info("{}: {}", state, partitionId);
                if (state == LockState.LOCKED) {
                    readers.computeIfAbsent(partitionId, pId -> {
                        CursorInfo cursorInfo = topic.cursor(partitionId, "foo").join();
                        return new PartitionReader(topic, pId, cursorInfo.getOffset(), (partitionId1, messageSet) -> {
                            logger.info("{}, {} containing {} messages", partitionId1, messageSet.startOffset(), messageSet.size());
                            topic.cursor(partitionId1, "foo", messageSet.startOffset() + messageSet.size()).join();
                            messagesConsumed.accumulateAndGet(messageSet.size(), (left, right) -> left + right);
                            logger.info("Cursor position committed");
                        });
                    }).start().join();
                } else {
                    if (readers.containsKey(partitionId)) {
                        PartitionReader partitionReader = readers.remove(partitionId);
                        partitionReader.stop().join();
                        logger.info("RELEASED: {}", partitionId);
                    }

                }
                return CompletableFuture.completedFuture(null);
            });
        });

        for (int i = 0; i < 10_000; i++) {
            logger.info("Consumed: {}", messagesConsumed.get());
            Thread.sleep(1000);
        }

    }

    @Test
    public void testJoinsWithinJoins() throws Exception {
        TopicManager topicManager = TopicManagers.network(endpoint);
//        TopicManager topicManager = TopicManagers.disk(messageStoreDirectory());
        Topic topic = topicManager.topic("Example.Messages");
        PartitionId partitionId = getPartitionId(topic);

//        Executors.newSingleThreadExecutor().execute(() -> {
        for (int i = 0; i < 100; i++) {
            topic.write(partitionId, Message.empty().build()).thenAccept(aVoid -> {
                topic.partitionInfo(partitionId).thenAccept(partitionInfo -> {
                    logger.info("{}: {}", partitionId, partitionInfo);
                }).join();
            });
        }
//        });

        Thread.sleep(1000);
    }

    public void sendMessages(TopicWriter topicWriter, int batches, int messagesPerBatch) throws InterruptedException {
        for (int j = 0; j < batches; j++) {
            CountDownLatch latch = new CountDownLatch(messagesPerBatch);
            Timer.Context time = timer.time();
            for (int i = 0; i < messagesPerBatch; i++) {
                topicWriter.write(message(i)).whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        logger.error("Error sending message", throwable);
                    }
                    latch.countDown();
                });
            }
            latch.await();
            logger.info("Sent {} messages in {}ms", messagesPerBatch, time.stop() / 1000000);
        }
    }

    private Message message(int messageNumber) {
        return Message.withBodyAsString("Hello World!")
                .withString("customerId", "value" + messageNumber)
                .withDouble("key", 32.0)
                .withFloat("key", 15f)
                .withInteger("key", 11)
                .withBoolean("hasKey", true)
                .withLong("created", System.currentTimeMillis())
                .withLong("createdns", System.nanoTime())
                .build();
    }
}