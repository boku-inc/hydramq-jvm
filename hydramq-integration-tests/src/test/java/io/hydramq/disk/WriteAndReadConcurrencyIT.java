package io.hydramq.disk;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.hydramq.Message;
import io.hydramq.Partitioners;
import io.hydramq.PersistenceTestsBase;
import io.hydramq.Topic;
import io.hydramq.TopicManager;
import io.hydramq.TopicWriter;
import io.hydramq.TopicWriters;
import io.hydramq.client.DefaultTopicReader;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
public class WriteAndReadConcurrencyIT extends PersistenceTestsBase {

    private static final Logger logger = getLogger(WriteAndReadConcurrencyIT.class);

    @Test(timeOut = 1000 * 60 * 10) // Maximum of 10 minutes
    public void testName() throws Exception {
        int messagesCount = 10_000_000;
        int threadCount = 5;

        ExecutorService executorService = Executors.newCachedThreadPool();
        CountDownLatch latch = new CountDownLatch(messagesCount);
        AtomicInteger messagesRead = new AtomicInteger();

        TopicManager topicManager = new DiskTopicManager(messageStoreDirectory());
        Topic MTs = topicManager.topic("Example.Messages");

        DefaultTopicReader topicReader = new DefaultTopicReader(MTs);
        topicReader.start((partitionId, messageSet) -> {
            messageSet.forEach(message -> {
                latch.countDown();
                messagesRead.incrementAndGet();
            });
            float latency = (System.nanoTime() - messageSet.iterator().next().getLong("created")) / 1_000_000f;
            logger.info("Partition: {}, Messages: {}, Offset: {}, Latch: {}, Latency: {}", partitionId, messageSet.size(), messageSet.startOffset(), latch.getCount(), latency);
        });

        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                TopicWriter writer = TopicWriters.simple(MTs, Partitioners.roundRobin());
                for (int j = 0; j < messagesCount / threadCount; j++) {
                    writer.write(Message.empty().withLong("created", System.nanoTime()).build()).join();
                }
            });
        }

        logger.info("Waiting to end...");
        latch.await();
        assertThat(messagesRead.get(), is(messagesCount));
        logger.info("Completed Successfully");
    }
}
