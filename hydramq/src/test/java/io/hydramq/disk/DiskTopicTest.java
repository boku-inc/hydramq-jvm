/*
 * The MIT License (MIT)
 *
 * Copyright © 2016-, Boku Inc., Jimmie Fulton
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package io.hydramq.disk;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.hydramq.Message;
import io.hydramq.PartitionId;
import io.hydramq.PartitionInfo;
import io.hydramq.Topic;
import io.hydramq.TopicManager;
import io.hydramq.TopicManagers;
import io.hydramq.listeners.Listen;
import io.hydramq.listeners.PartitionFlags;
import io.hydramq.subscriptions.LockState;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
@SuppressWarnings("Duplicates")
public class DiskTopicTest extends PersistenceTestsBase {

    private static final Logger logger = getLogger(DiskTopicTest.class);

    @Test
    public void testPartitionNotFound() throws Exception {
        // TODO: implement
    }

    @Test
    public void testBasicConstruction() throws Exception {
        Path topicDirectory = topicDirectory();
        Path partitionDirectory = topicDirectory.resolve("partitions");
        try (DiskTopic topic = new DiskTopic(topicDirectory.getFileName().toString(), topicDirectory, new DiskPartitionBuilder(),
                new DefaultPartitioningStrategy(4, 4))) {
            AtomicInteger partitionCount = new AtomicInteger();
            Files.list(partitionDirectory).forEach(path -> partitionCount.incrementAndGet());
            assertThat(partitionCount.get(), is(4));

            CountDownLatch latch = new CountDownLatch(topic.partitionIds().size());
            AtomicInteger errorCount = new AtomicInteger(0);
            for (PartitionId partitionId : topic.partitionIds()) {
                topic.write(partitionId, Message.empty().build()).whenComplete((aVoid, throwable) -> {
                    latch.countDown();
                    if (throwable != null) {
                        errorCount.incrementAndGet();
                    }
                });
            }
            latch.await();
            assertThat(errorCount.get(), is(0));
        }
    }

    @Test
    public void testMinimumPartitionsOnExistingTopicsWithInsufficientPartitionCount() throws Exception {
        Path topicDirectory = topicDirectory();
        Path partitionDirectory = topicDirectory.resolve("partitions");
        try (Topic topic = new DiskTopic(topicDirectory.getFileName().toString(), topicDirectory, new DiskPartitionBuilder(),
                new DefaultPartitioningStrategy(2, 2))) {
            AtomicInteger partitionCount = new AtomicInteger();
            Files.list(partitionDirectory).forEach(path -> partitionCount.incrementAndGet());
            assertThat(partitionCount.get(), is(2));
        }
        try (Topic topic = new DiskTopic(topicDirectory.getFileName().toString(), topicDirectory, new DiskPartitionBuilder(),
                new DefaultPartitioningStrategy(2, 4))) {
            AtomicInteger partitionCount = new AtomicInteger();
            Files.list(partitionDirectory).forEach(path -> partitionCount.incrementAndGet());
            assertThat(partitionCount.get(), is(4));
        }
    }

    @Test
    public void testMinimumPartitionsOnExistingTopicsDroppedPartitionCount() throws Exception {
        Path topicDirectory = topicDirectory();
        Path partitionDirectory = topicDirectory.resolve("partitions");
        try (Topic topic = new DiskTopic(topicDirectory.getFileName().toString(), topicDirectory, new DiskPartitionBuilder(),
                new DefaultPartitioningStrategy(4, 4))) {
            AtomicInteger partitionCount = new AtomicInteger();
            Files.list(partitionDirectory).forEach(path -> partitionCount.incrementAndGet());
            assertThat(partitionCount.get(), is(4));
        }
        try (Topic topic = new DiskTopic(topicDirectory.getFileName().toString(), topicDirectory, new DiskPartitionBuilder(),
                new DefaultPartitioningStrategy(2, 2))) {
            AtomicInteger partitionCount = new AtomicInteger();
            Files.list(partitionDirectory).forEach(path -> partitionCount.incrementAndGet());
            assertThat(partitionCount.get(), is(4));
        }
    }

    @Test
    public void testIncreasePartitionCountWithInsufficientPartitions() throws Exception {
        Path topicDirectory = topicDirectory();
        Path partitionDirectory = topicDirectory.resolve("partitions");
        try (Topic topic = new DiskTopic(topicDirectory.getFileName().toString(), topicDirectory, new DiskPartitionBuilder(),
                new DefaultPartitioningStrategy(4, 4))) {
            AtomicInteger partitionCount = new AtomicInteger();
            Files.list(partitionDirectory).forEach(path -> partitionCount.incrementAndGet());
            assertThat(partitionCount.get(), is(4));
            topic.partitions(8);
            assertThat(topic.writablePartitions(), is(8));
            partitionCount.set(0);
            Files.list(partitionDirectory).forEach(path -> partitionCount.incrementAndGet());
            assertThat(partitionCount.get(), is(8));
        }
    }

    @Test
    public void testIncreasePartitionCountWithExcessivePartitions() throws Exception {
        Path topicDirectory = topicDirectory();
        Path partitionDirectory = topicDirectory.resolve("partitions");
        try (Topic topic = new DiskTopic(topicDirectory.getFileName().toString(), topicDirectory, new DiskPartitionBuilder(),
                new DefaultPartitioningStrategy(4, 4))) {
            AtomicInteger partitionCount = new AtomicInteger();
            Files.list(partitionDirectory).forEach(path -> partitionCount.incrementAndGet());
            assertThat(partitionCount.get(), is(4));
            topic.partitions(2);
            assertThat(topic.partitions(), is(2));
            partitionCount.set(0);
            Files.list(partitionDirectory).forEach(path -> partitionCount.incrementAndGet());
            assertThat(partitionCount.get(), is(4));
        }
    }

    @Test
    public void testPartitionInfo() throws Exception {
        Path topicDirectory = topicDirectory();
        try (Topic topic = new DiskTopic(topicDirectory.getFileName().toString(), topicDirectory, new DiskPartitionBuilder(),
                new DefaultPartitioningStrategy(4, 4))) {
            logger.info("{}", topicDirectory);

            PartitionId partitionId = getPartitionId(topic);

            PartitionInfo partitionInfo = topic.partitionInfo(partitionId).get();
            assertThat(partitionInfo.head(), is(0L));
            assertThat(partitionInfo.tail(), is(0L));

            topic.write(partitionId, Message.empty().build()).join();
            partitionInfo = topic.partitionInfo(partitionId).get();
            assertThat(partitionInfo.head(), is(0L));
            assertThat(partitionInfo.tail(), is(1L));

            partitionInfo = topic.partitionInfo(getPartitionId(topic, 1)).get();
            assertThat(partitionInfo.head(), is(0L));
            assertThat(partitionInfo.tail(), is(0L));
        }
    }

    @Test
    public void testLoadDirectoryOfPartitionsContainingInvalidPartitionId() throws Exception {
        int partitionCount;
        Path topicsDirectory = topicDirectory();
        try (TopicManager topicManager = TopicManagers.disk(topicsDirectory)) {
            partitionCount = topicManager.topic("foo").writablePartitions();
        }

        Files.createDirectories(topicsDirectory.resolve("foo/partitions/blah"));

        try (TopicManager topicManager = TopicManagers.disk(topicsDirectory)) {
            Topic foo = topicManager.topic("foo");
            assertThat(foo.writablePartitions(), is(partitionCount));
        }
    }

    @Test
    public void testDiscoverPartitionsOnceAllAvailable() throws Exception {
        Path topicsDirectory = topicDirectory();
        try (TopicManager topicManager = TopicManagers.disk(topicsDirectory)) {
            Topic blah = topicManager.topic("blah");

            Map<PartitionId, PartitionFlags> discoveredPartitions = new HashMap<>();
            blah.discoverPartitions(discoveredPartitions::put, Listen.ONCE);

            for (PartitionId partitionId : discoveredPartitions.keySet()) {
                Path partitionDirectory = topicsDirectory.resolve("blah/partitions").resolve(partitionId.toString());
                assertThat(Files.exists(partitionDirectory), is(true));
            }
        }
    }

    @Test
    public void testDiscoverPartitionsOncePartialReadOnly() throws Exception {
        Path topicsDirectory = topicDirectory();
        try (TopicManager topicManager = TopicManagers.disk(topicsDirectory)) {
            Topic blah = topicManager.topic("blah");

            Map<PartitionId, PartitionFlags> discoveredPartitions = new HashMap<>();
            blah.discoverPartitions(discoveredPartitions::put, Listen.ONCE);

            for (PartitionId partitionId : discoveredPartitions.keySet()) {
                Path partitionDirectory = topicsDirectory.resolve("blah/partitions").resolve(partitionId.toString());
                assertThat(Files.exists(partitionDirectory), is(true));
            }
        }
    }

    @Test
    public void testWritablePartitionsOnNewTopics() throws Exception {
        try (TopicManager topicManager = TopicManagers.disk(topicDirectory())) {
            Topic blah = topicManager.topic("blah");
            assertThat(blah.writablePartitions(), is(blah.partitions()));
        }
    }

    @Test
    public void testDiscoverPartitionsOnceOnNewTopics() throws Exception {
        try (TopicManager topicManager = TopicManagers.disk(topicDirectory())) {
            Topic blah = topicManager.topic("blah");
            AtomicInteger readWriteCount = new AtomicInteger();
            blah.discoverPartitions((partitionId, partitionState) -> {
                logger.info("{}: {}", partitionId, partitionState);
                if (partitionState.isWritable() && partitionState.isReadable()) {
                    readWriteCount.incrementAndGet();
                }
            }, Listen.ONCE);
            assertThat(readWriteCount.get(), is(blah.partitions()));
        }
    }


    @Test
    public void testChangeWritablePartitionsContinuously() throws Exception {
        Path topicsDirectory = topicDirectory();
        try (TopicManager topicManager = TopicManagers.disk(topicsDirectory)) {
            Topic topic = topicManager.topic("simple");

            Map<PartitionId, PartitionFlags> discoveredPartitions = new HashMap<>();
            topic.discoverPartitions(discoveredPartitions::put, Listen.CONTINUOUSLY);

            topic.discoverPartitions((partitionId, state) -> logger.info("{}: {}", partitionId, state), Listen.CONTINUOUSLY);

            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.WRITE, topic.partitions());


            topic.writablePartitions(topic.partitions() - 1);

            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.WRITE, topic.partitions() - 1);


            topic.writablePartitions(topic.partitions());
            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.WRITE, 4);

            Random rnd = new Random();
            for (int i = 0; i < 100; i++) {
                int writable = rnd.nextInt(topic.partitions());
                logger.info("Making Writable: {}", writable);
                topic.writablePartitions(writable);
                assertFlagCount(discoveredPartitions, PartitionFlags.Flag.WRITE, writable);
                assertFlagCount(discoveredPartitions, PartitionFlags.Flag.READ, topic.partitions());
                assertThat(topic.writablePartitions(), is(writable));
            }
        }
    }


    @Test
    public void testChangeWritablePartitionsOnce() throws Exception {
        Path topicsDirectory = topicDirectory();
        try (TopicManager topicManager = TopicManagers.disk(topicsDirectory)) {
            Topic topic = topicManager.topic("simple");

            Map<PartitionId, PartitionFlags> discoveredPartitions = new HashMap<>();
            topic.discoverPartitions(discoveredPartitions::put, Listen.ONCE);
            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.WRITE, topic.partitions());

            discoveredPartitions.clear();
            topic.writablePartitions(topic.partitions() - 1);
            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.READ, 0);
            topic.discoverPartitions(discoveredPartitions::put, Listen.ONCE);
            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.WRITE, topic.partitions() - 1);
        }
    }

    @Test
    public void testChangeReadablePartitionsContinuously() throws Exception {
        Path topicsDirectory = topicDirectory();
        try (TopicManager topicManager = TopicManagers.disk(topicsDirectory)) {
            Topic blah = topicManager.topic("blah");

            Map<PartitionId, PartitionFlags> discoveredPartitions = new HashMap<>();
            blah.discoverPartitions(discoveredPartitions::put, Listen.CONTINUOUSLY);

            blah.discoverPartitions((partitionId, state) -> logger.info("{}: {}", partitionId, state), Listen.CONTINUOUSLY);

            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.READ, blah.partitions());


            blah.readablePartitions(blah.partitions() - 1);
            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.READ, blah.partitions() - 1);


            blah.readablePartitions(blah.partitions());
            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.READ, blah.partitions());

            Random rnd = new Random();
            for (int i = 0; i < 100; i++) {
                int readable = rnd.nextInt(blah.partitions());
                logger.info("Making Readable: {}", readable);
                blah.readablePartitions(readable);
                assertFlagCount(discoveredPartitions, PartitionFlags.Flag.READ, readable);
                assertThat(blah.readablePartitions(), is(readable));
            }
        }
    }


    @Test
    public void testChangeReadablePartitionsOnce() throws Exception {
        Path topicsDirectory = topicDirectory();
        try (TopicManager topicManager = TopicManagers.disk(topicsDirectory)) {
            Topic blah = topicManager.topic("blah");

            Map<PartitionId, PartitionFlags> discoveredPartitions = new HashMap<>();
            blah.discoverPartitions(discoveredPartitions::put, Listen.ONCE);
            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.READ, blah.partitions());

            discoveredPartitions.clear();
            blah.readablePartitions(blah.partitions() - 1);
            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.READ, 0);
            blah.discoverPartitions(discoveredPartitions::put, Listen.ONCE);
            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.READ, blah.partitions() - 1);
        }
    }

    @Test
    public void testChangeUnavailableContinuously() throws Exception {
        Random rnd = new Random();
        Path topicsDirectory = topicDirectory();
        try (TopicManager topicManager = TopicManagers.disk(topicsDirectory)) {
            Topic topic = topicManager.topic("simple");

            Map<PartitionId, PartitionFlags> discoveredPartitions = new HashMap<>();
            topic.discoverPartitions(discoveredPartitions::put, Listen.CONTINUOUSLY);

            topic.discoverPartitions((partitionId, state) -> logger.info("{}: {}", partitionId, state), Listen.CONTINUOUSLY);

            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.READ, topic.partitions());
            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.WRITE, topic.partitions());

            topic.writablePartitions(0);

            topic.readablePartitions(topic.partitions() - 1);
            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.WRITE, 0);
            assertFlagCount(discoveredPartitions, PartitionFlags.Flag.READ, 3);

            for (int i = 0; i < 100; i++) {
                int readable = rnd.nextInt(topic.partitions());
                logger.info("Making Readable: {}", readable);
                topic.readablePartitions(readable);
                assertFlagCount(discoveredPartitions, PartitionFlags.Flag.READ, readable);
                assertFlagCount(discoveredPartitions, PartitionFlags.Flag.WRITE, 0);
            }

            topic.readablePartitions(0);
            topic.writablePartitions(topic.partitions());

            for (int i = 0; i < 100; i++) {
                int writable = rnd.nextInt(topic.partitions());
                logger.info("Making Writable: {}", writable);
                topic.writablePartitions(writable);
                assertFlagCount(discoveredPartitions, PartitionFlags.Flag.WRITE, writable);
                assertFlagCount(discoveredPartitions, PartitionFlags.Flag.READ, 0);
            }

        }
    }

    @Test
    public void testLocking() throws Exception {
        try (TopicManager topicManager = TopicManagers.disk(topicDirectory())) {
            DiskTopic topic = (DiskTopic) topicManager.topic("topic");
            topic.partitions(1000);

            AtomicInteger locks1 = new AtomicInteger();
            AtomicInteger locks2 = new AtomicInteger();
            ExecutorService executorService = Executors.newCachedThreadPool();
            executorService.execute(() -> {
                topic.acquirePartitionLocks("foo", (partitionId, state) -> {
                    if (state == LockState.LOCKED) {
                        locks1.incrementAndGet();
                    } else {
                        locks1.decrementAndGet();
                    }
                    logger.info("{} (1): {}", state, partitionId);
                    return CompletableFuture.completedFuture(null);
                });
            });
            executorService.execute(() -> {
                topic.acquirePartitionLocks("foo", (partitionId, state) -> {
                    if (state == LockState.LOCKED) {
                        locks2.incrementAndGet();
                    } else {
                        locks2.decrementAndGet();
                    }
                    logger.info("{} (2): {}", state, partitionId);
                    return CompletableFuture.completedFuture(null);
                });
            });

            Thread.sleep(1000);

            assertThat(locks1.get(), is(locks2.get()));
        }
    }

    @Test
    public void testCursors() throws Exception {
        try (TopicManager topicManager = TopicManagers.disk(topicDirectory())) {
            DiskTopic topic = (DiskTopic) topicManager.topic("topic");
            PartitionId partitionId = getPartitionId(topic);
            assertThat(topic.cursor(partitionId, "foo").get().getOffset(), is(0L));
            topic.cursor(partitionId, "foo", 1000L).join();
            assertThat(topic.cursor(partitionId, "foo").get().getOffset(), is(1000L));
        }
    }

    private void assertFlagCount(Map<PartitionId, PartitionFlags> partitions, PartitionFlags.Flag expectedFlag, int expectedCount) {
        int results = 0;
        for (PartitionFlags flags : partitions.values()) {
            if (flags.hasFlag(expectedFlag)) {
                results++;
            }
        }
        assertThat(results, is(expectedCount));
    }
}