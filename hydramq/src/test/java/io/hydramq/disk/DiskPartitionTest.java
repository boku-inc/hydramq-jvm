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
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.hydramq.DefaultSegmentationStrategy;
import io.hydramq.Message;
import io.hydramq.MessageSet;
import io.hydramq.Partition;
import io.hydramq.PartitionInfo;
import io.hydramq.Segment;
import io.hydramq.SegmentedPartition;
import io.hydramq.client.DiskPartitionReader;
import io.hydramq.disk.flushing.FlushStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static java.lang.System.nanoTime;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
@SuppressWarnings("Duplicates")
public class DiskPartitionTest extends PersistenceTestsBase {

    private static final Logger logger = LoggerFactory.getLogger(DiskPartitionTest.class);

    @Test
    public void testConstruction() throws Exception {
        Path partitionDirectory = partitionDirectory();
        Partition partition = new DiskPartition(partitionDirectory,
                new DiskSegmentBuilder().flushStrategy(FlushStrategies::afterEveryWrite), new DefaultSegmentationStrategy(100),
                new SegmentCountArchiveStrategy(1), new DeletingSegmentArchiver());
        assertThat(Files.exists(partitionDirectory.resolve(Paths.get("segments","0000000000000000000"))), is(true));
        partition.close();
    }

    @Test
    public void testReadOfEmptyPartition() throws Exception {
        AtomicBoolean thenAcceptCalled = new AtomicBoolean(false);
        try (Partition partition = partition(10)) {
            partition.read(0, 1, Duration.ofMillis(100)).thenAccept(messageSet -> {
                thenAcceptCalled.set(true);
                assertThat(messageSet.startOffset(), is(0L));
                assertThat(messageSet.isEmpty(), is(true));
            }).join();
        }
        assertThat(thenAcceptCalled.get(), is(true));
    }

    @Test
    public void testReadTimeouts() throws Exception {
        AtomicInteger readCounts = new AtomicInteger(0);
        try (Partition partition = partition(10)) {
            for (int i = 0; i < 10; i++) {
                long start = System.currentTimeMillis();
                partition.read(0, 1, Duration.ofMillis(100)).thenAccept(messageSet -> {
                    readCounts.incrementAndGet();
                    logger.info("Timed Out after {}ms", System.currentTimeMillis() - start);
                });
            }
        }
        Thread.sleep(500);
        assertThat(readCounts.get(), is(10));
    }

    @Test
    public void testReadPastTail() throws Exception {
        try (Partition partition = partition(10)) {
            partition.read(1, 1, Duration.ofMillis(100)).thenAccept(messageSet -> {
                assertThat(messageSet.startOffset(), is(0L));
                assertThat(messageSet.isEmpty(), is(true));
            }).join();
        }
    }

    @Test
    public void testPartitionInfo() throws Exception {
        try (Partition partition = partition(10)) {
            PartitionInfo partitionInfo = partition.partitionInfo().get();
            assertThat(partitionInfo.head(), is(0L));
            assertThat(partitionInfo.tail(), is(0L));

            partition.read(0, 10, Duration.ofMillis(10)).whenComplete((messageSet, throwable) -> {
                assertThat(messageSet.nextOffset(), is(0));
            });

            partition.write(Message.empty().build()).join();
            partitionInfo = partition.partitionInfo().get();
            assertThat(partitionInfo.head(), is(0L));
            assertThat(partitionInfo.tail(), is(1L));

            partition.read(0, 10, Duration.ofMillis(10)).whenComplete((messageSet, throwable) -> {
                assertThat(messageSet.size(), is(0));
                assertThat(messageSet.startOffset(), is(1));
                assertThat(messageSet.nextOffset(), is(1));
            });

            partition.read(partitionInfo.tail(), 10, Duration.ofMillis(10)).whenComplete((messageSet, throwable) -> {
                assertThat(messageSet.nextOffset(), is(1));
            });

            for (int i = 0; i < 99; i++) {
                partition.write(Message.empty().build()).join();
            }
            partitionInfo = partition.partitionInfo().get();
            assertThat(partitionInfo.head(), is(0L));
            assertThat(partitionInfo.tail(), is(100L));
        }
    }

    @Test
    public void testSegmentRollover() throws Exception {
        Path partitionDirectory = partitionDirectory();
        Path segmentsDirectory = partitionDirectory.resolve("segments");
        Partition partition = new DiskPartition(partitionDirectory,
                new DiskSegmentBuilder().flushStrategy(FlushStrategies::afterEveryWrite), new DefaultSegmentationStrategy(2),
                new SegmentCountArchiveStrategy(10), new DeletingSegmentArchiver());
        int messageCount = 0;
        for (int i = 0; i < 2; i++) {
            partition.write(Message.empty().withString("messageCount", "messageCount:" + messageCount++).build())
                     .join();
        }
        assertThat(Files.exists(segmentsDirectory.resolve(Paths.get("0000000000000000000"))), is(true));
        assertThat(Files.exists(segmentsDirectory.resolve(Paths.get("0000000000000000002"))), is(false));
        partition.write(Message.empty().withString("messageCount", "messageCount:" + messageCount++).build()).join();
        assertThat(Files.exists(segmentsDirectory.resolve(Paths.get("0000000000000000002"))), is(true));
        assertThat(Files.exists(segmentsDirectory.resolve(Paths.get("0000000000000000004"))), is(false));
        for (int i = 0; i < 2; i++) {
            partition.write(Message.empty().withString("messageCount", "messageCount:" + messageCount++).build())
                     .join();
        }
        assertThat(Files.exists(segmentsDirectory.resolve(Paths.get("0000000000000000004"))), is(true));
    }

    @Test
    public void testWriteAfterSegmentationStrategyChangesToLowerValue() throws Exception {
        Path partitionDirectory = partitionDirectory();
        Path segmentsDirectory = segmentsDirectory(partitionDirectory);
        DiskSegmentBuilder diskSegmentBuilder = new DiskSegmentBuilder().flushStrategy(FlushStrategies::afterEveryWrite);
        try (Segment segment = diskSegmentBuilder.build(segmentsDirectory.resolve(segmentName(0)))) {
            for (int i = 0; i < 97; i++) {
                segment.write(numberedMessage(i));
            }
        }
        try (Partition partition = partition(partitionDirectory, 10)) {
            for (int i = 97; i < 120; i++) {
                partition.write(numberedMessage(i)).join();
            }
        }
        try (Segment segment = diskSegmentBuilder.build(segmentsDirectory.resolve(segmentName(0)))) {
            assertThat(segment.size(), is(100));
        }
        try (Segment segment = diskSegmentBuilder.build(segmentsDirectory.resolve(segmentName(100)))) {
            assertThat(segment.size(), is(10));
        }
        try (Segment segment = diskSegmentBuilder.build(segmentsDirectory.resolve(segmentName(110)))) {
            assertThat(segment.size(), is(10));
        }
        assertThat(Files.list(segmentsDirectory).count(), is(3L));
        assertThat(Files.exists(segmentsDirectory.resolve(Paths.get("0000000000000000000"))), is(true));
        assertThat(Files.exists(segmentsDirectory.resolve(Paths.get("0000000000000000100"))), is(true));
        assertThat(Files.exists(segmentsDirectory.resolve(Paths.get("0000000000000000110"))), is(true));
    }

    @Test
    public void testReadSingleSegment() throws Exception {
        Path partitionDirectory = partitionDirectory();
        try (Partition partition = partition(partitionDirectory, 10)) {
            for (int i = 0; i < 10; i++) {
                partition.write(Message.empty()
                                       .withString("messageNumber", String.valueOf(i))
                                       .withInteger("messageNumber", i)
                                       .build()).join();
            }
            partition.read(0, 10, Duration.ofMillis(1000)).thenAccept(messageSet -> {
                assertThat(messageSet.size(), is(10));
                assertThat(messageSet.startOffset(), is(0L));
                int expectedMessageNumber = 0;
                for (Message message : messageSet) {
                    assertThat(message.getInteger("messageNumber"), is(expectedMessageNumber++));
                }
            }).join();
        }
    }

    @Test
    public void testReadAcrossContiguousSegments() throws Exception {
        Path partitionDirectory = partitionDirectory();
        Path segmentsDirectory = partitionDirectory.resolve("segments");
        try (Partition partition = partition(partitionDirectory, 10)) {
            int messageCount = 30;
            for (int i = 0; i < messageCount; i++) {
                partition.write(Message.empty()
                                       .withString("messageNumber", String.valueOf(i))
                                       .withInteger("messageNumber", i)
                                       .build()).join();
            }
            assertThat(Files.exists(segmentsDirectory.resolve(Paths.get("0000000000000000000"))), is(true));
            assertThat(Files.exists(segmentsDirectory.resolve(Paths.get("0000000000000000010"))), is(true));
            assertThat(Files.exists(segmentsDirectory.resolve(Paths.get("0000000000000000020"))), is(true));
            partition.read(0, messageCount, Duration.ofMillis(1000)).thenAccept(messageSet -> {
                assertThat(messageSet.size(), is(messageCount));
                assertThat(messageSet.startOffset(), is(0L));
                int expectedMessageNumber = 0;
                for (Message message : messageSet) {
                    assertThat(message.getInteger("messageNumber"), is(expectedMessageNumber++));
                }
            }).join();
        }
    }

    @Test
    public void testReadAcrossSegmentGaps() throws Exception {
        Path partitionDirectory = partitionDirectory();
        Path segmentsDirectory = segmentsDirectory(partitionDirectory);
        DiskSegmentBuilder diskSegmentBuilder = new DiskSegmentBuilder();
        try (Segment segment = diskSegmentBuilder.build(segmentsDirectory.resolve(segmentName(0)))) {
            for (int i = 0; i < 75; i++) {
                segment.write(Message.empty().withInteger("messageNumber", i).build());
            }
        }
        try (Segment segment = diskSegmentBuilder.build(segmentsDirectory.resolve(segmentName(100)))) {
            for (int i = 0; i < 100; i++) {
                segment.write(Message.empty().build());
            }
        }
        try (Partition partition = partition(partitionDirectory, 100)) {
            partition.read(60, 100, Duration.ofMillis(1000)).thenAccept(messageSet -> {
                assertThat(messageSet.size(), is(15));
                assertThat(messageSet.startOffset(), is(60L));
                int expectedMessageNumber = 60;
                for (Message message : messageSet) {
                    assertThat(message.getInteger("messageNumber"), is(expectedMessageNumber++));
                }
            }).join();
        }
    }

    @Test
    public void testReadBetweenSegmentGaps() throws Exception {
        Path partitionDirectory = partitionDirectory();
        Path segmentsDirectory = segmentsDirectory(partitionDirectory);
        DiskSegmentBuilder diskSegmentBuilder = new DiskSegmentBuilder();
        try (Segment segment = diskSegmentBuilder.build(segmentsDirectory.resolve(segmentName(0)))) {
            for (int i = 0; i < 75; i++) {
                segment.write(Message.empty().withInteger("messageNumber", i).build());
            }
        }
        try (Segment segment = diskSegmentBuilder.build(segmentsDirectory.resolve(segmentName(100)))) {
            for (int i = 0; i < 100; i++) {
                segment.write(Message.empty().withInteger("messageNumber", 100 + i).build());
            }
        }
        try (Partition partition = partition(partitionDirectory, 100)) {
            partition.read(80, 10, Duration.ofMillis(1000)).thenAccept(messageSet -> {
                assertThat(messageSet.startOffset(), is(100L));
                assertThat(messageSet.size(), is(10));
                int expectedMessageNumber = 100;
                for (Message message : messageSet) {
                    assertThat(message.getInteger("messageNumber"), is(expectedMessageNumber++));
                }
            }).join();
        }
    }

    @Test
    public void testReadBeforeEarliestOffset() throws Exception {
        Path partitionDirectory = partitionDirectory();
        Path segmentsDirectory = segmentsDirectory(partitionDirectory);
        DiskSegmentBuilder diskSegmentBuilder = new DiskSegmentBuilder();
        try (Segment segment = diskSegmentBuilder.build(segmentsDirectory.resolve(segmentName(100)))) {
            for (int i = 0; i < 75; i++) {
                segment.write(Message.empty().withInteger("messageNumber", 100 + i).build());
            }
        }
        try (Partition partition = partition(partitionDirectory, 100)) {
            partition.read(50, 200, Duration.ofMillis(1000)).thenAccept(messageSet -> {
                assertThat(messageSet.size(), is(75));
                assertThat(messageSet.startOffset(), is(100L));
                int expectedMessageNumber = 100;
                for (Message message : messageSet) {
                    assertThat(message.getInteger("messageNumber"), is(expectedMessageNumber++));
                }
            }).join();
        }
    }

    @Test
    public void testReadAfterLastMessageOffset() throws Exception {
        Path partitionDirectory = partitionDirectory();
        try (Partition partition = partition(partitionDirectory, 100)) {
            int messageNumber = 0;
            for (int i = 0; i < 200; i++) {
                partition.write(Message.empty().withInteger("messageNumber", messageNumber++).build()).join();
            }
            partition.read(250, 100, Duration.ofMillis(1000)).thenAccept(messageSet -> {
                assertThat(messageSet.startOffset(), is(200L));
                assertThat(messageSet.size(), is(0));
            }).join();
            for (int i = 0; i < 200; i++) {
                partition.write(Message.empty().withInteger("messageNumber", messageNumber++).build()).join();
            }
            partition.read(250, 100, Duration.ofMillis(1000)).thenAccept(messageSet -> {
                assertThat(messageSet.startOffset(), is(250L));
                assertThat(messageSet.size(), is(100));
                int expectedMessageNumber = 250;
                for (Message message : messageSet) {
                    assertThat(message.getInteger("messageNumber"), is(expectedMessageNumber++));
                }
            }).join();
        }
    }

    @Test
    public void testTrimToMaxSegments() throws Exception {
        Path partitionDirectory = partitionDirectory();
        try (SegmentedPartition partition = partition(partitionDirectory, 1)) {
            int messageNumber = 0;
            for (int i = 0; i < 10; i++) {
                partition.write(numberedMessage(messageNumber++)).join();
                assertThat(partition.segmentCount(), is(i + 1));
            }
            for (int i = 0; i < 10; i++) {
                partition.write(numberedMessage(messageNumber++)).join();
                assertThat(partition.segmentCount(), is(10));
            }
            partition.read(0, 10, Duration.ofMillis(1000)).thenAccept(messageSet -> {
                int expectedMessageNumber = 10;
                for (Message message : messageSet) {
                    assertThat(message.getInteger("messageNumber"), is(expectedMessageNumber++));
                }
            });
        }
    }

    @Test
    public void testSimpleDelayedRead() throws Exception {
        final AtomicReference<MessageSet> messageSetReference = new AtomicReference<>();
        try (Partition partition = partition(100)) {
            partition.read(0, 100, Duration.ofMillis(1000)).thenAccept(messageSetReference::set);
            assertThat(messageSetReference.get(), is(nullValue()));
            partition.write(Message.empty().build()).join();
            Thread.sleep(100);
            assertThat(messageSetReference.get(), not(nullValue()));
            assertThat(messageSetReference.get().startOffset(), is(0L));
            assertThat(messageSetReference.get().size(), is(1));
        }
    }

    @Test(timeOut = 60_000)
    public void testReadingAndWriting() throws Exception {
        for (int j = 0; j < 5; j++) {
            int messageToProcess = 10_000;
            CountDownLatch latch = new CountDownLatch(messageToProcess);
            AtomicInteger messagesRead = new AtomicInteger(0);
            try (Partition partition = partition(10_000)) {
                DiskPartitionReader.from(partition).withMaxMessages(10).startingAtHead().followingTail(messageSet -> {
                    logger.info("Reading {} messages from offset {}", messageSet.size(), messageSet.startOffset());
                    messagesRead.accumulateAndGet(messageSet.size(), (left, right) -> left + right);
                    for (Message message : messageSet) {
                        logger.info("Latency: {} ms ", (System.nanoTime() - message.getLong("created")) / 1000000f);
                        latch.countDown();
                    }
                }).whenComplete((aVoid, throwable) -> {
                    // Stops when partition is closed
                    logger.info("Performing assertion");
                    assertThat(messagesRead.get(), is(messageToProcess));
                });
                for (int i = 0; i < messageToProcess; i++) {
                    partition.write(timedMessage()).join();
                }
                latch.await(60_000, TimeUnit.MILLISECONDS);
            }

        }
    }

    @Test
    public void testOneShotLatency() throws Exception {
        try (Partition partition = partition(10_000)) {
            DiskPartitionReader.from(partition).withMaxMessages(10).startingAtHead().followingTail(messageSet -> {
                for (Message message : messageSet) {
                    logger.info("Latency: {} ms ", (nanoTime() - message.getLong("created")) / 1_000_000f);
                }
            });
            for (int i = 0; i < 100; i++) {
                partition.write(timedMessage()).join();
                Thread.sleep(100);
            }
        }
    }

    @Test
    public void testReadNegativeOffset() throws Exception {
        try (Partition partition = partition(10)) {
            for (int i = 0; i < 20; i++) {
                partition.write(Message.empty().build()).join();
            }
            MessageSet messageSet = partition.read(-1, 20, Duration.ofMillis(1000)).join();
            assertThat(messageSet.startOffset(), is(0L));
            assertThat(messageSet.size(), is(20));
        }
    }

    @Test
    public void testMakeUnWritable() throws Exception {
        Path partitionDirectory = partitionDirectory();
        try (DiskPartition partition = (DiskPartition)partition(partitionDirectory, 10)) {
            assertThat(partition.writable(), is(true));
            partition.writable(false);
            partition.writable(false); // Try this twice
            assertThat(partition.writable(), is(false));
            assertThat(Files.exists(partitionDirectory.resolve("nowrite")), is(true));
            partition.writable(true);
            partition.writable(true); // Try this twice
            assertThat(Files.exists(partitionDirectory.resolve("nowrite")), is(false));
            partition.writable(false);
        }

        // Re-read partition
        try (DiskPartition partition = (DiskPartition)partition(partitionDirectory, 10)) {
            assertThat(partition.writable(), is(false));
        }
    }

    @Test
    public void testMakeUnReadable() throws Exception {
        Path partitionDirectory = partitionDirectory();
        try (DiskPartition partition = (DiskPartition)partition(partitionDirectory, 10)) {
            assertThat(partition.readable(), is(true));
            partition.readable(false);
            assertThat(partition.readable(), is(false));
            assertThat(Files.exists(partitionDirectory.resolve("noread")), is(true));
            partition.readable(true);
            assertThat(Files.exists(partitionDirectory.resolve("noread")), is(false));
            partition.readable(false);
        }

        // Re-read partition
        try (DiskPartition partition = (DiskPartition)partition(partitionDirectory, 10)) {
            assertThat(partition.readable(), is(false));
        }
    }



    private String segmentName(long messageOffset) {
        return SegmentUtils.getSegmentName(messageOffset);
    }
}