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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.hydramq.DefaultSegmentationStrategy;
import io.hydramq.Message;
import io.hydramq.PartitionId;
import io.hydramq.SegmentedPartition;
import io.hydramq.Topic;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.internal.util.DiskUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * @author jfulton
 */
public class PersistenceTestsBase {

    public static final String MESSAGE_NUMBER = "messageNumber";
    public static final String CREATED = "created";
    private Path outputDirectory = Paths.get(this.getClass().getCanonicalName());
    private boolean performCleanup = true;
    private static AtomicInteger messageStoreCount = new AtomicInteger(0);
    private static AtomicInteger topicCount = new AtomicInteger(0);
    private static AtomicInteger partitionCount = new AtomicInteger(0);
    private static AtomicInteger segmentCount = new AtomicInteger(0);

    @BeforeClass
    public void setup() throws IOException {
        Files.createDirectories(outputDirectory);
    }

    @AfterClass
    public void cleanup() throws IOException {
        if (shouldPerformCleanup()) {
            DiskUtils.deleteDirectory(outputDirectory);
        }
    }

    public Path getOutputDirectory() {
        return outputDirectory;
    }

    public boolean shouldPerformCleanup() {
        return performCleanup;
    }

    public void setPerformCleanup(final boolean performCleanup) {
        this.performCleanup = performCleanup;
    }

    public SegmentedPartition partition(int messagesPerSegment) throws HydraRuntimeException {
        return partition(partitionDirectory(), messagesPerSegment);
    }

    public SegmentedPartition partition(Path partitionDirectory, int messagePerPartition)
            throws HydraRuntimeException {
        return new DiskPartition(partitionDirectory,
                new DiskSegmentBuilder(),
                new DefaultSegmentationStrategy(messagePerPartition), new SegmentCountArchiveStrategy(1), new DeletingSegmentArchiver());
    }

    public Path messageStoreDirectory() {
        return outputDirectory.resolve("messageStore" + messageStoreCount.incrementAndGet());
    }

    public Path topicDirectory() {
        return topicDirectory("simple" + topicCount.getAndIncrement());
    }

    public Path topicDirectory(String topicName) {
        return outputDirectory.resolve(topicName);
    }

    public Path partitionDirectory(Path topicDirectory) {
        return topicDirectory.resolve(partitionName());
    }

    public Path partitionDirectory() {
        return outputDirectory.resolve(partitionName());
    }

    public Path segmentsDirectory(Path partitionDirectory) {
        return partitionDirectory.resolve("segments");
    }

    public Path segmentDirectory() {
        return outputDirectory.resolve("segments/" + segmentCount.incrementAndGet());
    }

    public String partitionName() {
        return "partition" + partitionCount.getAndIncrement();
    }

    public Message numberedMessage(int messageNumber) {
        return Message.empty().withInteger(MESSAGE_NUMBER, messageNumber).build();
    }

    public Message timedMessage() {
        return Message.empty().withLong(CREATED, System.nanoTime()).build();
    }

    public PartitionId getPartitionId(Topic topic) {
        Optional<PartitionId> partitionId = topic.partitionIds().stream().findFirst();
        if (partitionId.isPresent()) {
            return partitionId.get();
        } else {
            throw new HydraRuntimeException("No partitionIds found");
        }
    }

    public PartitionId getPartitionId(Topic topic, int partitionNumber) {
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<PartitionId> selected = new AtomicReference<>();
        for (PartitionId partitionId : topic.partitionIds()) {
            if (counter.getAndIncrement() == partitionNumber) {
                return partitionId;
            }
        }
        throw new HydraRuntimeException("No partitionIds found");
    }
}
