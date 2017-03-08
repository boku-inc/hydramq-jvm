package io.hydramq;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.hydramq.disk.DeletingSegmentArchiver;
import io.hydramq.disk.DiskPartition;
import io.hydramq.disk.DiskSegmentBuilder;
import io.hydramq.disk.SegmentCountArchiveStrategy;
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
    private AtomicInteger messageStoreCount = new AtomicInteger(0);
    private AtomicInteger topicCount = new AtomicInteger(0);
    private AtomicInteger partitionCount = new AtomicInteger(0);

    @BeforeClass
    public void setup() throws IOException {
        Files.createDirectories(outputDirectory);
    }

    @AfterClass
    public void cleanup() throws IOException {
        if (shouldPerformCleanup()) {
            if (Files.exists(outputDirectory)) {
                DiskUtils.deleteDirectory(outputDirectory);
            }
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
        return topicDirectory("topicProtocol" + topicCount.getAndIncrement());
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
