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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.hydramq.Message;
import io.hydramq.MessageSet;
import io.hydramq.PartitionInfo;
import io.hydramq.Segment;
import io.hydramq.SegmentationStrategy;
import io.hydramq.SegmentedPartition;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.internal.util.AsyncUtils;
import io.hydramq.listeners.MessageIOListener;
import io.hydramq.listeners.PartitionFlags;
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jfulton
 */
@SuppressWarnings("Duplicates")
public class DiskPartition implements SegmentedPartition {

    private static final Logger logger = LoggerFactory.getLogger(DiskPartition.class);
    public static final String SEGMENTS_DIRECTORY_NAME = "segments";
    public static final String NOWRITE = "nowrite";
    public static final String NOREAD = "noread";
    private final Path partitionDirectory;
    private final Path segmentsDirectory;
    private final DiskSegmentBuilder diskSegmentBuilder;
    private final SegmentationStrategy segmentationStrategy;
    private final SegmentArchiveStrategy segmentArchiveStrategy;
    private final SegmentArchiver segmentArchiver;
    private final Long2ObjectSortedMap<Segment> segments =
            new Long2ObjectAVLTreeMap<>((Comparator<Long>) (o1, o2) -> o2.compareTo(o1));
    private ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private AtomicBoolean closed = new AtomicBoolean(false);
    private Segment currentSegment;
    long currentSegmentOffset;
    private volatile Map<CompletableFuture<MessageSet>, ReadRequest> delayedReadRequests = new HashMap<>();
    private CompletableFuture<Void> closingFuture = new CompletableFuture<>();
    private List<MessageIOListener> messageWriteListeners = new ArrayList<>();
    private List<MessageIOListener> messageReadListeners = new ArrayList<>();

    public DiskPartition(final Path partitionDirectory, final DiskSegmentBuilder diskSegmentBuilder,
                         final SegmentationStrategy segmentationStrategy, final SegmentArchiveStrategy segmentArchiveStrategy,
                         final SegmentArchiver segmentArchiver) throws HydraRuntimeException {
        this.partitionDirectory = partitionDirectory;
        this.segmentsDirectory = partitionDirectory.resolve(SEGMENTS_DIRECTORY_NAME);
        this.segmentArchiveStrategy = segmentArchiveStrategy;
        this.segmentArchiver = segmentArchiver;
        try {
            Files.createDirectories(this.partitionDirectory);
            Files.createDirectories(this.segmentsDirectory);
        } catch (IOException e) {
            throw new HydraRuntimeException(
                    "Error creating container directories for partition " + this.partitionDirectory.toString(), e);
        }
        this.diskSegmentBuilder = diskSegmentBuilder;
        this.segmentationStrategy = segmentationStrategy;
        loadSegments(this.segmentsDirectory, segments);
        if (segments.size() == 0) {
            currentSegmentOffset = 0;
            currentSegment = diskSegmentBuilder.build(segmentsDirectory.resolve(SegmentUtils.getSegmentName(currentSegmentOffset)));
            segments.put(currentSegmentOffset, currentSegment);
        } else {
            currentSegmentOffset = segments.firstLongKey();
            currentSegment = segments.get(currentSegmentOffset);
        }
    }

    public long head() {
        assertNotClosed();
        return segments.lastLongKey();
    }

    public long tail() {
        assertNotClosed();
        try {
            lock.readLock().lock();
            long segmentOffset = segments.firstLongKey();
            return segmentOffset + segments.get(segmentOffset).size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Void> write(Message message) {
        assertNotClosed();
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            writeMessage(message);
            future.complete(null);
        } catch (HydraRuntimeException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> write(List<Message> messages) {
        throw new UnsupportedOperationException("Unimplemented");
    }

    @Override
    public CompletableFuture<Void> write(final MessageSet messageSet) {
        throw new UnsupportedOperationException("Unimplemented");
    }

    private void writeMessage(Message message) throws HydraRuntimeException {
        try {
            lock.writeLock().lock();
            long writeSegmentKey = getSegmentForMessageOffset(currentSegmentOffset + currentSegment.size());
            if (writeSegmentKey < currentSegmentOffset + currentSegment.size()) {
                writeSegmentKey = currentSegmentOffset;
            }
            if (!segments.containsKey(writeSegmentKey)) {
                currentSegment = diskSegmentBuilder.build(segmentsDirectory.resolve(SegmentUtils.getSegmentName(writeSegmentKey)));
                segments.put(writeSegmentKey, currentSegment);
                currentSegmentOffset = writeSegmentKey;
                trimToMaxSegments();
            }
            currentSegment.write(message);
            if (delayedReadRequests.size() > 0) {
                handlePendingReads();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public CompletableFuture<MessageSet> read(long messageOffset, int maxMessages, Duration timeout) {
        assertNotClosed();

        CompletableFuture<MessageSet> responseFuture = new CompletableFuture<>();
        try {
            MessageSet messageSet = read(messageOffset, maxMessages);
            if (messageSet.isEmpty()) {
                try {
                    lock.writeLock().lock();
                    if (messageSet.startOffset() >= tail()) {
                        ReadRequest readRequest = new ReadRequest(messageSet.startOffset(), maxMessages, responseFuture);
                        delayedReadRequests.put(responseFuture, readRequest);
                        return AsyncUtils.within(responseFuture, timeout, messageSet);
                    } else {
                        // In the case where the tail has moved since initial read, read again eagerly
                        messageSet = read(messageSet.startOffset(), maxMessages);
                        responseFuture.complete(messageSet);
                    }
                } catch (Exception ex) {
                    logger.warn("Weird", ex);
                } finally {
                    lock.writeLock().unlock();
                }
            } else {
                responseFuture.complete(messageSet);
            }
        } catch (HydraRuntimeException e) {
            responseFuture.completeExceptionally(e);
        }
        return responseFuture;
    }

    @Override
    public CompletableFuture<PartitionInfo> partitionInfo() {
        return CompletableFuture.completedFuture(new PartitionInfo(head(), tail()));
    }

    private MessageSet read(final long messageOffset, final int maxMessages) throws HydraRuntimeException {
        MessageSet messageSet = null;
        try {
            lock.readLock().lock(); // TODO: review locking strategy
            LongBidirectionalIterator segmentOffsetIterator = segments.keySet().iterator();
            messageSet = new MessageSet(Math.min(messageOffset, tail()));
            while (segmentOffsetIterator.hasNext()) {
                long segmentOffset = segmentOffsetIterator.nextLong();
                if (messageSet.startOffset() >= segmentOffset) {
                    Segment segment = segments.get(segmentOffset);
                    segment.read((int) (Math.abs(messageSet.startOffset() - segmentOffset)), maxMessages, messageSet);
                    segmentOffsetIterator.previousLong(); // Consume the first read nextLong()
                    break;
                }
            }
            while (messageSet.size() < maxMessages && segmentOffsetIterator.hasPrevious()) {
                long segmentOffset = segmentOffsetIterator.previousLong();
                // If in the previous segment reads, there are no records, recreate the MessageSet with the
                // correct starting message offset
                if (messageSet.isEmpty()) {
                    messageSet = new MessageSet(segmentOffset);
                }
                // If the offset + number of records read from the previous segment are not greater or equal
                // to the next segment offset, there is a gap, and we cannot collect any more messages within
                // the same messageSet.  Send now, and let the client pick up after the gap.
                if (messageSet.nextOffset() >= segmentOffset) {
                    int remainingMessages = maxMessages - messageSet.size();
                    Segment segment = segments.get(segmentOffset);
                    segment.read(0, remainingMessages, messageSet);
                } else {
                    break;
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        return messageSet;
    }

    @Override
    public CompletableFuture<Void> closingFuture() {
        return closingFuture;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }


    @Override
    public void close() throws IOException {
        try {
            lock.writeLock().lock();
            handlePendingReads();
            if (closed.compareAndSet(false, true)) {
                logger.debug("Closing");
                closingFuture.complete(null);
                for (Segment segment : segments.values()) {
                    segment.close();
                }
                logger.debug("Closed");
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void handlePendingReads() {
        Map<CompletableFuture<MessageSet>, ReadRequest> delayedReadRequestsCopy = null;
        try {
            lock.writeLock().lock();

            delayedReadRequestsCopy = delayedReadRequests;
            delayedReadRequests = new HashMap<>();
        } finally {
            lock.writeLock().unlock();
        }
        if (delayedReadRequestsCopy != null) {
            for (ReadRequest readRequest : delayedReadRequestsCopy.values()) {
                try {
                    MessageSet messageSet = read(readRequest.getMessageOffset(), readRequest.getMaxMessages());
                    readRequest.getFuture().complete(messageSet);
                } catch (HydraRuntimeException e) {
                    readRequest.getFuture().completeExceptionally(e);
                }
            }
        }
    }


    private void trimToMaxSegments() throws HydraRuntimeException {
        if (segments.size() <= segmentationStrategy.maxSegments()) {
            return;
        }
        try {
            lock.writeLock().lock();
            if (segments.size() > segmentationStrategy.maxSegments()) {
                int segmentsToArchive = segments.size() - segmentationStrategy.maxSegments();
                LongBidirectionalIterator iterator = segments.keySet().iterator();
                // Go to last
                while (iterator.hasNext()) {
                    iterator.next();
                }
                for (int i = segmentsToArchive; i > 0; i--) {
                    long segmentOffset = iterator.previousLong();
                    logger.info("Trimming segment {}", segmentOffset);
                    Segment segment = segments.remove(segmentOffset);
                    try {
                        segment.delete();
                    } catch (IOException e) {
                        throw new HydraRuntimeException("Error archiving segment for offset " + segmentOffset);
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long getSegmentForMessageOffset(long messageOffset) {
        return (long) ((Math.ceil(Math.abs(messageOffset / segmentationStrategy.maxMessages()))) *
                segmentationStrategy.maxMessages());
    }

    private void assertNotClosed() {
        if (closed.get()) {
            throw new HydraRuntimeException("This partition has been closed");
        }
    }

    @Override
    public int segmentCount() {
        return segments.size();
    }

    PartitionFlags flags() {
        EnumSet<PartitionFlags.Flag> flagSet = EnumSet.noneOf(PartitionFlags.Flag.class);
        if (readable()) {
            flagSet.add(PartitionFlags.Flag.READ);
        }
        if (writable()) {
            flagSet.add(PartitionFlags.Flag.WRITE);
        }
        return new PartitionFlags(flagSet);
    }

    boolean writable() {
        return !getState(NOWRITE);
    }

    boolean writable(boolean writable) {
        return setState(NOWRITE, writable);
    }

    boolean readable() {
        return !getState(NOREAD);
    }

    boolean readable(boolean readable) {
        return setState(NOREAD, readable);
    }

    private boolean getState(String state) {
        try {
            lock.readLock().lock();
            return Files.exists(partitionDirectory.resolve(state));
        } finally {
            lock.readLock().unlock();
        }
    }

    private boolean setState(String state, boolean value) {
        try {
            lock.writeLock().lock();
            if (value) {
                if (getState(state)) {
                    Files.deleteIfExists(partitionDirectory.resolve(state));
                    return true;
                }
            } else {
                if (!getState(state)) {
                    Files.createFile(partitionDirectory.resolve(state));
                    return true;
                }
            }
        } catch (IOException e) {
            throw new HydraRuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
        return false;
    }


    public void onWriteMessages(MessageIOListener consumer) {
        this.messageWriteListeners.add(consumer);
    }

    public void onReadMessages(MessageIOListener consumer) {
        this.messageReadListeners.add(consumer);
    }

    @SuppressWarnings("Duplicates")
    private void loadSegments(final Path baseDirectory, Map<Long, Segment> segments) throws HydraRuntimeException {
        try {
            Files.walkFileTree(baseDirectory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs)
                        throws IOException {
                    if (baseDirectory == dir) {
                        return FileVisitResult.CONTINUE;
                    }
                    try {
                        // TODO: more robust handling of loading segments.  Ensure that directory has a valid segment
                        // TODO: name pattern, and has segment index/data files.  We need to be able to handle renamed
                        // TODO: segment files, etc.
                        Segment segment = diskSegmentBuilder.build(dir);
                        segments.put(Long.parseLong(dir.getFileName().toString()), segment);
                    } catch (HydraRuntimeException e) {
                        if (e.getCause() instanceof IOException) {
                            throw (IOException) e.getCause();
                        } else {
                            throw new HydraRuntimeException("Unexpected HydraRuntimeException", e);
                        }
                    }
                    return FileVisitResult.SKIP_SUBTREE;
                }
            });

        } catch (IOException e) {
            throw new HydraRuntimeException("Error loading segments contained in " + baseDirectory.toString());
        }
    }

    private class ReadRequest {

        private long messageOffset;
        private int maxMessages;
        private CompletableFuture<MessageSet> future;

        public ReadRequest(final long messageOffset, final int maxMessages,
                           final CompletableFuture<MessageSet> future) {
            this.messageOffset = messageOffset;
            this.maxMessages = maxMessages;
            this.future = future;
        }

        public long getMessageOffset() {
            return messageOffset;
        }

        public int getMaxMessages() {
            return maxMessages;
        }

        public CompletableFuture<MessageSet> getFuture() {
            return future;
        }
    }
}
