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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import io.hydramq.CursorInfo;
import io.hydramq.CursorSetManager;
import io.hydramq.Message;
import io.hydramq.MessageSet;
import io.hydramq.Partition;
import io.hydramq.PartitionId;
import io.hydramq.PartitionInfo;
import io.hydramq.Topic;
import io.hydramq.common.AbstractTopic;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.exceptions.InvalidPartitionIdFormat;
import io.hydramq.internal.apis.TopicInternal;
import io.hydramq.internal.util.Assert;
import io.hydramq.listeners.Listen;
import io.hydramq.listeners.PartitionFlags;
import io.hydramq.listeners.PartitionListener;
import io.hydramq.subscriptions.DefaultLockManager;
import io.hydramq.subscriptions.LockListener;
import io.hydramq.subscriptions.LockManager;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectSortedMap;
import org.slf4j.Logger;

import static io.hydramq.listeners.PartitionFlags.Flag.CREATED;
import static io.hydramq.listeners.PartitionFlags.Flag.READ;
import static io.hydramq.listeners.PartitionFlags.Flag.REMOVED;
import static io.hydramq.listeners.PartitionFlags.Flag.WRITE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
public class DiskTopic extends AbstractTopic implements Topic, TopicInternal {

    private static final Logger logger = getLogger(DiskTopic.class);
    private final String name;
    private final SortedMap<PartitionId, DiskPartition> partitions = new TreeMap<>();
    private int writablePartitions = 0;
    private int readablePartitions = 0;
    private final Int2ObjectSortedMap<PartitionId> partitionIdMap =
            new Int2ObjectAVLTreeMap<>((Comparator<Integer>) Integer::compareTo);
    private final Path partitionsDirectory;
    private final PartitioningStrategy partitioningStrategy;
    private final DiskPartitionBuilder diskPartitionBuilder;
    private Set<PartitionListener> partitionListeners = new HashSet<>();
    private LockManager lockManager;
    private CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private Duration defaultTimout = Duration.ofMillis(5000);

    private CursorSetManager cursorSetManager;

    public DiskTopic(final String topicName, final Path topicDirectory, final DiskPartitionBuilder diskPartitionBuilder,
                     final PartitioningStrategy partitioningStrategy) throws HydraRuntimeException {
        this.name = topicName;
        this.partitionsDirectory = topicDirectory.resolve("partitions");
        this.partitioningStrategy = partitioningStrategy;
        this.diskPartitionBuilder = diskPartitionBuilder;
        this.lockManager = new DefaultLockManager(this);

        try {
            Files.createDirectories(topicDirectory);
            Files.createDirectories(partitionsDirectory);
        } catch (IOException e) {
            throw new HydraRuntimeException(
                    "Error creating container directories for partition " + topicDirectory.toString(), e);
        }
        this.cursorSetManager = new DefaultCursorSetManager(topicDirectory.resolve("cursors"));
        loadPartitions(partitionsDirectory, partitions);
        verifyConfiguration();
    }

    public String getName() {
        return name;
    }

    public DiskPartition partition(final PartitionId partitionId) throws HydraRuntimeException {
        assertNotClosed();
        return partitions.get(partitionId);
    }

    @Override
    public CompletableFuture<Void> write(PartitionId partitionId, Message message) {
        return partition(partitionId).write(message);
    }

    @Override
    public CompletableFuture<Void> write(PartitionId partitionId, MessageSet messageSet) {
        return partition(partitionId).write(messageSet);
    }

    @Override
    public CompletableFuture<Void> write(Map<PartitionId, List<Message>> messageBatch) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        messageBatch.forEach((partitionId, messages) -> futures.add(partition(partitionId).write(messages)));
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    }

    @Override
    public CompletableFuture<MessageSet> read(PartitionId partitionId, long messageOffset, int maxMessages) {
        return partition(partitionId).read(messageOffset, maxMessages, defaultTimout);
    }

    public void discoverPartitions(PartitionListener listener) {
        discoverPartitions(listener, Listen.ONCE);
    }

    @Override
    public void discoverPartitions(PartitionListener listener, Listen listen) {
        Assert.argumentNotNull(listener, "listener");
        Assert.argumentNotNull(listen, "listen");
        List<PartitionId> results = new ArrayList<>();
        try {
            lock.writeLock().lock();
            if (listen == Listen.ONCE || listen == Listen.CONTINUOUSLY) {
                if (listen == Listen.CONTINUOUSLY) {
                    partitionListeners.add(listener);
                }
                partitions.keySet().forEach(results::add);
            } else if (listen == Listen.REMOVE) {
                partitionListeners.remove(listener);
            }
        } finally {
            lock.writeLock().unlock();
        }
        results.forEach(partitionId -> {
            listener.onPartitionDiscovered(partitionId, partitions.get(partitionId).flags());
        });
    }

    @Override
    public void discoverPartitions(PartitionListener listener, Listen listen, Map<PartitionId, PartitionFlags> knownPartitionStates) {
        Assert.argumentNotNull(listener, "listener");
        Assert.argumentNotNull(listen, "listen");
        List<PartitionId> results = new ArrayList<>();
        try {
            lock.writeLock().lock();
            if (listen == Listen.ONCE || listen == Listen.CONTINUOUSLY) {
                if (listen == Listen.CONTINUOUSLY) {
                    partitionListeners.add(listener);
                }
                partitions.keySet().forEach(results::add);
            } else if (listen == Listen.REMOVE) {
                partitionListeners.remove(listener);
            }
        } finally {
            lock.writeLock().unlock();
        }
        results.forEach(partitionId -> {
            PartitionFlags flags = partitions.get(partitionId).flags();
            if (knownPartitionStates.containsKey(partitionId) && knownPartitionStates.get(partitionId).equals(flags)) {
                // do nothing
            } else {
                listener.onPartitionDiscovered(partitionId, flags);
            }
        });
    }

    @Override
    public CompletableFuture<PartitionInfo> partitionInfo(PartitionId partitionNumber) {
        return partition(partitionNumber).partitionInfo();
    }

    public SortedSet<PartitionId> partitionIds() {
        SortedSet<PartitionId> results = new TreeSet<>();
        try {
            lock.readLock().lock();
            results.addAll(partitions.keySet());
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void acquirePartitionLocks(String lockGroup, LockListener lockListener) {
        lockManager.subscribe(lockGroup, lockListener);
    }

    @Override
    public void acquirePartitionLocks(String lockGroup, LockListener lockListener, Listen listen) {
        if (listen == Listen.CONTINUOUSLY || listen == Listen.ONCE) {
            lockManager.subscribe(lockGroup, lockListener);
        } else {
            lockManager.unsubscribe(lockGroup, lockListener);
        }
    }

    @Override
    public CompletableFuture<CursorInfo> cursor(PartitionId partitionId, String cursorName) {
        long cursorPosition = cursorSetManager.getCursorSet(cursorName).get(partitionId);
        long head = partition(partitionId).head();
        long tail = partition(partitionId).tail();
        return CompletableFuture.completedFuture(new CursorInfo(cursorPosition, head, tail));
    }

    @Override
    public CompletableFuture<Void> cursor(PartitionId partitionId, String cursorName, long messageOffset) {
        cursorSetManager.getCursorSet(cursorName).set(partitionId, messageOffset);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public int partitions() {
        return partitions.size();
    }

    @Override
    public void partitions(int targetPartitionCount) throws HydraRuntimeException {
        Map<PartitionId, PartitionFlags> changedPartitions = new HashMap<>();
        List<PartitionListener> listeners = new ArrayList<>();
        try {
            lock.writeLock().lock();
            assertNotClosed();
            if (targetPartitionCount > partitions.size()) {
                for (int i = partitions.size(); i < targetPartitionCount; i++) {
                    PartitionId partitionId = PartitionId.create();
                    DiskPartition partition = diskPartitionBuilder.build(partitionsDirectory.resolve(partitionId.toString()));
                    partitions.put(partitionId, partition);
                    PartitionFlags flags = partition.flags();
                    changedPartitions.put(partitionId, flags);
                    if (flags.hasFlag(WRITE)) {
                        writablePartitions++;
                    }
                    if (flags.hasFlag(READ)) {
                        readablePartitions++;
                    }
                    flags.setFlag(CREATED);
                }
            } else if (targetPartitionCount < partitions.size()) {
                partitionIdMap.tailMap(targetPartitionCount).forEach((partitionNumber, partitionId) -> {
                    try {
                        partition(partitionId).close();
                    } catch (IOException e) {
                        throw new HydraRuntimeException(e);
                    }
                    partitionIdMap.remove(partitionNumber);
                    partitions.remove(partitionId);
                    changedPartitions.put(partitionId, PartitionFlags.of(REMOVED));
                    writablePartitions--;
                    readablePartitions--;
                });
            }
            buildPartitionIdMap();
            if (changedPartitions.size() > 0) {
                partitionListeners.forEach(listeners::add);
            }
        } finally {
            lock.writeLock().unlock();
        }
        for (PartitionListener listener : listeners) {
            changedPartitions.forEach(listener::onPartitionDiscovered);
        }

    }

    private void buildPartitionIdMap() {
        partitionIdMap.clear();
        AtomicInteger count = new AtomicInteger();
        partitions.keySet().forEach(partitionId -> {
            partitionIdMap.put(count.getAndIncrement(), partitionId);
        });
    }

    @Override
    public int writablePartitions() {
        try {
            lock.readLock().lock();
            assertNotClosed();
            return writablePartitions;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void writablePartitions(int targetWritablePartitionCount) {
        Map<PartitionId, PartitionFlags> changedPartitions = new HashMap<>();
        List<PartitionListener> listeners = new ArrayList<>();
        try {
            lock.readLock().lock();
            assertNotClosed();
            if (targetWritablePartitionCount <= 0) {
                partitions.keySet().forEach(partitionId -> {
                    DiskPartition partition = partitions.get(partitionId);
                    if (partition.writable(false)) {
                        changedPartitions.put(partitionId, partition.flags());
                    }
                });
                writablePartitions = 0;
            } else if (targetWritablePartitionCount >= partitions()) {
                partitions.keySet().forEach(partitionId -> {
                    DiskPartition partition = partitions.get(partitionId);
                    if (partition.writable(true)) {
                        changedPartitions.put(partitionId, partition.flags());
                    }
                });
                writablePartitions = partitions();
            } else if (targetWritablePartitionCount < writablePartitions) {
                //|ooooooxx|
                //|ooooxxxx|
                partitionIdMap.keySet().subSet(targetWritablePartitionCount, writablePartitions).forEach(partitionNumber -> {
                    PartitionId partitionId = partitionIdMap.get(partitionNumber);
                    DiskPartition partition = partitions.get(partitionId);
                    if (partition.writable(false)) {
                        changedPartitions.put(partitionId, partition.flags());
                    }
                });
                writablePartitions = targetWritablePartitionCount;
            } else if (targetWritablePartitionCount > writablePartitions) {
                //|ooooxxxx|
                //|ooooooxx|
                partitionIdMap.keySet().subSet(writablePartitions, targetWritablePartitionCount).forEach(partitionNumber -> {
                    PartitionId partitionId = partitionIdMap.get(partitionNumber);
                    DiskPartition partition = partitions.get(partitionId);
                    if (partition.writable(true)) {
                        changedPartitions.put(partitionId, partition.flags());
                    }
                });
                writablePartitions = targetWritablePartitionCount;
            }
            if (changedPartitions.size() > 0) {
                partitionListeners.forEach(listeners::add);
            }
        } finally {
            lock.readLock().unlock();
        }
        listeners.forEach(listener -> {
            for (Map.Entry<PartitionId, PartitionFlags> entry : changedPartitions.entrySet()) {
                listener.onPartitionDiscovered(entry.getKey(), entry.getValue());
            }
        });
    }

    @Override
    public int readablePartitions() {
        try {
            lock.readLock().lock();
            assertNotClosed();
            return readablePartitions;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void readablePartitions(int targetReadablePartitionCount) {
        Map<PartitionId, PartitionFlags> changedPartitions = new HashMap<>();
        List<PartitionListener> listeners = new ArrayList<>();
        try {
            lock.readLock().lock();
            assertNotClosed();
            if (targetReadablePartitionCount <= 0) {
                partitions.keySet().forEach(partitionId -> {
                    DiskPartition partition = partitions.get(partitionId);
                    if (partition.readable(false)) {
                        changedPartitions.put(partitionId, partition.flags());
                    }
                });
                readablePartitions = 0;
            } else if (targetReadablePartitionCount >= partitions()) {
                partitions.keySet().forEach(partitionId -> {
                    DiskPartition partition = partitions.get(partitionId);
                    if (partition.readable(true)) {
                        changedPartitions.put(partitionId, partition.flags());
                    }
                });
                readablePartitions = partitions();
            } else if (targetReadablePartitionCount < readablePartitions) {
                //|ooooooxx|
                //|ooooxxxx|
                partitionIdMap.keySet().subSet(targetReadablePartitionCount, readablePartitions).forEach(partitionNumber -> {
                    PartitionId partitionId = partitionIdMap.get(partitionNumber);
                    DiskPartition partition = partitions.get(partitionId);
                    if (partition.readable(false)) {
                        changedPartitions.put(partitionId, partition.flags());
                    }
                });
                readablePartitions = targetReadablePartitionCount;
            } else if (targetReadablePartitionCount > readablePartitions) {
                //|ooooxxxx|
                //|ooooooxx|
                partitionIdMap.keySet().subSet(readablePartitions, targetReadablePartitionCount).forEach(partitionNumber -> {
                    PartitionId partitionId = partitionIdMap.get(partitionNumber);
                    DiskPartition partition = partitions.get(partitionId);
                    if (partition.readable(true)) {
                        changedPartitions.put(partitionId, partition.flags());
                    }
                });
                readablePartitions = targetReadablePartitionCount;
            }
            if (changedPartitions.size() > 0) {
                partitionListeners.forEach(listeners::add);
            }
        } finally {
            lock.readLock().unlock();
        }
        listeners.forEach(listener -> {
            for (Map.Entry<PartitionId, PartitionFlags> entry : changedPartitions.entrySet()) {
                listener.onPartitionDiscovered(entry.getKey(), entry.getValue());
            }
        });
    }

    private void verifyConfiguration() throws HydraRuntimeException {
        if (partitions.size() == 0) {
            partitions(partitioningStrategy.minimumPartitionsOnNewTopics());
        } else if (partitions.size() < partitioningStrategy.minimumPartitionsOnExistingTopics()) {
            partitions(partitioningStrategy.minimumPartitionsOnExistingTopics());
        }
    }

    @Override
    public void close() throws IOException {
        try {
            lock.writeLock().lock();
            closeFuture.complete(null);
            for (Partition partition : partitions.values()) {
                partition.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void loadPartitions(final Path baseDirectory, Map<PartitionId, DiskPartition> partitions) throws HydraRuntimeException {
        try {
            Files.walkFileTree(baseDirectory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs)
                        throws IOException {
                    if (baseDirectory == dir) {
                        return FileVisitResult.CONTINUE;
                    }
                    try {
                        PartitionId partitionId = PartitionId.create(dir.getFileName().toString());
                        DiskPartition partition = diskPartitionBuilder.build(dir);
                        partitions.put(partitionId, partition);
                        if (partition.writable()) {
                            writablePartitions++;
                        }
                        if (partition.readable()) {
                            readablePartitions++;
                        }
                    } catch (InvalidPartitionIdFormat e) {
                        logger.warn("There was an invalid Partition detected at {}.  Skipping.", dir.toAbsolutePath());
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
            buildPartitionIdMap();
        } catch (IOException e) {
            throw new HydraRuntimeException("Error loading segments contained in " + baseDirectory.toString(), e);
        }
    }

    @Override
    public CompletableFuture<Void> closeFuture() {
        return closeFuture;
    }
}
