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

package io.hydramq.topics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import io.hydramq.CursorInfo;
import io.hydramq.Message;
import io.hydramq.MessageSet;
import io.hydramq.PartitionId;
import io.hydramq.PartitionInfo;
import io.hydramq.Topic;
import io.hydramq.common.AbstractTopic;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.internal.apis.TopicInternal;
import io.hydramq.internal.util.Assert;
import io.hydramq.listeners.Listen;
import io.hydramq.listeners.PartitionFlags;
import io.hydramq.listeners.PartitionListener;
import io.hydramq.subscriptions.LockListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hydramq.listeners.Listen.CONTINUOUSLY;
import static io.hydramq.listeners.Listen.REMOVE;
import static io.hydramq.listeners.PartitionFlags.Flag.CREATED;

/**
 * @author jfulton
 */
public class CompositeTopic extends AbstractTopic implements Topic, TopicInternal {

    private static final Logger logger = LoggerFactory.getLogger(CompositeTopic.class);
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private final String topicName;
    private Map<Topic, PartitionListener> topics = new HashMap<>();
    private Map<PartitionId, PartitionIdData> partitionIdMappings = new HashMap<>();
    private Set<PartitionListener> partitionListeners = new HashSet<>();
    private CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    public CompositeTopic(String topicName, Topic... topics) {
        this.topicName = topicName;
        for (Topic t : topics) {
            add(t);
        }
    }

    public void add(Topic topic) {
        Map<PartitionId, PartitionFlags> partitionStateChanges = new HashMap<>();
        List<PartitionListener> listeners = new ArrayList<>();
        try {
            lock.writeLock().lock();
            if (!topics.containsKey(topic)) {
                PartitionListener listener = (partitionId, partitionFlags) -> {
                    try {
                        lock.writeLock().lock();
                        if (partitionIdMappings.containsKey(partitionId)) {
                            PartitionIdData data = partitionIdMappings.get(partitionId);
                            if (!data.getFlags().equals(partitionFlags)) {
                                // TODO: CheckForDelete
                                data.setFlags(partitionFlags);
                                partitionStateChanges.put(partitionId, partitionFlags);
                            }
                        } else {
                            if (partitionFlags.hasFlag(CREATED)) {
                                PartitionFlags storedFlags = new PartitionFlags(partitionFlags.toEnumSet());
                                storedFlags.removeFlag(CREATED);
                                partitionIdMappings.put(partitionId, new PartitionIdData(topic, partitionId, storedFlags));
                            } else {
                                partitionIdMappings.put(partitionId, new PartitionIdData(topic, partitionId, partitionFlags));
                            }
                            partitionStateChanges.put(partitionId, partitionFlags);
                        }
                    } finally {
                        lock.writeLock().unlock();
                    }
                };
                topics.put(topic, listener);
                topic.discoverPartitions(listener, CONTINUOUSLY);
                if (partitionStateChanges.size() > 0) {
                    listeners.addAll(partitionListeners);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        listeners.forEach(listener -> {
            partitionStateChanges.forEach(listener::onPartitionDiscovered);
        });
    }

    public void remove(Topic topic) {
        Map<PartitionId, PartitionFlags> partitionStateChanges = new HashMap<>();
        List<PartitionListener> listeners = new ArrayList<>();
        // TODO: notify listeners
        try {
            lock.writeLock().lock();
            if (topics.containsKey(topic)) {
                topic.discoverPartitions(topics.get(topic), REMOVE);
                topics.remove(topic);
                List<PartitionIdData> datas = new ArrayList<>();
                datas.addAll(partitionIdMappings.values());
                for (PartitionIdData data : datas) {
                    if (data.getTopic().equals(topic)) {
                        partitionIdMappings.remove(data.getPartitionId());
                        partitionStateChanges.put(data.getPartitionId(), PartitionFlags.of(PartitionFlags.Flag.REMOVED));
                    }
                }
                if (partitionStateChanges.size() > 0) {
                    listeners.addAll(partitionListeners);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        listeners.forEach(listener -> partitionStateChanges.forEach(listener::onPartitionDiscovered));
    }

    @Override
    public String getName() {
        return topicName;
    }

    @Override
    public CompletableFuture<Void> write(PartitionId partitionId, Message message) {
        Topic topic = null;
        try {
            lock.readLock().lock();
            if (partitionIdMappings.containsKey(partitionId)) {
                topic = partitionIdMappings.get(partitionId).getTopic();
            }
        } finally {
            lock.readLock().unlock();
        }
        if (topic != null) {
            return topic.write(partitionId, message);
        }
        throw new HydraRuntimeException("Invalid partitionId: " + partitionId);
    }

    @Override
    public CompletableFuture<Void> write(PartitionId partitionId, MessageSet messageSet) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<Void> write(Map<PartitionId, List<Message>> messageBatch) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<MessageSet> read(PartitionId partitionId, long messageOffset, int maxMessages) {
        Topic topic = null;
        try {
            lock.readLock().lock();
            if (partitionIdMappings.containsKey(partitionId)) {
                topic = partitionIdMappings.get(partitionId).getTopic();
            }
        } finally {
            lock.readLock().unlock();
        }
        if (topic != null) {
            return topic.read(partitionId, messageOffset, maxMessages);
        }

        throw new HydraRuntimeException("Invalid partitionId: " + partitionId);
    }


    @Override
    public void discoverPartitions(PartitionListener listener) {
        discoverPartitions(listener, Listen.ONCE);
    }

    @Override
    public void discoverPartitions(PartitionListener listener, Listen listen) {
        Assert.argumentNotNull(listener, "listener");
        Assert.argumentNotNull(listen, "listen");
        Map<PartitionId, PartitionFlags> results = new HashMap<>();
        try {
            lock.writeLock().lock();
            if (listen == Listen.ONCE || listen == Listen.CONTINUOUSLY) {
                if (listen == Listen.CONTINUOUSLY) {
                    partitionListeners.add(listener);
                }
                partitionIdMappings.forEach((partitionId, partitionIdData) -> results.put(partitionId, partitionIdData.getFlags()));
            } else if (listen == Listen.REMOVE) {
                partitionListeners.remove(listener);
            }
        } finally {
            lock.writeLock().unlock();
        }
        results.forEach(listener::onPartitionDiscovered);
    }

    @Override
    public void discoverPartitions(PartitionListener listener, Listen listen, Map<PartitionId, PartitionFlags> knownPartitionStates) {
        Assert.argumentNotNull(listener, "listener");
        Assert.argumentNotNull(listen, "listen");
        Map<PartitionId, PartitionFlags> results = new HashMap<>();
        try {
            lock.writeLock().lock();
            if (listen == Listen.ONCE || listen == Listen.CONTINUOUSLY) {
                if (listen == Listen.CONTINUOUSLY) {
                    partitionListeners.add(listener);
                }
                partitionIdMappings.forEach((partitionId, partitionIdData) -> {
                    if (knownPartitionStates.containsKey(partitionId) && knownPartitionStates.get(partitionId).equals(partitionIdData.getFlags())) {
                        // do nothing
                    } else {
                        results.put(partitionId, partitionIdData.getFlags());
                    }
                });
            } else if (listen == Listen.REMOVE) {
                partitionListeners.remove(listener);
            }
        } finally {
            lock.writeLock().unlock();
        }
        results.forEach(listener::onPartitionDiscovered);
    }

    @Override
    public CompletableFuture<PartitionInfo> partitionInfo(PartitionId partitionId) {
        Topic topic = null;
        try {
            lock.readLock().lock();
            if (partitionIdMappings.containsKey(partitionId)) {
                topic = partitionIdMappings.get(partitionId).getTopic();
            }
        } finally {
            lock.readLock().unlock();
        }
        if (topic != null) {
            return topic.partitionInfo(partitionId);
        }
        throw new HydraRuntimeException("Invalid partitionId: " + partitionId);
    }

    @Override
    public SortedSet<PartitionId> partitionIds() {
        SortedSet<PartitionId> results = new TreeSet<>();
        try {
            lock.readLock().lock();
            results.addAll(partitionIdMappings.keySet());
        } finally {
            lock.readLock().unlock();
        }
        return results;
    }

    @Override
    public void acquirePartitionLocks(String lockGroup, LockListener lockListener) {
        try {
            lock.readLock().lock();
            topics.keySet().forEach(topic -> {
                topic.acquirePartitionLocks(lockGroup, lockListener);
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void acquirePartitionLocks(String lockGroup, LockListener lockListener, Listen listen) {
        try {
            lock.readLock().lock();
            topics.keySet().forEach(topic -> {
                topic.acquirePartitionLocks(lockGroup, lockListener, listen);
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public CompletableFuture<CursorInfo> cursor(PartitionId partitionId, String cursorName) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<Void> cursor(PartitionId partitionId, String cursorName, long messageOffset) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int partitions() {
        return partitionIdMappings.size();
    }

    @Override
    public void partitions(int targetPartitionCount) throws HydraRuntimeException {
        throw new UnsupportedOperationException("Increasing partition counts is not currently supported on " +
                CompositeTopic.class.getSimpleName());
    }

    @Override
    public int writablePartitions() {
        return partitionIdMappings.size();
    }

    @Override
    public void writablePartitions(int targetWritablePartitionCount) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int readablePartitions() {
        return partitionIdMappings.size();
    }

    @Override
    public void readablePartitions(int targetReadablePartitionCount) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void close() throws IOException {
        List<CompletableFuture<Void>> closeFutures = topics.keySet().stream().map(topic -> {
            try {
                topic.close();
            } catch (IOException e) {
                logger.error("Error shutting down topic {}", topic);
            }
            return topic.closeFuture();
        }).collect(Collectors.toList());
        CompletableFuture<Void> aggregateFuture = CompletableFuture.allOf(closeFutures.toArray(new CompletableFuture[]{}));
        aggregateFuture.whenComplete((aVoid, throwable) -> closeFuture.complete(null));
    }

    @Override
    public CompletableFuture<Void> closeFuture() {
        return closeFuture;
    }

    private static class PartitionIdData {
        private final Topic topic;
        private final PartitionId partitionId;
        private PartitionFlags flags;

        public PartitionIdData(Topic topic, PartitionId partitionId, PartitionFlags flags) {
            this.topic = topic;
            this.partitionId = partitionId;
            this.flags = flags;
        }

        public Topic getTopic() {
            return topic;
        }

        public PartitionId getPartitionId() {
            return partitionId;
        }

        public PartitionFlags getFlags() {
            return flags;
        }

        public void setFlags(PartitionFlags flags) {
            this.flags = flags;
        }


    }
}
