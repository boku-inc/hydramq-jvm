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

package io.hydramq.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.hydramq.Message;
import io.hydramq.PartitionId;
import io.hydramq.Partitioner;
import io.hydramq.Topic;
import io.hydramq.TopicWriter;
import io.hydramq.listeners.PartitionFlags;
import io.hydramq.listeners.PartitionListener;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;

import static io.hydramq.listeners.Listen.CONTINUOUSLY;
import static io.hydramq.listeners.Listen.REMOVE;

/**
 * @author jfulton
 */
public class BatchTopicWriter implements TopicWriter {

    private Partitioner partitioner;
    private int maxMessages = 100;
    private Topic topic;
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile Map<PartitionId, List<Message>> messageCache = new HashMap<>();
    private volatile List<CompletableFuture<Void>> futures = new ArrayList<>();
    private AtomicInteger messageCacheSize = new AtomicInteger();
    private final SortedMap<PartitionId, PartitionFlags> partitionIds = new TreeMap<>();
    private Int2ObjectAVLTreeMap<PartitionId> partitionNumbers = new Int2ObjectAVLTreeMap<>();
    PartitionListener partitionListener;

    public BatchTopicWriter(Partitioner partitioner, Topic topic) {
        this.partitioner = partitioner;
        this.topic = topic;
        partitionListener = (partitionId, partitionFlags) -> {
            try {
                lock.writeLock().lock();

                if (partitionFlags.isWritable()) {
                    partitionIds.put(partitionId, partitionFlags);
                } else {
                    partitionIds.remove(partitionId);
                }
                populatePartitionIds();
            } finally {
                lock.writeLock().unlock();
            }
        };
        topic.discoverPartitions(partitionListener, CONTINUOUSLY);
    }

    @Override
    public CompletableFuture<Void> write(Message message) {
        try {
            lock.writeLock().lock();
            int partitionNumber = partitioner.partition(message, partitionNumbers.size());
            messageCache.computeIfAbsent(partitionNumbers.get(partitionNumber), partitionId -> new ArrayList<>())
                    .add(message);
            CompletableFuture<Void> future = new CompletableFuture<>();
            futures.add(future);
            if (messageCacheSize.incrementAndGet() >= maxMessages) {
                List<CompletableFuture<Void>> localFutures = futures;
                Map<PartitionId, List<Message>> localMessageCache = messageCache;
                futures = new ArrayList<>();
                messageCache = new HashMap<>();

                topic.write(localMessageCache).thenAccept(aVoid -> {
                    for (CompletableFuture<Void> f : localFutures) {
                        f.complete(null);
                    }
                }).exceptionally(throwable -> {
                    future.completeExceptionally(throwable);
                    return null;
                });
            }
            return future;
        } finally {
            lock.writeLock().unlock();
        }
    }


    private void populatePartitionIds() {
        partitionNumbers.clear();
        int partitionCount = 0;
        for (PartitionId partitionId : partitionIds.keySet()) {
            partitionNumbers.put(partitionCount++, partitionId);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        topic.discoverPartitions(partitionListener, REMOVE);
    }
}
