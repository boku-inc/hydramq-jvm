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

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.hydramq.Message;
import io.hydramq.PartitionId;
import io.hydramq.Partitioner;
import io.hydramq.Topic;
import io.hydramq.TopicWriter;
import io.hydramq.listeners.PartitionFlags;
import io.hydramq.listeners.PartitionListener;
import io.hydramq.partitioners.RandomPartitioner;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hydramq.listeners.Listen.CONTINUOUSLY;
import static io.hydramq.listeners.Listen.REMOVE;

/**
 *
 * @author jfulton
 */
public class DefaultTopicWriter implements TopicWriter {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTopicWriter.class);
    private Lock lock = new ReentrantLock();
    private Condition writable = lock.newCondition();
    private final Topic topic;
    private final Partitioner partitioner;
    private final SortedMap<PartitionId, PartitionFlags> partitionIds = new TreeMap<>();
    private Int2ObjectAVLTreeMap<PartitionId> partitionNumbers = new Int2ObjectAVLTreeMap<>();
    private PartitionListener partitionListener;

    public DefaultTopicWriter(Topic topic, Partitioner partitioner) {
        this.topic = topic;
        this.partitioner = partitioner;
        partitionListener = (partitionId, partitionFlags) -> {
            try {
                lock.lock();
                if (partitionFlags.isWritable()) {
                    partitionIds.put(partitionId, partitionFlags);
                } else {
                    partitionIds.remove(partitionId);
                }
                populatePartitionIds();
                writable.signalAll();
            } finally {
                lock.unlock();
            }
        };
        topic.discoverPartitions(partitionListener, CONTINUOUSLY);
    }

    public DefaultTopicWriter(Topic topic) {
        this(topic, new RandomPartitioner());
    }

    @Override
    public CompletableFuture<Void> write(Message message) {
        try {
            lock.lock();
            while (partitionIds.size() == 0) {
                try {
                    writable.await();
                } catch (InterruptedException e) {
                    logger.error("Interrupted", e);
                }
            }
            int partitionNumber = partitioner.partition(message, partitionNumbers.size());
            return topic.write(partitionNumbers.get(partitionNumber), message);
        } finally {
            lock.unlock();
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
