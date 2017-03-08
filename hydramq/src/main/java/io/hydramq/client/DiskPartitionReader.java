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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.hydramq.MessageSet;
import io.hydramq.Partition;
import io.hydramq.internal.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jfulton
 */
@SuppressWarnings("Duplicates")
public class DiskPartitionReader {

    private static final Logger logger = LoggerFactory.getLogger(DiskPartitionReader.class);

    public static DiskPartitionReader from(Partition partition) {
        return new DiskPartitionReader(partition);
    }

    private Partition partition;
    private Duration readTimeout = Duration.ofMillis(100);
    private int maxMessages = 100;
    private long startOffset = 0;

    public DiskPartitionReader(final Partition partition) {
        this.partition = partition;
    }

    public DiskPartitionReader withTimeout(Duration readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }

    public DiskPartitionReader withMaxMessages(final int maxMessages) {
        Assert.argumentIsTrue(maxMessages > 0, "maxMessages must be greater than 0");
        this.maxMessages = maxMessages;
        return this;
    }

    public DiskPartitionReader startingAt(final long startOffset) {
        Assert.argumentIsTrue(startOffset >= 0, "startOffset should be greater than 0");
        this.startOffset = startOffset;
        return this;
    }

    public DiskPartitionReader startingAtTail() {
        startOffset = Long.MAX_VALUE;
        return this;
    }

    public DiskPartitionReader startingAtHead() {
        startOffset = 0L;
        return this;
    }

    public CompletableFuture<Void> readingToTail(Consumer<MessageSet> messageSetConsumer) {
        return CompletableFuture.runAsync(() -> {
            AtomicLong messageOffset = new AtomicLong(startOffset);
            AtomicBoolean tailReached = new AtomicBoolean(false);
            while (!partition.isClosed() && !tailReached.get()) {
                partition.read(messageOffset.get(), maxMessages, readTimeout).thenAccept(messageSet -> {
                    messageOffset.set(messageSet.nextOffset());
                    if (messageSet.size() > 0) {
                        messageSetConsumer.accept(messageSet);
                    } else {
                        tailReached.set(true);
                    }
                }).join();
            }
        });
    }

    public CompletableFuture<Void> followingTail(Consumer<MessageSet> messageSetConsumer) {
        return CompletableFuture.runAsync(() -> {
            AtomicLong messageOffset = new AtomicLong(startOffset);
            while (!partition.isClosed()) {
                partition.read(messageOffset.get(), maxMessages, readTimeout).thenAccept(messageBatch -> {
                    messageOffset.set(messageBatch.nextOffset());
                    messageSetConsumer.accept(messageBatch);
                }).join();
            }
        });
    }

    public CompletableFuture<Void> readingToTail(BiConsumer<MessageSet, Throwable> messageSetConsumer) {
        return CompletableFuture.runAsync(() -> {
            AtomicLong messageOffset = new AtomicLong(startOffset);
            AtomicBoolean tailReached = new AtomicBoolean(false);
            while (!partition.isClosed() && !tailReached.get()) {
                partition.read(messageOffset.get(), maxMessages, readTimeout).thenAccept(messageSet -> {
                    messageOffset.set(messageSet.nextOffset());
                    if (messageSet.size() > 0) {
                        messageSetConsumer.accept(messageSet, null);
                    } else {
                        tailReached.set(true);
                    }
                }).exceptionally(throwable -> {
                    messageSetConsumer.accept(null, throwable);
                    return null;
                }).join();
            }
        });
    }

    public CompletableFuture<Void> followingTail(BiConsumer<MessageSet, Throwable> messageSetConsumer) {
        return CompletableFuture.runAsync(() -> {
            AtomicLong messageOffset = new AtomicLong(startOffset);
            while (!partition.isClosed()) {
                partition.read(messageOffset.get(), maxMessages, readTimeout).thenAccept(messageBatch -> {
                    messageOffset.set(messageBatch.nextOffset());
                    messageSetConsumer.accept(messageBatch, null);
                }).exceptionally(throwable -> {
                    messageSetConsumer.accept(null, throwable);
                    return null;
                }).join();
            }
        });
    }
}
