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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.hydramq.MessageSet;
import io.hydramq.PartitionId;
import io.hydramq.Topic;
import io.hydramq.internal.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jfulton
 */
@SuppressWarnings("Duplicates")
public class TopicPartitionReader {

    private static final Logger logger = LoggerFactory.getLogger(TopicPartitionReader.class);

    public static TopicPartitionReader from(Topic topic, PartitionId partitionId) {
        return new TopicPartitionReader(topic, partitionId);
    }

    private Topic topic;
    private PartitionId partitionId;
    private int maxMessages = 100;
    private long startOffset = 0;

    public TopicPartitionReader(final Topic topic, PartitionId partitionId) {
        this.topic = topic;
        this.partitionId = partitionId;
    }

    public TopicPartitionReader withMaxMessages(final int maxMessages) {
        Assert.argumentIsTrue(maxMessages > 0, "maxMessages must be greater than 0");
        this.maxMessages = maxMessages;
        return this;
    }

    public TopicPartitionReader startingAt(final long startOffset) {
        Assert.argumentIsTrue(startOffset >= 0, "startOffset should be greater than 0");
        this.startOffset = startOffset;
        return this;
    }

    public TopicPartitionReader startingAtTail() {
        startOffset = Long.MAX_VALUE;
        return this;
    }

    public TopicPartitionReader startingAtHead() {
        startOffset = 0L;
        return this;
    }

    public CompletableFuture<Void> readingToTail(Consumer<MessageSet> messageSetConsumer) {
        // TODO: follow to initial tail read and empty messageSet
        return CompletableFuture.runAsync(() -> {
            AtomicLong messageOffset = new AtomicLong(startOffset);
            AtomicBoolean tailReached = new AtomicBoolean(false);
            while (!tailReached.get()) {
                topic.read(partitionId, messageOffset.get(), maxMessages).thenAccept(messageSet -> {
                    messageOffset.set(messageSet.nextOffset());
                    messageSetConsumer.accept(messageSet);
                    if (messageSet.size() == 0) {
                        tailReached.set(true);
                    }
                }).join();
            }
        });
    }

    public CompletableFuture<Void> followingTail(Consumer<MessageSet> messageSetConsumer) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            future.complete(null);
            AtomicLong messageOffset = new AtomicLong(startOffset);
            AtomicBoolean stop = new AtomicBoolean(false);

            while (!stop.get()) {
                topic.read(partitionId, messageOffset.get(), maxMessages).thenAccept(messageBatch -> {
                    messageOffset.set(messageBatch.nextOffset());
                    messageSetConsumer.accept(messageBatch);
                }).join();
            }
        });
        return future;
    }

    public CompletableFuture<Void> readingToTail(BiConsumer<MessageSet, Throwable> messageSetConsumer) {
        // TODO: follow to initial tail read and empty messageSet
        return CompletableFuture.runAsync(() -> {
            AtomicLong messageOffset = new AtomicLong(startOffset);
            AtomicBoolean tailReached = new AtomicBoolean(false);
            while (!topic.closeFuture().isDone() && !tailReached.get()) {
                topic.read(partitionId, messageOffset.get(), maxMessages).thenAccept(messageBatch -> {
                    messageOffset.set(messageBatch.nextOffset());
                    messageSetConsumer.accept(messageBatch, null);
                    if (messageBatch.size() == 0) {
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
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {

            AtomicLong messageOffset = new AtomicLong(startOffset);
            while (!topic.closeFuture().isDone()) {
                topic.read(partitionId, messageOffset.get(), maxMessages).thenAccept(messageBatch -> {
                    messageOffset.set(messageBatch.nextOffset());
                    messageSetConsumer.accept(messageBatch, null);
                }).exceptionally(throwable -> {
                    messageSetConsumer.accept(null, throwable);
                    return null;
                }).join();
            }
        });
        future.complete(null);
        return future;
    }
}
