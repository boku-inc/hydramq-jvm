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

package io.hydramq.readers;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.hydramq.PartitionId;
import io.hydramq.Topic;
import io.hydramq.listeners.MessageConsumer;
import io.hydramq.listeners.MessageSetConsumer;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
public class PartitionReader {

    private static final Logger logger = getLogger(PartitionReader.class);
    private final Topic topic;
    private final PartitionId partitionId;
    private final AtomicLong messageOffset;
    private int maxMessages = 100;
    private Duration timeout = Duration.ofMillis(5000);
    private final MessageSetConsumer messageSetConsumer;
    private static ExecutorService executorService = Executors.newCachedThreadPool();
    private volatile CompletableFuture<Void> startFuture;
    private volatile CompletableFuture<Void> stopFuture;
    private AtomicBoolean started = new AtomicBoolean(false);

    public PartitionReader(Topic topic, PartitionId partitionId, long startOffset, MessageSetConsumer messageSetConsumer) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.messageOffset = new AtomicLong(startOffset);
        this.messageSetConsumer = messageSetConsumer;
    }

    public PartitionReader(Topic topic, PartitionId partitionId, long startOffset, MessageConsumer messageConsumer) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.messageOffset = new AtomicLong(startOffset);
        this.messageSetConsumer = (id, messageSet) -> {
            AtomicLong messageCount = new AtomicLong(messageSet.startOffset());
            messageSet.forEach(message -> messageConsumer.onMessage(partitionId, messageCount.getAndIncrement(), message));
        };
    }

    public synchronized CompletableFuture<Void> start() {
        if (started.compareAndSet(false, true)) {
            startFuture = new CompletableFuture<>();
            run();
        }
        // return an already existing future, that may or may not be started.
        return startFuture;
    }

    public synchronized CompletableFuture<Void> stop() {
        if (started.compareAndSet(true, false)) {
            stopFuture = new CompletableFuture<>();
            return stopFuture;
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private void run() {
        executorService.execute(() -> {
            // TODO: handle topic close
            try {
                while (started.get()) {
                    topic.read(partitionId, messageOffset.get(), maxMessages)
                            .thenAccept(messageSet -> {
                                messageOffset.set(messageSet.nextOffset());
                                if (messageSet.size() > 0) {
                                    messageSetConsumer.consume(partitionId, messageSet);
                                }
                            }).exceptionally(throwable -> {
                        logger.error("Error processing messageSet", throwable);
                        return null;
                    }).join();
                }
                stopFuture.complete(null);
                stopFuture = null;
                started.set(false);
                startFuture = null;
            } catch (Exception ex) {
                logger.error("Error!", ex);
            }
        });
        startFuture.complete(null);
    }
}
