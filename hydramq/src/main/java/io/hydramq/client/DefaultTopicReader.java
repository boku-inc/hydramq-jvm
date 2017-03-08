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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.hydramq.PartitionId;
import io.hydramq.Topic;
import io.hydramq.TopicWriter;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.listeners.MessageConsumer;
import io.hydramq.listeners.MessageSetConsumer;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
public class DefaultTopicReader {

    private static final Logger logger = getLogger(DefaultTopicReader.class);
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private final Topic topic;
    private long startOffset = 0;
    private int maxMessages = 100;
    private AtomicBoolean started = new AtomicBoolean(false);

    public DefaultTopicReader(Topic topic) {
        this.topic = topic;
    }

    public DefaultTopicReader start(MessageSetConsumer messageSetConsumer) {
        if (started.compareAndSet(false, true)) {
            for (PartitionId partitionId : topic.partitionIds()) {
                start(partitionId, messageSetConsumer);
            }
        } else {
            throw new HydraRuntimeException("Already started");
        }
        return this;
    }

    public DefaultTopicReader start(MessageConsumer messageConsumer) {
        if (started.compareAndSet(false, true)) {
            for (PartitionId partitionId : topic.partitionIds()) {
                start(partitionId, messageConsumer);
            }
        } else {
            throw new HydraRuntimeException("Already started");
        }
        return this;
    }

    public DefaultTopicReader start(TopicWriter topicWriter) {
        if (started.compareAndSet(false, true)) {
            for (PartitionId partitionId : topic.partitionIds()) {
                start(partitionId, (MessageConsumer) (id, messageOffset, message) -> topicWriter.write(message));
            }
        } else {
            throw new HydraRuntimeException("Already started");
        }
        return this;
    }

    private void start(PartitionId partitionId, MessageSetConsumer messageSetConsumer) {
        executorService.submit(() -> {
            AtomicLong messageOffset = new AtomicLong(startOffset);
            // TODO: Replace with onClose
            while (started.get()) {
                topic.read(partitionId, messageOffset.get(), maxMessages)
                        .thenAccept(messageSet -> {
                            messageOffset.set(messageSet.nextOffset());
                            if (messageSet.size() > 0) {
                                messageSetConsumer.consume(partitionId, messageSet);
                            }
                        }).exceptionally(throwable -> {
                    logger.error("Error!", throwable);
                    return null;
                }).join();
            }
        });
    }

    private void start(PartitionId partitionId, MessageConsumer messageConsumer) {
        executorService.submit(() -> {
            AtomicLong messageOffset = new AtomicLong(startOffset);
            while (started.get()) {
                topic.read(partitionId, messageOffset.get(), maxMessages)
                        .thenAccept(messageSet -> {
                            messageOffset.set(messageSet.nextOffset());
                            AtomicInteger messageCount = new AtomicInteger();
                            messageSet.forEach(message -> messageConsumer.onMessage(partitionId, messageSet.startOffset() + messageCount.getAndIncrement(), message));
                        }).exceptionally(throwable -> {
                    logger.error("Error!", throwable);
                    return null;
                }).join();
            }
        });
    }

    public void stop() {
        started.set(false);
    }

    public DefaultTopicReader setStartOffset(long startOffset) {
        this.startOffset = startOffset;
        return this;
    }

    public DefaultTopicReader setMaxMessages(int maxMessages) {
        this.maxMessages = maxMessages;
        return this;
    }
}
