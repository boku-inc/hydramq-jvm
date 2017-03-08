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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import io.hydramq.PartitionId;
import io.hydramq.ReaderControl;
import io.hydramq.Topic;
import io.hydramq.TopicReader;
import io.hydramq.TopicWriter;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.listeners.MessageConsumer;
import io.hydramq.listeners.MessageSetConsumer;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
public abstract class AbstractTopicReader implements TopicReader {

    private static final Logger logger = getLogger(AbstractTopicReader.class);
    protected static ExecutorService executorService = Executors.newCachedThreadPool();
    private final Topic topic;

    protected AbstractTopicReader(Topic topic) {
        this.topic = topic;
    }

    abstract protected long startOffset();

    abstract protected int maxMessages();

    @Override
    public void read(MessageSetConsumer messageSetConsumer) {
        for (PartitionId partitionId : topic.partitionIds()) {
            start(partitionId, messageSetConsumer);
        }
    }

    @Override
    public void read(MessageConsumer messageConsumer) {
        read((partitionId, messageSet) -> {
            AtomicLong messageCount = new AtomicLong(messageSet.startOffset());
            messageSet.forEach(message -> messageConsumer.onMessage(partitionId, messageCount.getAndIncrement(), message));
        });
    }

    @Override
    public void read(TopicWriter writer) {
        read((partitionId, messageOffset, message) -> {
            writer.write(message);
        });
    }

    private void start(PartitionId partitionId, MessageSetConsumer messageSetConsumer) {
        executorService.submit(() -> {

            ReaderControl control = new ReaderControl();
            AtomicLong messageOffset = new AtomicLong(startOffset());
            while (!control.isCancelled() && !topic.closeFuture().isDone()) {
                topic.read(partitionId, messageOffset.get(), maxMessages())
                        .thenAccept(messageSet -> {
                            messageOffset.set(messageSet.nextOffset());
                            if (messageSet.size() > 0) {
                                messageSetConsumer.consume(partitionId, messageSet);
                            }
                        }).exceptionally(throwable -> {
                    // TODO: do better
                    throw new HydraRuntimeException("Reading failed on partition " + partitionId);
                }).join();
            }
        });
    }
}
