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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import io.hydramq.Message;
import io.hydramq.Partitioner;
import io.hydramq.TopicManager;
import io.hydramq.TopicWriter;

/**
 * @author jfulton
 */
public class Router implements TopicWriter {

    private TopicManager topicManager;
    private Function<Message, String> router;
    private Partitioner partitioner;
    private Map<String, TopicWriter> topicWriterCache = new ConcurrentHashMap<>();

    public Router(TopicManager topicManager, Partitioner partitioner, Function<Message, String> router) {
        this.topicManager = topicManager;
        this.router = router;
        this.partitioner = partitioner;
    }

    @Override
    public CompletableFuture<Void> write(Message message) {
        String topicName = router.apply(message);
        TopicWriter writer = topicWriterCache.computeIfAbsent(topicName, s -> new DefaultTopicWriter(topicManager.topic(topicName), partitioner));
        return writer.write(message);
    }
}
