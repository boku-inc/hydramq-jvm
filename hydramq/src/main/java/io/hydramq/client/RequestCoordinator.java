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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import io.hydramq.Message;
import io.hydramq.Partitioners;
import io.hydramq.TopicManager;
import io.hydramq.TopicWriter;
import io.hydramq.TopicWriters;
import io.hydramq.internal.util.AsyncUtils;
import io.hydramq.readers.MulticastTopicReader;

import static io.hydramq.PropertyKeys.RequestReply.CLIENT_ID;
import static io.hydramq.PropertyKeys.RequestReply.CORRELATION_ID;
import static io.hydramq.PropertyKeys.RequestReply.REPLY_TO;

/**
 * @author jfulton
 */
public class RequestCoordinator {

    private final String replyTo;
    private String clientId = UUID.randomUUID().toString();
    private TopicManager topicManager;
    private TopicWriter topicWriter;
    private Map<String,CompletableFuture<Message>> pendingRequests = new ConcurrentHashMap<>();

    public RequestCoordinator(TopicManager topicManager, String outgoingTopic, String replyTo) {
        this.replyTo = replyTo;
        this.topicManager = topicManager;
        this.topicWriter = TopicWriters.simple(topicManager.topic(outgoingTopic), Partitioners.random());
    }

    public CompletableFuture<Message> sendForReply(Message message, Duration timeout) {
        final String correlationId = UUID.randomUUID().toString();
        message.setString(CLIENT_ID, clientId);
        message.setString(CORRELATION_ID, correlationId); // Could be more efficient, but this is at least safe
        message.setString(REPLY_TO, replyTo);
        CompletableFuture<Message> responseFuture = new CompletableFuture<>();
        pendingRequests.put(correlationId, responseFuture);

        CompletableFuture<Message> timeoutFuture = AsyncUtils.within(responseFuture, timeout);
        timeoutFuture.exceptionally(throwable -> {
              pendingRequests.remove(correlationId);
            return null;
        });
        topicWriter.write(message).join();
        return timeoutFuture;
    }

    public void start() {
        new MulticastTopicReader(topicManager.topic(replyTo)).read((partitionId, messageSet) -> {
            messageSet.forEach(message -> {
                if (message.hasString(CLIENT_ID) && message.hasString(CORRELATION_ID)) {
                    if (message.getString(CLIENT_ID).equals(clientId)) {
                        CompletableFuture<Message> future = pendingRequests.remove(message.getString(CORRELATION_ID));
                        if (future != null) {
                            future.complete(message);
                        }
                    }
                }
            });
        });
    }
}
