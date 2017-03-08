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

import java.util.function.BiFunction;
import java.util.function.Function;

import io.hydramq.Message;
import io.hydramq.Partitioners;
import io.hydramq.TopicManager;
import io.hydramq.TopicWriter;
import io.hydramq.TopicWriters;
import io.hydramq.readers.MulticastTopicReader;

import static io.hydramq.PropertyKeys.RequestReply.CLIENT_ID;
import static io.hydramq.PropertyKeys.RequestReply.CORRELATION_ID;
import static io.hydramq.PropertyKeys.RequestReply.REPLY_TO;

/**
 * @author jfulton
 */
public class ReplyCoordinator {

    private String incomingTopic;
    private TopicManager topicManager;
    private BiFunction<String, TopicManager, TopicWriter> writerFactory;

    public ReplyCoordinator(TopicManager topicManager, final String incomingTopic) {
        // TODO: cache writers
        this.topicManager = topicManager;
        this.incomingTopic = incomingTopic;
        writerFactory = ((topicName, topics) -> TopicWriters.simple(topics.topic(topicName),Partitioners.random()));
    }

    // TODO: add stop
    public void start(Function<Message, Message> replyFunction) {
        new MulticastTopicReader(topicManager.topic(incomingTopic)).read((partitionId, messageOffset, message) -> {
            if (message.hasString(REPLY_TO) && message.hasString(CORRELATION_ID) && message.hasString(CLIENT_ID)) {
                TopicWriter replyTopic = writerFactory.apply(message.getString(REPLY_TO), topicManager);
                Message reply = replyFunction.apply(message);
                reply.setString(CLIENT_ID, message.getString(CLIENT_ID));
                reply.setString(CORRELATION_ID, message.getString(CORRELATION_ID));
                replyTopic.write(reply);
            }
        });
    }
}
