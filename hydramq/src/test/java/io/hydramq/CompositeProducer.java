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

package io.hydramq;

import java.net.InetSocketAddress;

import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
public class CompositeProducer {

    private static final Logger logger = getLogger(CompositeProducer.class);

    public static void main(String[] args) {

        TopicManager remoteTM = TopicManagers.network(new InetSocketAddress("10.11.101.211", 7070));
        TopicManager localTM = TopicManagers.network(new InetSocketAddress("10.11.101.211", 7070));

        TopicManager composite = TopicManagers.composite(remoteTM, localTM);

        composite.discoverTopics(topicName -> {
            if (topicName.compareToIgnoreCase("Example.Messages") == 0) {
                logger.info("Starting reader");
                Topic topic = composite.topic(topicName);
                TopicWriter writer = TopicWriters.simple(topic);
                for (int i = 0; i < 1_000_000; i++) {
                    writer.write(Message.empty().withInteger("count", i).build()).join();
                }
            }
        });
    }
}
