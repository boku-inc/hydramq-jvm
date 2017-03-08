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

import io.hydramq.client.DefaultTopicReader;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
public class SimpleConsumer {
    private static final Logger logger = getLogger(SimpleConsumer.class);
    private static final InetSocketAddress endpoint = new InetSocketAddress("localhost", 7070);

    public static void main(String[] args) throws InterruptedException {
        TopicManager topicManager = TopicManagers.network(endpoint);
        Topic topic = topicManager.topic("Example.Messages");

        DefaultTopicReader mtsReader = new DefaultTopicReader(topic);
        mtsReader.setStartOffset(Long.MAX_VALUE);
        mtsReader.setMaxMessages(250);
        mtsReader.start((partitionId, messageSet) -> {
            logger.info("MessageSet - Partition: {} Size: {}", partitionId, messageSet.size());
        });
    }
}
