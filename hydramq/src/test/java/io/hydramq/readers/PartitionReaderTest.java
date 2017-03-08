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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import io.hydramq.Message;
import io.hydramq.Topic;
import io.hydramq.TopicManager;
import io.hydramq.TopicManagers;
import io.hydramq.TopicWriter;
import io.hydramq.TopicWriters;
import io.hydramq.disk.PersistenceTestsBase;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
public class PartitionReaderTest  extends PersistenceTestsBase{
    private static final Logger logger = getLogger(PartitionReaderTest.class);

    @Test(timeOut = 30_000)
    public void testSimpleReads() throws Exception {
        int sendCount = 10_000;
        CountDownLatch latch = new CountDownLatch(sendCount);
        Map<Integer, String> cache = new ConcurrentHashMap<>();

        List<PartitionReader> readers = new ArrayList<>();

        try (TopicManager topicManager = TopicManagers.disk(messageStoreDirectory())) {
            Topic topic = topicManager.topic("topic");
            Topic topic2 = topicManager.topic("topic2");
            TopicWriter writer2 = TopicWriters.simple(topic2);
            topic.discoverPartitions((partitionId, flags) -> {
                PartitionReader reader = new PartitionReader(topic, partitionId, 0, (partitionId1, messageOffset, message) -> {
                    logger.info("Partition: {}, Offset: {}, Message: {}", partitionId, messageOffset, message.bodyAsString());
                    latch.countDown();
                    writer2.write(message);
                    cache.put(message.getInteger("id"), message.bodyAsString());
                });
                readers.add(reader);
                reader.start().join();
                // Ensure we can start it twice without issue
                reader.start().join();
            });


            TopicWriter writer = TopicWriters.simple(topic);
            for (int i = 0; i < sendCount; i++) {
                if (i == 2500) {
                    for (PartitionReader reader : readers) {
                        // Demonstrate stopping
                        reader.stop().join();
                    }
                }
                writer.write(Message.withBodyAsString("Message " + i).withInteger("id", i).build());
                if (i == 7500) {
                    for (PartitionReader reader : readers) {
                        // And then restarting
                        reader.start().join();
                    }
                }
            }

            latch.await();

            for (PartitionReader reader : readers) {
                reader.stop().join();
            }

            // Ensure we can stop it twice
            for (PartitionReader reader : readers) {
                reader.stop().join();
            }

            assertThat(cache.size(), is(sendCount));
        }
    }
}