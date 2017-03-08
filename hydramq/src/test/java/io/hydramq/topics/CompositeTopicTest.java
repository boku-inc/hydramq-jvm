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

package io.hydramq.topics;

import java.util.concurrent.atomic.AtomicInteger;

import io.hydramq.Message;
import io.hydramq.MessageSet;
import io.hydramq.PartitionId;
import io.hydramq.Partitioners;
import io.hydramq.Topic;
import io.hydramq.TopicManager;
import io.hydramq.TopicManagers;
import io.hydramq.TopicWriter;
import io.hydramq.TopicWriters;
import io.hydramq.disk.PersistenceTestsBase;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import static io.hydramq.listeners.Listen.CONTINUOUSLY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
public class CompositeTopicTest extends PersistenceTestsBase {

    private static final Logger logger = getLogger(CompositeTopicTest.class);

    @Test
    public void testSingleTopicPartitionCount() throws Exception {
        try (TopicManager topicManager = TopicManagers.disk(messageStoreDirectory())) {
            CompositeTopic topic = new CompositeTopic("simple", topicManager.topic("topic1"));
            assertThat(topic.writablePartitions(), is(topicManager.topic("topic1").writablePartitions()));
        }
    }

    @Test
    public void testMultipleTopicPartitionCountThroughConstructor() throws Exception {
        try (TopicManager topicManager = TopicManagers.disk(messageStoreDirectory())) {
            int expectedPartitionCount = 0;
            Topic topic1 = topicManager.topic("topic1");
            expectedPartitionCount += topic1.writablePartitions();
            Topic topic2 = topicManager.topic("topic2");
            expectedPartitionCount += topic2.writablePartitions();
            Topic topic3 = topicManager.topic("topic3");
            expectedPartitionCount += topic3.writablePartitions();
            CompositeTopic topic = new CompositeTopic("simple", topic1, topic2, topic3);
            assertThat(topic.writablePartitions(), is(expectedPartitionCount));
        }
    }

    @Test
    public void testMultipleTopicPartitionCountThroughAdd() throws Exception {
        try (TopicManager topicManager = TopicManagers.disk(messageStoreDirectory())) {
            int expectedPartitionCount = 0;
            CompositeTopic topic = new CompositeTopic("simple");
            for (int i = 0; i < 4; i++) {
                Topic t = topicManager.topic("simple" + i);
                expectedPartitionCount += t.writablePartitions();
                topic.add(t);
            }
            assertThat(topic.writablePartitions(), is(expectedPartitionCount));
        }
    }

    @Test
    public void testWriteAndReadAcrossMultipleTopics() throws Exception {
        try (TopicManager topicManager = TopicManagers.disk(messageStoreDirectory())) {
            CompositeTopic topic = new CompositeTopic("simple");
            for (int i = 0; i < 4; i++) {
                Topic t = topicManager.topic("simple" + i);
                topic.add(t);
            }
            TopicWriter writer = TopicWriters.simple(topic, Partitioners.roundRobin());
            for (int i = 0; i < topic.writablePartitions(); i++) {
                writer.write(Message.empty().withInteger("count", i).build()).join();
            }
            AtomicInteger messageCount = new AtomicInteger();
            for (PartitionId partitionId : topic.partitionIds()) {
                logger.info("Reading from partition {}", partitionId);
                MessageSet messageSet = topic.read(partitionId, 0, 10).join();
                assertThat(messageSet.size(), is(1));
                messageSet.forEach(message -> {
                    logger.info("Message Count {}", message.getInteger("count"));
                    assertThat(message.getInteger("count"), is(messageCount.getAndIncrement()));
                });
                assertThat(topic.partitionInfo(partitionId).join().tail(), is(1L));
            }
        }
    }
    @Test
    public void testWriteAndReadAcrossMultipleTopicsWithAddsRemoves() throws Exception {
        try (TopicManager topicManager = TopicManagers.disk(messageStoreDirectory())) {
            CompositeTopic topic = new CompositeTopic("simple");
            topic.discoverPartitions((partitionId, partitionState) -> {
                logger.info("{}: {}", partitionId, partitionState);
            }, CONTINUOUSLY);
            TopicWriter writer = TopicWriters.simple(topic, Partitioners.roundRobin());

            Topic topic1 = topicManager.topic("topic1");
            Topic topic2 = topicManager.topic("topic2");

            topic.add(topic1);
            assertThat(topic.partitionIds().size(), is(topic1.partitionIds().size()));


            for (PartitionId partitionId : topic.partitionIds()) {
                writer.write(Message.empty().build());
            }

            topic.add(topic2);

            for (PartitionId partitionId : topic.partitionIds()) {
                writer.write(Message.empty().build());
            }

            topic.remove(topic1);

            for (PartitionId partitionId : topic.partitionIds()) {
                writer.write(Message.empty().build());
            }

            topic.add(topic1);

            assertThat(topic.writablePartitions(), is(topic1.writablePartitions() + topic2.writablePartitions()));
            for (PartitionId partitionId : topic.partitionIds()) {
                logger.info("Reading from partition {}", partitionId);
                MessageSet messageSet = topic.read(partitionId, 0, 10).join();
                assertThat(messageSet.size(), is(2));
                assertThat(topic.partitionInfo(partitionId).join().tail(), is(2L));
            }
        }
    }
}