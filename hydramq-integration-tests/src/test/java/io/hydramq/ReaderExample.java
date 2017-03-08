package io.hydramq;

import java.net.InetSocketAddress;

import io.hydramq.client.TopicPartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * @author jfulton
 */
public class ReaderExample {
    private static final Logger logger = LoggerFactory.getLogger(ReaderExample.class);

    @Test
    public void test() throws Exception {
        Topic topic = TopicManagers.network(new InetSocketAddress("localhost", 9090)).topic("Example");

        // Read all messages from start of time, and stop at the end
        TopicPartitionReader
                .from(topic, topic.partitionIds().first())
                .startingAtHead()
                .withMaxMessages(100)
                .readingToTail(messages -> {
                    for (Message message : messages) {
                        logger.info("Message received!");
                    }
                });

        // Read any new messages added, and listen indefinitely
        TopicPartitionReader
                .from(topic, topic.partitionIds().first())
                .startingAtTail()
                .followingTail(messages -> {
                    for (Message message : messages) {
                        logger.info("Message received!");
                    }
                });

    }
}
