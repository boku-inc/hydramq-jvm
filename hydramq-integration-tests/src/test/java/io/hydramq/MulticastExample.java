package io.hydramq;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * @author jfulton
 */
public class MulticastExample {
    private static final Logger logger = LoggerFactory.getLogger(MulticastExample.class);

    @Test
    public void test() throws Exception {
        TopicManager topicManager = TopicManagers.network(new InetSocketAddress("localhost", 9090));

        Topic mts = topicManager.topic("System.Commands");

        TopicReaders.multicast(mts).read((partitionId, messageOffset, message) -> {
            if (message.getString("Command").equals("InvalidateCaches")) {
                logger.info("Invalidating caches...");
            }
        });

        topicManager.discoverTopics(topicName -> {
            logger.info("Topic Discovered {}", topicName);
        });
    }
}
