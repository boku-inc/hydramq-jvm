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

package io.hydramq.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import io.hydramq.HydraServer;
import io.hydramq.PartitionId;
import io.hydramq.Topic;
import io.hydramq.disk.DiskTopicManager;
import io.hydramq.disk.PersistenceTestsBase;
import io.hydramq.listeners.PartitionFlags;
import org.slf4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static io.hydramq.listeners.Listen.CONTINUOUSLY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
public class NetworkTopicTest extends PersistenceTestsBase {

    private static final Logger logger = getLogger(NetworkTopicManagerTest.class);
    public int port;
    private HydraServer server;
    private DiskTopicManager diskTopicManager;

    @BeforeMethod(timeOut = 5000)
    public void setup() throws IOException {
        super.setup();
        diskTopicManager = new DiskTopicManager(messageStoreDirectory());
        server = new HydraServer(diskTopicManager, 0);
        server.start().thenAccept(port -> {
            this.port = port;
            logger.info("Server Started on port {}", port);
        }).join();
    }

    @AfterMethod(timeOut = 5000)
    public void cleanup() throws IOException {
        if (server != null) {
            server.stop().thenAccept(aVoid -> logger.info("Server Stopped")).join();
        }
        diskTopicManager.close();
        super.cleanup();
    }

    @Test
    public void testDiscoverPartitionsBeforeConnection() throws Exception {
        NetworkTopic topic = new NetworkTopic("topic1");
        Map<PartitionId, PartitionFlags> discoveredPartitions = new HashMap<>();
        topic.discoverPartitions(discoveredPartitions::put, CONTINUOUSLY);
        assertThat(discoveredPartitions.size(), is(0));
        topic.connect(endpoint()).join();
        assertThat(discoveredPartitions.size(), greaterThan(0));
        assertThat(discoveredPartitions.size(), is(topic.partitions()));
    }

    @Test(timeOut = 10_000)
    public void testDiscoverCreatedPartitions() throws Exception {
        Topic diskTopic = diskTopicManager.topic("topic1");
        NetworkTopic networkTopic = new NetworkTopic("topic1");
        Map<PartitionId, PartitionFlags> discoveredPartitions = new HashMap<>();
        networkTopic.discoverPartitions(discoveredPartitions::put, CONTINUOUSLY);
        networkTopic.discoverPartitions((partitionId, flags) -> logger.info("{}: {}", partitionId, flags), CONTINUOUSLY);
        assertThat(discoveredPartitions.size(), is(0));
        networkTopic.connect(endpoint()).join();

        assertThat(discoveredPartitions.size(), is(networkTopic.partitions()));

        diskTopic.partitions(diskTopic.partitions() + 2);
        Thread.sleep(500);
        assertThat(discoveredPartitions.size(), is(diskTopic.partitions()));
    }

    @Test
    public void testReadExistingCursor() throws Exception {
        Topic diskTopic = diskTopicManager.topic("topic1");
        PartitionId id = getPartitionId(diskTopic);
        diskTopic.cursor(id, "cursor1", 1024L);
        NetworkTopic networkTopic = new NetworkTopic("topic1");
        networkTopic.connect(endpoint()).join();

        networkTopic.cursor(id, "cursor1").thenAccept(cursorInfo -> {
            assertThat(cursorInfo.getOffset(), is(1024L));
        }).join();
    }

    @Test
    public void testWriteNewCursor() throws Exception {
        Topic diskTopic = diskTopicManager.topic("topic1");
        PartitionId id = getPartitionId(diskTopic);

        NetworkTopic networkTopic = new NetworkTopic("topic1");
        networkTopic.connect(endpoint()).join();

        networkTopic.cursor(id, "cursor2", 4532L).whenComplete((aVoid, throwable) -> {
            logger.info("Write cursor over the network!");
        }).join();

        diskTopic.cursor(id, "cursor2").whenComplete((cursorInfo, throwable) -> {
            assertThat(cursorInfo.getOffset(), is(4532L));
        }).join();
    }

    private InetSocketAddress endpoint() {
        return new InetSocketAddress("localhost", port);
    }

}