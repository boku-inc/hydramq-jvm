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
import java.util.ArrayList;
import java.util.List;

import io.hydramq.HydraServer;
import io.hydramq.TopicManager;
import io.hydramq.TopicManagers;
import io.hydramq.disk.DiskTopicManager;
import io.hydramq.disk.PersistenceTestsBase;
import io.hydramq.listeners.DiscoverTopicsListener;
import org.slf4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static io.hydramq.listeners.Listen.CONTINUOUSLY;
import static io.hydramq.listeners.Listen.REMOVE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
@Test(timeOut = 5000)
public class NetworkTopicManagerTest extends PersistenceTestsBase {

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

    @Test(timeOut = 5000)
    public void testConnection() throws Exception {
        NetworkTopicManager topicManager = new NetworkTopicManager();
        topicManager.connect(endpoint()).thenAccept(aVoid -> logger.info("TopicManager started")).join();
        topicManager.disconnect().thenAccept(aVoid -> logger.info("TopicManager stopped")).join();
    }

    @Test(timeOut = 5000)
    public void testDiscoveryListenerOnce_InitiatedByDisk() throws Exception {
        NetworkTopicManager topicManager = new NetworkTopicManager();
        topicManager.connect(endpoint()).thenAccept(aVoid -> logger.info("TopicManager started")).join();
        List<String> discoveredTopics = new ArrayList<>();
        topicManager.discoverTopics(discoveredTopics::add);
        assertThat(discoveredTopics.size(), is(0));
        diskTopicManager.topic("foo");
        Thread.sleep(100);
        topicManager.discoverTopics(discoveredTopics::add);
        assertThat(discoveredTopics.size(), is(1));
        diskTopicManager.topic("foo").close();
    }

    @Test(timeOut = 5000)
    public void testDiscoveryListenerOnce_InitiatedByNetwork() throws Exception {
        NetworkTopicManager topicManager = new NetworkTopicManager();
        topicManager.connect(endpoint()).thenAccept(aVoid -> logger.info("TopicManager started")).join();
        List<String> discoveredTopics = new ArrayList<>();
        topicManager.discoverTopics(discoveredTopics::add);
        assertThat(discoveredTopics.size(), is(0));
        topicManager.topic("foo");
        topicManager.topic("bar");
        Thread.sleep(100); // Discovery happens asynchronously/eventually
        topicManager.discoverTopics(discoveredTopics::add);
        assertThat(discoveredTopics.size(), is(2));
    }

    @Test(timeOut = 5000)
    public void testDiscoveryListenerContinuously() throws Exception {
        NetworkTopicManager topicManager = new NetworkTopicManager();
        topicManager.connect(endpoint()).thenAccept(aVoid -> logger.info("TopicManager started")).join();
        List<String> discoveredTopics = new ArrayList<>();
        topicManager.discoverTopics(discoveredTopics::add, CONTINUOUSLY);
        assertThat(discoveredTopics.size(), is(0));
        topicManager.topic("foo");
        topicManager.topic("bar");
        Thread.sleep(500); // Discovery happens asynchronously/eventually
        assertThat(discoveredTopics.size(), is(2));
        topicManager.close();
    }

    @Test(timeOut = 5000)
    public void testDiscoveryListenerRemove() throws Exception {
        try (TopicManager topicManager = TopicManagers.network(endpoint())) {
            List<String> discoveredTopics = new ArrayList<>();
            DiscoverTopicsListener listener = discoveredTopics::add;
            topicManager.discoverTopics(listener, CONTINUOUSLY);
            assertThat(discoveredTopics.size(), is(0));
            topicManager.topic("foo");
            topicManager.topic("bar");
            Thread.sleep(100); // Discovery happens asynchronously/eventually
            assertThat(discoveredTopics.size(), is(2));
            topicManager.discoverTopics(listener, REMOVE);
            topicManager.topic("OH");
            topicManager.topic("SNAP");
            Thread.sleep(100);
            assertThat(discoveredTopics.size(), is(2));
        }
    }

    @Test(timeOut = 5000)
    public void testReturnedTopicsEquals() throws Exception {
        try (TopicManager topicManager = TopicManagers.network(endpoint())) {
            assertThat(topicManager.topic("foo").equals(topicManager.topic("foo")), is(true));
        }
    }

    public void testDiscoverTopicsListenerRegisteredBeforeEstablishingConnection() throws Exception {
        diskTopicManager.topic("topic1");
        diskTopicManager.topic("topic2");
        List<String> discoveredTopics = new ArrayList<>();
        NetworkTopicManager topicManager = new NetworkTopicManager();
        topicManager.discoverTopics(discoveredTopics::add, CONTINUOUSLY);
        topicManager.connect(endpoint()).thenAccept(aVoid -> logger.info("TopicManager started")).join();
        assertThat(discoveredTopics.size(), is(2));
        assertThat(discoveredTopics, containsInAnyOrder("topic1", "topic2"));
        diskTopicManager.topic("topic3");
    }


    private InetSocketAddress endpoint() {
        return new InetSocketAddress("localhost", port);
    }
}