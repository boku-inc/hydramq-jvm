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

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import io.hydramq.HydraServer;
import io.hydramq.TopicManager;
import io.hydramq.TopicManagers;
import io.hydramq.disk.PersistenceTestsBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static io.hydramq.listeners.Listen.CONTINUOUSLY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class SimpleConnectionManagerTest extends PersistenceTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(SimpleConnectionManagerTest.class);

    @Test
    public void testReconnect() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        InetSocketAddress endpoint = new InetSocketAddress("localhost", 10124);
        TopicManager topicManager = TopicManagers.disk(messageStoreDirectory());
        HydraServer server = new HydraServer(topicManager, 10124);
        NetworkTopic topic = new NetworkTopic("Foo");
        topic.monitor((connection, state) -> {
            if (state == ConnectionState.DISCONNECTED) {
                latch.countDown();
            }
        }, CONTINUOUSLY);

        server.start().join();

        ConnectionManager connectionManager = new SimpleConnectionManager(endpoint);
        connectionManager.manage(topic);

        server.stop().thenAccept(aVoid -> logger.info("Server stopped")).join();

        server = new HydraServer(topicManager, 10124);
        server.start().join();

        topic.close();

        topic.closeFuture().whenComplete((aVoid, throwable) -> {
            logger.info("Client onStateChanged");
            assertThat(throwable, is(nullValue()));
        }).join();

        server.stop().join();
        latch.await();
    }
}