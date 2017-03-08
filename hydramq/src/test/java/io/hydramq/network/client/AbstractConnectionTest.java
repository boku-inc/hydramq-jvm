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

package io.hydramq.network.client;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import io.hydramq.HydraServer;
import io.hydramq.PartitionId;
import io.hydramq.TopicManagers;
import io.hydramq.disk.PersistenceTestsBase;
import io.hydramq.internal.util.AsyncUtils;
import io.hydramq.network.ConnectionState;
import io.hydramq.network.NetworkTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static io.hydramq.listeners.Listen.CONTINUOUSLY;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author jfulton
 */
public class AbstractConnectionTest extends PersistenceTestsBase {
    private static final Logger logger = LoggerFactory.getLogger(AbstractConnectionTest.class);

    @Test(timeOut = 10_000)
    public void testOnDisconnectedCalledOnDisconnectedConnection() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        NetworkTopic topic = new NetworkTopic("Foo");

        topic.monitor((connection, state) -> {
            if (state == ConnectionState.DISCONNECTED) {
                latch.countDown();
                logger.info("Latch called");
            }
        }, CONTINUOUSLY);
        latch.await();
    }

    @Test(timeOut = 10_000)
    public void testOnDisconnectedCalledOnInitialConnectionFailure() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        NetworkTopic topic = new NetworkTopic("Foo");

        topic.monitor((connection, state) -> {
            if (state == ConnectionState.DISCONNECTED) {
                latch.countDown();
                logger.info("Latch called");
            }
        }, CONTINUOUSLY);
        topic.connect(new InetSocketAddress("localhost", 10123)).whenComplete((aVoid, throwable) -> {
            logger.info("Error connecting", throwable);
        });
        latch.await();
    }


    @Test(timeOut = 10000)
    public void testOnDisconnectedServerDisconnects() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        HydraServer server = new HydraServer(TopicManagers.disk(messageStoreDirectory()), 0);
        NetworkTopic topic = new NetworkTopic("Foo");

        server.start().thenAccept(port -> {
            logger.info("Server started");
            topic.connect(new InetSocketAddress("localhost", port));
            logger.info("Client connected");
        }).join();

        topic.monitor((connection, state) -> {
            if (state == ConnectionState.DISCONNECTED) {
                logger.info("Client onStateChanged!");
                latch.countDown();
            }
        }, CONTINUOUSLY);

        server.stop().thenAccept(aVoid -> logger.info("Server stopped"));

        latch.await();

    }

    @Test
    public void testBlockOnConnection() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        NetworkTopic topic = new NetworkTopic("Foo");
        AsyncUtils.within(CompletableFuture.runAsync(() -> {
            topic.partitionInfo(PartitionId.create());
        }), Duration.ofMillis(5000)).whenComplete((aVoid, throwable) -> {
            logger.error("{}", throwable);
            assertThat(throwable.getCause(), instanceOf(TimeoutException.class));
            latch.countDown();
        });
        latch.await();
    }
}