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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.hydramq.network.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hydramq.listeners.Listen.CONTINUOUSLY;
import static io.hydramq.listeners.Listen.REMOVE;

/**
 * @author jfulton
 */
public class SimpleConnectionManager implements ConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(SimpleConnectionManager.class);
    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private Map<Connection,ReconnectStrategy> connections = new HashMap<>();
    private InetSocketAddress endpoint;

    public SimpleConnectionManager(final InetSocketAddress endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void manage(final Connection connection) {
        if (connections.putIfAbsent(connection, new ReconnectStrategy()) == null) {
            ConnectionStateListener connectionStateListener = (conn, state) -> {
                if (!conn.closeFuture().isDone() && state == ConnectionState.DISCONNECTED) {
                    int delay = connections.get(conn).nextDelay();
                    logger.info("Reconnecting {} to {} in {}ms", conn, endpoint, delay);
                    executorService
                            .schedule(() -> conn.connect(endpoint).whenComplete((aVoid, throwable) -> {
                                if (conn.isConnected()) {
                                    connections.get(conn).reset();
                                }
                            }), delay, TimeUnit.MILLISECONDS);
                }
            };
            connection.monitor(connectionStateListener, CONTINUOUSLY);
            connection.closeFuture().whenComplete((aVoid, throwable) -> {
                connections.remove(connection);
                connection.monitor(connectionStateListener, REMOVE);
            });
        }
    }

    private class ReconnectStrategy {

        private AtomicInteger delay = new AtomicInteger();

        public int nextDelay() {
            return delay.getAndSet(1000);
        }

        public void reset() {
            delay.set(0);
        }
    }
}
