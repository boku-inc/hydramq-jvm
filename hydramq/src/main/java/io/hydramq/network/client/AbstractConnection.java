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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.hydramq.core.net.Command;
import io.hydramq.core.net.netty.ChannelAttributes;
import io.hydramq.exceptions.HydraConnectionException;
import io.hydramq.exceptions.HydraDisconnectException;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.internal.util.Assert;
import io.hydramq.listeners.Listen;
import io.hydramq.network.ConnectionState;
import io.hydramq.network.ConnectionStateListener;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hydramq.listeners.Listen.ONCE;

/**
 * @author jfulton
 */
public abstract class AbstractConnection implements Connection {

    private static final Logger logger = LoggerFactory.getLogger(AbstractConnection.class);
    private Lock connectionLock = new ReentrantLock();
    private Condition connectedCondition = connectionLock.newCondition();
    private AtomicBoolean connecting = new AtomicBoolean(false);
    private EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    private volatile Channel channel;
    private Bootstrap bootstrap;
    private InetSocketAddress endpoint;
    private CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private Map<ConnectionStateListener, Listen> disconnectionListeners = new HashMap<>();

    public AbstractConnection() {
        this.bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup).channel(NioSocketChannel.class).handler(channelInitializer());
    }

    @Override
    public CompletableFuture<Void> connect(final InetSocketAddress endpoint) {
        Assert.argumentNotNull(endpoint, "endpoint");
        final CompletableFuture<Void> connectFuture = new CompletableFuture<>();

        // Already connected, or a connection is in progress.  Keep the existing endpoint.
        if (isConnected() || connecting.compareAndSet(true, false)) {
            connectFuture.completeExceptionally(new HydraRuntimeException("Already connected or connecting to " + this.endpoint));
            return connectFuture;
        }

        this.endpoint = endpoint;

        final ChannelFuture bootstrapConnect = bootstrap().connect(endpoint);
        channel = bootstrapConnect.channel();
        channel.attr(ChannelAttributes.COMMAND_FUTURES).set(new ConcurrentHashMap<>());
        bootstrapConnect.addListener(f -> {
            if (f.isSuccess()) {
                handshake().whenComplete((aVoid, throwable) -> {
                    List<ConnectionStateListener> handshakeListeners = new ArrayList<>();
                    try {
                        connectionLock.lock();

                        if (throwable != null) {
                            // We connected, but there was a handshake error.
                            channel().disconnect().addListener(closeFuture -> {
                                connecting.set(false);
                                connectFuture.completeExceptionally(new HydraRuntimeException("Handshaking error", throwable));
                                handshakeListeners.addAll(toNotify());
                            });
                        } else { // We successfully connected, and handshake was also successful.
                            channel().closeFuture().addListener(closeFuture -> { // Register disconnect future.  Disconnect can happen for any reason.
                                List<ConnectionStateListener> closeListeners = new ArrayList<>();
                                try {
                                    connectionLock.lock();
                                    // Clean up pending futures.
                                    for (CompletableFuture<Command> pendingResponse : channel().attr(ChannelAttributes.COMMAND_FUTURES).get().values()) {
                                        pendingResponse.completeExceptionally(new HydraDisconnectException(endpoint));
                                    }
                                    closeListeners.addAll(toNotify());
                                } finally {
                                    connectionLock.unlock();
                                }
                                closeListeners.forEach(listener -> listener.onStateChanged(this, ConnectionState.DISCONNECTED));
                            });
                            connecting.set(false);
                            connectedCondition.signalAll();

                            logger.info("Connected to {}!", endpoint);
                            connectFuture.complete(null);
                        }
                    } finally {
                        connectionLock.unlock();
                    }
                    handshakeListeners.forEach(disconnectionListener -> disconnectionListener.onStateChanged(this, ConnectionState.DISCONNECTED));
                });
            } else {
                List<ConnectionStateListener> connectFailureListeners = new ArrayList<>();
                try {
                    connectionLock.lock();
                    connecting.set(false);
                    connectFuture.completeExceptionally(new HydraConnectionException(endpoint, f.cause()));
                    connectFailureListeners.addAll(toNotify());
                } finally {
                    connectionLock.unlock();
                }
                connectFailureListeners.forEach(listener -> listener.onStateChanged(this, ConnectionState.DISCONNECTED));
            }

        });
        return connectFuture;
    }

    private List<ConnectionStateListener> toNotify() {
        List<ConnectionStateListener> listeners = new ArrayList<>();
        disconnectionListeners.forEach((listener, listen) -> {
            if (listen == ONCE) {
                disconnectionListeners.remove(listener);
            }
            listeners.add(listener);
        });
        return listeners;
    }

    @Override
    public CompletableFuture<Void> disconnect() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        channel().close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(final ChannelFuture closeFuture) throws Exception {
                if (closeFuture.isSuccess()) {
                    future.complete(null);
                    logger.info("Disconnected");
                } else {
                    future.completeExceptionally(closeFuture.cause());
                }
            }
        });
        return future;
    }

    @Override
    public void monitor(final ConnectionStateListener connectionStateListener, final Listen listen) {
        try {
            connectionLock.lock();
            if (listen == Listen.REMOVE) {
                this.disconnectionListeners.remove(connectionStateListener);
            } else if (listen == Listen.CONTINUOUSLY) {
                this.disconnectionListeners.put(connectionStateListener, listen);
                if (!isConnected()) {
                    connectionStateListener.onStateChanged(this, ConnectionState.DISCONNECTED);
                }
            } else if (listen == ONCE) {
                if (!isConnected()) {
                    connectionStateListener.onStateChanged(this, ConnectionState.DISCONNECTED);
                } else {
                    disconnectionListeners.put(connectionStateListener, listen);
                }
            }
        } finally {
            connectionLock.unlock();
        }

    }

    //    @Override
//    public CompletableFuture<Void> close() {
//        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
//        if (open.compareAndSet(true, false)) {
//            connectionFailureListener = null;
//            disconnect().thenAccept(aVoid -> eventLoopGroup.shutdownGracefully().addListener(elgFuture -> {
//                if (elgFuture.isSuccess()) {
//                    closeFuture.complete(null);
//                } else {
//                    closeFuture.completeExceptionally(elgFuture.cause());
//                }
//            }));
//            return closeFuture;
//        } else {
//            closeFuture.completeExceptionally(new PuntException("Already closed"));
//            return closeFuture;
//        }
//    }

    @Override
    public CompletableFuture<Void> closeFuture() {
        return closeFuture;
    }

    @Override
    public void close() throws IOException {
        try {
            closeFuture().complete(null);
            channel().close().await();
        } catch (InterruptedException e) {
            throw new HydraRuntimeException(e);
        }
    }

    public InetSocketAddress getEndpoint() {
        if (endpoint == null) {
            throw new HydraRuntimeException("Not connected");
        }
        return endpoint;
    }

    protected abstract CompletableFuture<Void> handshake();

    protected abstract void onCommand(ChannelHandlerContext ctx, Command command);

    protected Bootstrap bootstrap() {
        return bootstrap;
    }

    protected Channel channel() {
        return channel;
    }

    protected abstract ChannelInitializer<Channel> channelInitializer();

    public boolean isConnected() {
        return channel != null && channel().isActive();
    }

    protected void blockForConnection() {
        try {
            connectionLock.lock();
            while (!isConnected()) {
                try {
                    connectedCondition.await();
                } catch (InterruptedException e) {
                    logger.error("Interrupted", e);
                }
            }
        } finally {
            connectionLock.unlock();
        }
    }
}
