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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import io.hydramq.Topic;
import io.hydramq.TopicManager;
import io.hydramq.core.PuntException;
import io.hydramq.core.net.Command;
import io.hydramq.core.net.Error;
import io.hydramq.core.net.netty.ChannelUtils;
import io.hydramq.core.net.netty.CommandDecoder;
import io.hydramq.core.net.netty.CommandEncoder;
import io.hydramq.core.net.protocols.topicmanager.TopicDiscoveredNotification;
import io.hydramq.core.net.protocols.topicmanager.TopicManagerHandshake;
import io.hydramq.core.type.ConversionContext;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.internal.apis.TopicManagerInternal;
import io.hydramq.internal.util.Assert;
import io.hydramq.listeners.DiscoverTopicsListener;
import io.hydramq.listeners.Listen;
import io.hydramq.listeners.TopicStateListener;
import io.hydramq.network.client.AbstractConnection;
import io.hydramq.network.client.RequestResponseHandler;
import io.hydramq.topics.NonClosingTopicWrapper;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import static io.hydramq.listeners.Listen.CONTINUOUSLY;
import static io.hydramq.listeners.Listen.ONCE;

/**
 * @author jfulton
 */
public class NetworkTopicManager extends AbstractConnection implements TopicManager, TopicManagerInternal {

    private static final int MAX_VERSION_SUPPORTED = 1;
    public static final int MAX_FRAME_LENGTH = 1024 * 1024; // TODO: Review.  Also, determine what happens / handle when this is exceeded.
    private int version = 0;
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Set<String> discoveredTopics = new HashSet<>();
    private ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    private Set<DiscoverTopicsListener> discoverTopicsListeners = new HashSet<>();
    private SimpleConnectionManager connectionManager = new SimpleConnectionManager(getEndpoint());

    public NetworkTopicManager() {
    }

    @Override
    public Topic topic(String topicName) throws HydraRuntimeException {
        return new NonClosingTopicWrapper(
                topics.computeIfAbsent(topicName.toLowerCase(), s -> {
                    NetworkTopic topic = new NetworkTopic(topicName);
                    topic.connect(getEndpoint()).join();
                    connectionManager.manage(topic);
                    return topic;
                }));
    }

    @Override
    public void discoverTopics(DiscoverTopicsListener listener) {
        discoverTopics(listener, ONCE);
    }

    @Override
    public void discoverTopics(DiscoverTopicsListener listener, Listen listen) {
        List<String> results = new ArrayList<>();
        try {
            lock.writeLock().lock();
            if (listen == ONCE || listen == CONTINUOUSLY) {
                if (listen == CONTINUOUSLY) {
                    discoverTopicsListeners.add(listener);
                }
                discoveredTopics.forEach(results::add);
            } else {
                discoverTopicsListeners.remove(listener);
            }
        } finally {
            lock.writeLock().unlock();
        }
        results.forEach(listener::onTopicsDiscovered);
    }

    @Override
    public void discoverTopics(DiscoverTopicsListener listener, Listen listen, Set<String> knownTopicNames) {
        Assert.argumentNotNull(listener, "listener");
        Assert.argumentNotNull(listen, "listen");
        List<String> results = new ArrayList<>();
        try {
            lock.writeLock().lock();
            if (listen == Listen.ONCE || listen == Listen.CONTINUOUSLY) {
                if (listen == Listen.CONTINUOUSLY) {
                    discoverTopicsListeners.add(listener);
                }
                topics.keySet().forEach(topicName -> {
                    if (!knownTopicNames.contains(topicName)) {
                        results.add(topicName);
                    }
                });
            } else if (listen == Listen.REMOVE) {
                discoverTopicsListeners.remove(listener);
            }
        } finally {
            lock.writeLock().unlock();
        }
        results.forEach(listener::onTopicsDiscovered);
    }

    @Override
    public void topicStatuses(TopicStateListener listener) {
        throw new UnsupportedOperationException("Not implemented!");
    }

    @Override
    public void topicStatuses(TopicStateListener listener, Listen status) {
        throw new UnsupportedOperationException("Not implemented!");
    }

    @Override
    protected CompletableFuture<Void> handshake() {
        return ChannelUtils.sendForReply(channel(), new TopicManagerHandshake(MAX_VERSION_SUPPORTED, new HashSet<>()))
                .thenCompose(command -> {
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    if (command instanceof TopicManagerHandshake) {
                        TopicManagerHandshake handshake = (TopicManagerHandshake) command;
                        List<DiscoverTopicsListener> listeners = new ArrayList<>();
                        List<String> newTopics = new ArrayList<>();
                        try {
                            lock.writeLock().lock();
                            newTopics.addAll(handshake.getTopicNames()
                                    .stream()
                                    .filter(topicName -> discoveredTopics.add(topicName))
                                    .collect(Collectors.toList()));
                            if (newTopics.size() > 0) {
                                discoverTopicsListeners.forEach(listeners::add);
                            }
                            f.complete(null);
                        } finally {
                            lock.writeLock().unlock();
                        }
                        listeners.forEach(listener -> {
                            newTopics.forEach(listener::onTopicsDiscovered);
                        });
                    } else if (command instanceof Error) {
                        f.completeExceptionally(new PuntException((Error) command));
                    } else {
                        f.completeExceptionally(new HydraRuntimeException("Unsupported command: " + command)); // TODO: generalize this
                    }
                    return f;
                });
    }

    @Override
    protected void onCommand(ChannelHandlerContext ctx, Command command) {
        if (command instanceof TopicDiscoveredNotification) {
            TopicDiscoveredNotification notification = (TopicDiscoveredNotification) command;
            try {
                lock.writeLock().lock();
                // If we've onStateChanged, and then reconnected, we'll get a stream of all topicNames from the server.
                // Only notify for NEW topics added between disconnect and reconnect
                if (discoveredTopics.add(notification.getTopicName())) {
                    for (DiscoverTopicsListener listener : discoverTopicsListeners) {
                        listener.onTopicsDiscovered(notification.getTopicName());
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }
        } else {
            throw new HydraRuntimeException("Unsupported command: " + command);
        }
    }

    @Override
    protected ChannelInitializer<Channel> channelInitializer() {
        return new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ConversionContext conversionContext = ConversionContext.topicManagerProtocol();
                ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
                ch.pipeline().addLast("commandDecoder", new CommandDecoder(conversionContext));
                ch.pipeline().addLast("frameEncoder", new LengthFieldPrepender(4));
                ch.pipeline().addLast("commandEncoder", new CommandEncoder(conversionContext));
                ch.pipeline().addLast("logic", new RequestResponseHandler(NetworkTopicManager.this));
            }
        };
    }
}
