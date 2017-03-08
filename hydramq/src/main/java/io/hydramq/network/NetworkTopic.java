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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.hydramq.CursorInfo;
import io.hydramq.Message;
import io.hydramq.MessageSet;
import io.hydramq.PartitionId;
import io.hydramq.PartitionInfo;
import io.hydramq.Topic;
import io.hydramq.core.PuntException;
import io.hydramq.core.net.Acknowledgement;
import io.hydramq.core.net.Command;
import io.hydramq.core.net.Error;
import io.hydramq.core.net.commands.CursorInfoRequest;
import io.hydramq.core.net.commands.CursorInfoResponse;
import io.hydramq.core.net.commands.PartitionInfoRequest;
import io.hydramq.core.net.commands.PartitionInfoResponse;
import io.hydramq.core.net.commands.WriteCursorRequest;
import io.hydramq.core.net.netty.ChannelAttributes;
import io.hydramq.core.net.netty.ChannelUtils;
import io.hydramq.core.net.netty.CommandDecoder;
import io.hydramq.core.net.netty.CommandEncoder;
import io.hydramq.core.net.protocols.topic.LockListenerNotification;
import io.hydramq.core.net.protocols.topic.LockListenerRequest;
import io.hydramq.core.net.protocols.topic.PartitionIdReadRequest;
import io.hydramq.core.net.protocols.topic.PartitionIdWriteRequest;
import io.hydramq.core.net.protocols.topic.PartitionsDiscoveredNotification;
import io.hydramq.core.net.protocols.topic.ReadResponse;
import io.hydramq.core.net.protocols.topic.TopicHandshake;
import io.hydramq.core.type.ConversionContext;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.internal.apis.TopicInternal;
import io.hydramq.internal.util.Assert;
import io.hydramq.listeners.Listen;
import io.hydramq.listeners.PartitionFlags;
import io.hydramq.listeners.PartitionListener;
import io.hydramq.network.client.AbstractConnection;
import io.hydramq.network.client.RequestResponseHandler;
import io.hydramq.subscriptions.LockListener;
import io.hydramq.subscriptions.LockState;
import io.hydramq.topics.TopicWrapper;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hydramq.listeners.Listen.REMOVE;

/**
 * @author jfulton
 */
public class NetworkTopic extends AbstractConnection implements Topic, TopicInternal {

    private static final Logger logger = LoggerFactory.getLogger(NetworkTopic.class);
    private static final int MAX_VERSION_SUPPORTED = 1;
    public static final int MAX_FRAME_LENGTH = 1024 * 1024;
    private int version = 0;
    private final SortedMap<PartitionId, PartitionFlags> partitions = new TreeMap<>();
    private final String topicName;
    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Set<PartitionListener> partitionListeners = new HashSet<>();
    private Map<UUID, LockListener> subscriptions = new HashMap<>();
    private AtomicInteger acquired = new AtomicInteger();
    private AtomicInteger released = new AtomicInteger();

    public NetworkTopic(final String topicName) {

        this.topicName = topicName;
    }

    @Override
    protected CompletableFuture<Void> handshake() {
        return ChannelUtils.sendForReply(channel(), new TopicHandshake(MAX_VERSION_SUPPORTED, getName(), new HashMap<>()))
                .thenCompose(command -> {
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    if (command instanceof TopicHandshake) {
                        // TODO: remove code duplication in onCommand
                        TopicHandshake handshake = (TopicHandshake) command;
                        List<PartitionListener> listeners = new ArrayList<>();
                        Map<PartitionId, PartitionFlags> changes = new HashMap<>();
                        try {
                            lock.writeLock().lock();
                            handshake.getPartitions().forEach((partitionId, partitionFlags) -> {
                                if (partitions.containsKey(partitionId)) {
                                    if (!partitions.get(partitionId).equals(partitionFlags)) {
                                        partitions.put(partitionId, partitionFlags);
                                        changes.put(partitionId, partitionFlags);
                                    }
                                } else {
                                    // If this partition is newly created, store it without this flag, but propagate the notification
                                    // to downstream components
                                    if (partitionFlags.hasFlag(PartitionFlags.Flag.CREATED)) {
                                        PartitionFlags storedFlags = new PartitionFlags(partitionFlags.toEnumSet());
                                        storedFlags.removeFlag(PartitionFlags.Flag.CREATED);
                                        partitions.put(partitionId, storedFlags);
                                    } else {
                                        partitions.put(partitionId, partitionFlags);
                                    }
                                    changes.put(partitionId, partitionFlags);
                                }
                            });
                            if (changes.size() > 0) {
                                partitionListeners.forEach(listeners::add);
                            }
                            f.complete(null);
                        } finally {
                            lock.writeLock().unlock();
                        }
                        listeners.forEach(listener -> {
                            changes.forEach(listener::onPartitionDiscovered);
                        });
                    } else if (command instanceof Error) {
                        f.completeExceptionally(new PuntException((Error) command));
                    }
                    return f;
                });
    }

    @Override
    protected void onCommand(final ChannelHandlerContext ctx, final Command command) {
        if (command instanceof PartitionsDiscoveredNotification) {
            PartitionsDiscoveredNotification notification = (PartitionsDiscoveredNotification) command;
            Set<PartitionListener> listeners = new HashSet<>();
            Map<PartitionId, PartitionFlags> changes = new HashMap<>();
            try {
                lock.writeLock().lock();
                if (partitions.containsKey(notification.getPartitionId())) {
                    if (!partitions.get(notification.getPartitionId()).equals(notification.getFlags())) {
                        partitions.put(notification.getPartitionId(), notification.getFlags());
                        changes.put(notification.getPartitionId(), notification.getFlags());
                    }
                } else {
                    PartitionFlags flags = notification.getFlags();
                    // If this partition is newly created, store it without this flag, but propagate the notification
                    // to downstream components
                    if (flags.hasFlag(PartitionFlags.Flag.CREATED)) {
                        PartitionFlags storedFlags = new PartitionFlags(flags.toEnumSet());
                        storedFlags.removeFlag(PartitionFlags.Flag.CREATED);
                        partitions.put(notification.getPartitionId(), storedFlags);
                    } else {
                        partitions.put(notification.getPartitionId(), flags);
                    }
                    changes.put(notification.getPartitionId(), notification.getFlags());
                }
                if (changes.size() > 0) {
                    partitionListeners.forEach(listeners::add);
                }
            } finally {
                lock.writeLock().unlock();
            }
            listeners.forEach(listener -> {
                changes.forEach(listener::onPartitionDiscovered);
            });
        } else if (command instanceof LockListenerNotification) {
            try {
                lock.readLock().lock();
                LockListenerNotification notification = (LockListenerNotification) command;
                if (subscriptions.containsKey(notification.getSubscriptionKey())) {
                        if (notification.isLocked()) {
                            subscriptions.get(notification.getSubscriptionKey()).onLockState(notification.getPartitionId(), LockState.LOCKED).whenComplete((aVoid, throwable) -> {
                                ChannelUtils.ack(ctx.channel(), notification); // TODO: try/catch
                                acquired.incrementAndGet();
                            });
                        } else {
                            subscriptions.get(notification.getSubscriptionKey()).onLockState(notification.getPartitionId(), LockState.RELEASING).whenComplete((aVoid, throwable) -> {
                                ChannelUtils.ack(ctx.channel(), notification);
                                released.incrementAndGet();
                            });
                        }
                } else {
                    logger.info("subscription not found");
                }
            } finally {
                lock.readLock().unlock();
            }
        } else {
            logger.warn("Unexpected command: {}, correlationId: {}, futures: {}", command, command.correlationId(), ctx.channel().attr(ChannelAttributes.COMMAND_FUTURES).get().size());
        }
    }

    public AtomicInteger getAcquired() {
        return acquired;
    }

    public AtomicInteger getReleased() {
        return released;
    }

    @Override
    protected ChannelInitializer<Channel> channelInitializer() {
        return new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(final Channel ch) throws Exception {
                ConversionContext conversionContext = ConversionContext.topicProtocol();
                ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
                ch.pipeline().addLast("commandDecoder", new CommandDecoder(conversionContext));
                ch.pipeline().addLast("frameEncoder", new LengthFieldPrepender(4));
                ch.pipeline().addLast("commandEncoder", new CommandEncoder(conversionContext));
                ch.pipeline().addLast("logic", new RequestResponseHandler(NetworkTopic.this));
            }
        };
    }

    @Override
    public String getName() {
        return topicName;
    }

    @Override
    public CompletableFuture<Void> write(PartitionId partitionId, Message message) {
        blockForConnection();
        PartitionIdWriteRequest command = new PartitionIdWriteRequest(partitionId, message);
        CompletableFuture<Command> replyFuture = ChannelUtils.sendForReply(channel(), command);
        return replyFuture.thenCompose(reply -> {
            CompletableFuture<Void> f = new CompletableFuture<>();
            if (reply instanceof Error) {
                f.completeExceptionally(new PuntException("Error code " + ((Error) reply).code()));
            } else {
                f.complete(null);
            }
            return f;
        });
    }

    @Override
    public CompletableFuture<Void> write(PartitionId partitionId, MessageSet messageSet) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<Void> write(Map<PartitionId, List<Message>> messageBatch) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<MessageSet> read(PartitionId partitionId, long messageOffset, int maxMessages) {
        blockForConnection();
        PartitionIdReadRequest readRequest = new PartitionIdReadRequest(partitionId, messageOffset, maxMessages);
        CompletableFuture<Command> replyFuture = ChannelUtils.sendForReply(channel(), readRequest);
        return replyFuture.thenCompose(reply -> {
            CompletableFuture<MessageSet> f = new CompletableFuture<>();
            if (reply instanceof Error) {
                f.completeExceptionally(new HydraRuntimeException("Error code" + ((Error) reply).code()));
            } else if (reply instanceof ReadResponse) {
                f.complete(((ReadResponse) reply).getMessageSet());
            } else {
                f.completeExceptionally(new HydraRuntimeException("Something is fucked up reading PartitionInfo"));
            }
            return f;
        });
    }

    @Override
    public void discoverPartitions(PartitionListener listener) {
        discoverPartitions(listener, Listen.ONCE);
    }

    @Override
    public void discoverPartitions(PartitionListener listener, Listen listen) {
        Assert.argumentNotNull(listener, "listener");
        Assert.argumentNotNull(listen, "listen");
        Map<PartitionId, PartitionFlags> results = new HashMap<>();
        try {
            lock.writeLock().lock();
            if (listen == Listen.ONCE || listen == Listen.CONTINUOUSLY) {
                if (listen == Listen.CONTINUOUSLY) {
                    partitionListeners.add(listener);
                }
                partitions.forEach(results::put);
            } else if (listen == REMOVE) {
                partitionListeners.remove(listener);
            }
        } finally {
            lock.writeLock().unlock();
        }
        results.forEach(listener::onPartitionDiscovered);
    }

    @Override
    public void discoverPartitions(PartitionListener listener, Listen listen, Map<PartitionId, PartitionFlags> knownPartitionStates) {
        Assert.argumentNotNull(listener, "listener");
        Assert.argumentNotNull(listen, "listen");
        Map<PartitionId, PartitionFlags> results = new HashMap<>();
        try {
            lock.writeLock().lock();
            if (listen == Listen.ONCE || listen == Listen.CONTINUOUSLY) {
                if (listen == Listen.CONTINUOUSLY) {
                    partitionListeners.add(listener);
                }
                partitions.forEach(results::put);
            } else if (listen == REMOVE) {
                partitionListeners.remove(listener);
            }
        } finally {
            lock.writeLock().unlock();
        }
        results.forEach((partitionId, partitionState) -> {
            if (!knownPartitionStates.containsKey(partitionId) || !knownPartitionStates.get(partitionId).equals(partitionState)) {
                listener.onPartitionDiscovered(partitionId, partitionState);
            }
        });
    }

    @Override
    public CompletableFuture<PartitionInfo> partitionInfo(PartitionId partitionId) {
        blockForConnection();
        PartitionInfoRequest request = new PartitionInfoRequest(partitionId);
        CompletableFuture<Command> replyFuture = ChannelUtils.sendForReply(channel(), request);
        return replyFuture.thenCompose(reply -> {
            CompletableFuture<PartitionInfo> f = new CompletableFuture<>();
            if (reply instanceof Error) {
                f.completeExceptionally(new HydraRuntimeException("Error reading PartitionInfo " + ((Error) reply).code()));
            } else if (reply instanceof PartitionInfoResponse) {
                f.complete(((PartitionInfoResponse) reply).getPartitionInfo());
            } else {
                f.completeExceptionally(new HydraRuntimeException("Something is fucked up reading PartitionInfo"));
            }
            return f;
        });
    }

    @Override
    public SortedSet<PartitionId> partitionIds() {
        SortedSet<PartitionId> results = new TreeSet<>();
        try {
            lock.readLock().lock();
            results.addAll(partitions.keySet());
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void acquirePartitionLocks(String lockGroup, LockListener lockListener) {
        blockForConnection();
        UUID subscriptionKey = UUID.randomUUID();
        subscriptions.put(subscriptionKey, lockListener);
        LockListenerRequest request = new LockListenerRequest(subscriptionKey, lockGroup, true);
        ChannelUtils.sendForReply(channel(), request).thenAccept(command -> {
            logger.debug("Registered!");
        }).exceptionally(throwable -> {
            logger.error("Punting", throwable);
            return null;
        });
    }

    @Override
    public void acquirePartitionLocks(String lockGroup, LockListener lockListener, Listen listen) {
        blockForConnection();
        if (listen == REMOVE) {
            try {
                lock.writeLock().lock();
                subscriptions.forEach((key, subscription1) -> {
                    if (subscription1 == lockListener) {
                        LockListenerRequest request = new LockListenerRequest(key, lockGroup, false);
                        ChannelUtils.sendForReply(channel(), request).thenAccept(command -> {
                            logger.debug("Unregistered!");
                        }).exceptionally(throwable -> {
                            logger.error("Punting", throwable);
                            return null;
                        });
                        subscriptions.remove(key);
                    }
                });
            } finally {
                lock.writeLock().unlock();
            }
        } else {
            UUID subscriptionKey = UUID.randomUUID();
            subscriptions.put(subscriptionKey, lockListener);
            LockListenerRequest request = new LockListenerRequest(subscriptionKey, lockGroup, true);
            ChannelUtils.sendForReply(channel(), request).thenAccept(command -> {
                if (command instanceof Acknowledgement) {
                    logger.debug("Registered!");
                } else {
                    logger.error("Something is wrong!");
                }
            }).exceptionally(throwable -> {
                logger.error("Punting", throwable);
                return null;
            });
        }
    }

    @Override
    public CompletableFuture<CursorInfo> cursor(PartitionId partitionId, String cursorName) {
        blockForConnection();
        CursorInfoRequest request = new CursorInfoRequest(partitionId, cursorName);
        CompletableFuture<Command> replyFuture = ChannelUtils.sendForReply(channel(), request);
        return replyFuture.thenCompose(reply -> {
            CompletableFuture<CursorInfo> f = new CompletableFuture<>();
            if (reply instanceof Error) {
                f.completeExceptionally(new HydraRuntimeException("Error reading CursorInfo" + ((Error) reply).code()));
            } else if (reply instanceof CursorInfoResponse) {
                f.complete(((CursorInfoResponse) reply).getCursorInfo());
            } else {
                f.completeExceptionally(new HydraRuntimeException("Something is fucked up reading CursorInfo"));
            }
            return f;
        });
    }

    @Override
    public CompletableFuture<Void> cursor(PartitionId partitionId, String cursorName, long messageOffset) {
        blockForConnection();
        WriteCursorRequest request = new WriteCursorRequest(partitionId, cursorName, messageOffset);
        CompletableFuture<Command> replyFuture = ChannelUtils.sendForReply(channel(), request);
        return replyFuture.thenCompose(reply -> {
            CompletableFuture<Void> f = new CompletableFuture<>();
            if (reply instanceof Error) {
                f.completeExceptionally(new PuntException("Error code " + ((Error) reply).code()));
            } else {
                f.complete(null);
            }
            return f;
        });
    }

    @Override
    public int partitions() {
        return partitions.size();
    }

    @Override
    public void partitions(int targetPartitionCount) throws HydraRuntimeException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int writablePartitions() {
        return partitions.size();
    }

    @Override
    public void writablePartitions(int targetWritablePartitionCount) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int readablePartitions() {
        return partitions.size();
    }

    @Override
    public void readablePartitions(int targetReadablePartitionCount) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TopicWrapper) {
            return super.equals(((TopicWrapper) obj).getWrapped());
        } else {
            return super.equals(obj);
        }
    }
}

