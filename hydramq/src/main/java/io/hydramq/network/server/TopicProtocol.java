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

package io.hydramq.network.server;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Meter;
import io.hydramq.PartitionId;
import io.hydramq.Topic;
import io.hydramq.TopicManager;
import io.hydramq.core.net.Acknowledgement;
import io.hydramq.core.net.Command;
import io.hydramq.core.net.Error;
import io.hydramq.core.net.commands.CursorInfoRequest;
import io.hydramq.core.net.commands.PartitionInfoRequest;
import io.hydramq.core.net.commands.WriteCursorRequest;
import io.hydramq.core.net.netty.ChannelAttributes;
import io.hydramq.core.net.netty.ChannelUtils;
import io.hydramq.core.net.protocols.topic.LockListenerRequest;
import io.hydramq.core.net.protocols.topic.NetworkLockListener;
import io.hydramq.core.net.protocols.topic.PartitionIdReadRequest;
import io.hydramq.core.net.protocols.topic.PartitionIdWriteRequest;
import io.hydramq.core.net.protocols.topic.PartitionsDiscoveredNotification;
import io.hydramq.core.net.protocols.topic.ReadResponse;
import io.hydramq.core.net.protocols.topic.TopicHandshake;
import io.hydramq.core.type.ConversionContext;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.internal.apis.TopicInternal;
import io.hydramq.listeners.PartitionFlags;
import io.hydramq.monitoring.Metrics;
import io.hydramq.subscriptions.LockListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hydramq.core.net.netty.ChannelAttributes.LOCK_LISTENERS;
import static io.hydramq.listeners.Listen.CONTINUOUSLY;
import static io.hydramq.listeners.Listen.REMOVE;
import static java.lang.Math.max;

/**
 * @author jfulton
 */
public class TopicProtocol extends BaseProtocolHandler {

    private final AttributeKey<Topic> TOPIC_KEY = AttributeKey.valueOf("topicProtocol");
    private Meter messageTotal = Metrics.regisry.meter("messageTotal");
    private Meter messageWrites = Metrics.regisry.meter("messageWrites");
    private Meter messageReads = Metrics.regisry.meter("messageReads");
    private static final Logger logger = LoggerFactory.getLogger(TopicProtocol.class);
    private static final int maxVersion = 1;
    private TopicManager topicManager;

    public TopicProtocol(final TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    public void onCommand(ChannelHandlerContext ctx, Command command) {
        if (command instanceof TopicHandshake) {
            TopicHandshake handshake = (TopicHandshake) command;
            Topic topic = topicManager.topic(handshake.getTopicName());
            ctx.channel().attr(TOPIC_KEY).set(topic);
            Map<PartitionId, PartitionFlags> knownStates = new HashMap<>();
            topic.discoverPartitions(knownStates::put);

            TopicHandshake reply = handshake.reply(max(handshake.getVersion(), maxVersion), knownStates);
            send(ctx, reply);

            ctx.channel().attr(ChannelAttributes.DISCOVER_PARTITIONS_LISTENER)
                    .set((partitionId, partitionState) -> send(ctx, new PartitionsDiscoveredNotification(partitionId, partitionState)));
            if (topic instanceof TopicInternal) {
                ((TopicInternal) topic).discoverPartitions(ctx.channel().attr(ChannelAttributes.DISCOVER_PARTITIONS_LISTENER).get(), CONTINUOUSLY, knownStates);
            } else {
                getTopic(ctx).discoverPartitions(ctx.channel().attr(ChannelAttributes.DISCOVER_PARTITIONS_LISTENER).get(), CONTINUOUSLY);
            }
        } else if (command instanceof PartitionIdWriteRequest) {
            PartitionIdWriteRequest writeRequest = (PartitionIdWriteRequest) command;
            messageWrites.mark();
            messageTotal.mark();
            try {
                getTopic(ctx).write(writeRequest.getPartitionId(), writeRequest.getMessage())
                        // TODO: Convert these to just send as commands, and let the pipeline take care of it
                        .thenAccept(aVoid -> send(ctx, Acknowledgement.replyTo(writeRequest)))
                        .exceptionally(throwable -> {
                            send(ctx, new Error(writeRequest.correlationId(), 0)); // TODO: better error translation
                            return null;
                        });
            } catch (HydraRuntimeException e) {
                logger.error("Error writing messages", e);
                send(ctx, new Error(writeRequest.correlationId(), 0));  // TODO: better error translation
            }
        } else if (command instanceof PartitionIdReadRequest) {
            PartitionIdReadRequest readRequest = (PartitionIdReadRequest) command;
            try {
                getTopic(ctx).read(readRequest.getPartitionId(), readRequest.getMessageOffset(),
                        readRequest.getMaxMessages())
                        .thenAccept(messages -> {
                            if (messages.size() > 0) {
                                messageReads.mark(messages.size());
                                messageTotal.mark(messages.size());
                            }
                            send(ctx, new ReadResponse(readRequest.correlationId(), messages));
                        })
                        .exceptionally(throwable -> {
                            logger.warn("Error reading messages", throwable);
                            send(ctx, new Error(readRequest.correlationId(), 0)); // TODO: better error translation
                            return null;
                        });
            } catch (HydraRuntimeException e) {
                logger.error("Error reading messages", e);
                send(ctx, new Error(readRequest.correlationId(), 0)); // TODO: better error translation
            }
        } else if (command instanceof PartitionInfoRequest) {
            PartitionInfoRequest request = (PartitionInfoRequest) command;
            try {
                getTopic(ctx).partitionInfo(request.getPartitionId()).thenAccept(partitionInfo -> {
                    send(ctx, request.reply(partitionInfo));
                }).exceptionally(throwable -> {
                    send(ctx, new Error(request.correlationId(), 0));
                    return null;
                });
            } catch (HydraRuntimeException e) {
                logger.error("Error reading PartitionInfo", e);
                send(ctx, new Error(request.correlationId(), 0)); // TODO: better error translation
            }
        } else if (command instanceof LockListenerRequest) {
            LockListenerRequest request = (LockListenerRequest) command;
            try {
                if (request.isRegistering()) {
                    NetworkLockListener listener = new NetworkLockListener(request.getClientKey(), request.getLockGroup(), ctx.channel());
                    ctx.channel().attr(LOCK_LISTENERS).get().putIfAbsent(request.getClientKey(), listener);
                    ctx.channel().attr(TOPIC_KEY).get().acquirePartitionLocks(request.getLockGroup(), listener, CONTINUOUSLY);
                } else {
                    LockListener lockListener = ctx.channel().attr(LOCK_LISTENERS).get().get(request.getClientKey());
                    if (lockListener != null) {
                        ctx.channel().attr(TOPIC_KEY).get().acquirePartitionLocks(request.getLockGroup(), lockListener, REMOVE);
                    }
                }
                ChannelUtils.ack(ctx.channel(), request);

            } catch (HydraRuntimeException e) {
                logger.error("Error reading PartitionInfo", e);
                send(ctx, new Error(request.correlationId(), 0)); // TODO: better error translation
            }
        } else if (command instanceof CursorInfoRequest) {
            CursorInfoRequest request = (CursorInfoRequest) command;

            try {
                getTopic(ctx).cursor(request.getPartitionId(), request.getCursorName()).thenAccept(cursorInfo -> {
                    send(ctx, request.reply(cursorInfo));
                }).exceptionally(throwable -> {
                    send(ctx, new Error(request.correlationId(), 0));
                    return null;
                });
            } catch (HydraRuntimeException e) {
                logger.error("Error reading PartitionInfo", e);
                send(ctx, new Error(request.correlationId(), 0)); // TODO: better error translation
            }

        } else if (command instanceof WriteCursorRequest) {
            WriteCursorRequest request = (WriteCursorRequest) command;
            try {
                getTopic(ctx).cursor(request.getPartitionId(), request.getCursorName(), request.getOffset()).thenAccept(aVoid -> {
                    send(ctx, Acknowledgement.replyTo(request));
                }).exceptionally(throwable -> {
                    send(ctx, new Error(request.correlationId(), 0));
                    return null;
                });
            } catch (HydraRuntimeException e) {
                logger.error("Error writing Cursor", e);
                send(ctx, new Error(request.correlationId(), 0)); // TODO: better error translation
            }
            // TODO: should all command handlers simply throw and let the sync handle it?
        } else {
            ctx.fireChannelRead(command);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().attr(LOCK_LISTENERS).set(new ConcurrentHashMap<>());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // Remove installed partition listeners
        getTopic(ctx).discoverPartitions(ctx.channel().attr(ChannelAttributes.DISCOVER_PARTITIONS_LISTENER).get(), REMOVE);

        // Remove installed subscriptions
        Map<UUID, NetworkLockListener> lockListeners = ctx.channel().attr(LOCK_LISTENERS).get();
        lockListeners.forEach((uuid, networkLockListener) -> {
            ctx.channel().attr(LOCK_LISTENERS).get().remove(uuid);
            ctx.channel().attr(TOPIC_KEY).get().acquirePartitionLocks(networkLockListener.getSubscriptionGroup(), networkLockListener, REMOVE);
        });
    }

    @Override
    protected ConversionContext initializeConversionContext() {
        return ConversionContext.topicProtocol();
    }

    private Topic getTopic(ChannelHandlerContext ctx) {
        return ctx.channel().attr(TOPIC_KEY).get();
    }
}
