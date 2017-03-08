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

import java.util.HashSet;
import java.util.Set;

import io.hydramq.TopicManager;
import io.hydramq.core.net.Command;
import io.hydramq.core.net.netty.ChannelAttributes;
import io.hydramq.core.net.protocols.topicmanager.TopicDiscoveredNotification;
import io.hydramq.core.net.protocols.topicmanager.TopicManagerHandshake;
import io.hydramq.core.type.ConversionContext;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.internal.apis.TopicManagerInternal;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;

import static io.hydramq.listeners.Listen.CONTINUOUSLY;
import static io.hydramq.listeners.Listen.REMOVE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
public class TopicManagerProtocol extends BaseProtocolHandler {

    private static final Logger logger = getLogger(TopicManagerProtocol.class);
    private final TopicManager topicManager;

    public TopicManagerProtocol(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    @Override
    protected ConversionContext initializeConversionContext() {
        return ConversionContext.topicManagerProtocol();
    }

    @Override
    public void onCommand(ChannelHandlerContext ctx, Command command) throws Exception {
        if (command instanceof TopicManagerHandshake) {
            TopicManagerHandshake handshake = (TopicManagerHandshake) command;
            Set<String> topicNames = new HashSet<>();
            topicManager.discoverTopics(topicNames::add);
            send(ctx, handshake.reply(0, topicNames));

            ctx.channel().attr(ChannelAttributes.DISCOVER_TOPICS_LISTENER)
                    .set(topicName -> send(ctx, new TopicDiscoveredNotification(topicName)));
            if (topicManager instanceof TopicManagerInternal) {
                ((TopicManagerInternal) topicManager)
                        .discoverTopics(ctx.channel().attr(ChannelAttributes.DISCOVER_TOPICS_LISTENER).get(), CONTINUOUSLY, topicNames);
            } else {
                topicManager.discoverTopics(ctx.channel().attr(ChannelAttributes.DISCOVER_TOPICS_LISTENER).get(), CONTINUOUSLY);
            }
        } else {
            throw new HydraRuntimeException("Unsupported command: " + command);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.debug("Deregistering topicDiscovery listener");
        topicManager.discoverTopics(ctx.channel().attr(ChannelAttributes.DISCOVER_TOPICS_LISTENER).get(), REMOVE);
    }
}
