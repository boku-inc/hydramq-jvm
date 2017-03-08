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

import java.util.ArrayList;
import java.util.List;

import io.hydramq.core.net.Error;
import io.hydramq.core.net.netty.CommandDecoder;
import io.hydramq.core.net.netty.CommandEncoder;
import io.hydramq.core.net.netty.CompletableFutureHandler;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jfulton
 */
@Sharable
public class ProtocolSelector extends ChannelInboundHandlerAdapter {

    public static final String NAME = "protocolSelector";
    private static final Logger logger = LoggerFactory.getLogger(ProtocolSelector.class);
    private List<ProtocolHandler> protocols = new ArrayList<>();
    private CompletableFutureHandler completableFutureHandler = new CompletableFutureHandler();

    public void addProtocol(ProtocolHandler protocolHandler) {
        protocols.add(protocolHandler);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        ByteBuf buffer = (ByteBuf)msg;
        for (ProtocolHandler protocolHandler : protocols) {
            if (protocolHandler.accept(buffer)) {
                logger.info("Loading {} protocol handler", protocolHandler);
                ctx.pipeline().addLast("commandEncoder", new CommandEncoder(protocolHandler.getConversionContext()));
                ctx.pipeline().addLast("commandDecoder", new CommandDecoder(protocolHandler.getConversionContext()));
                ctx.pipeline().addLast("protocol", protocolHandler);
                ctx.pipeline().addLast("responseHandler", completableFutureHandler);
                ctx.pipeline().remove(this);
                ctx.pipeline().fireChannelActive();
                ctx.fireChannelRead(msg);
                return;
            }
        }
        logger.error("No acceptable protocols found to handle client with protocol id of " + buffer.getInt(0));
        ctx.writeAndFlush(new Error(buffer.getInt(1), 5)).addListener(ChannelFutureListener.CLOSE);
    }
}
