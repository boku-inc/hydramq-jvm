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

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import io.hydramq.core.net.netty.ChannelAttributes;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jfulton
 */
public class HydraServerTransport {

    private static final Logger logger = LoggerFactory.getLogger(HydraServerTransport.class);
    public static final int MAX_FRAME_LENGTH = 1024 * 1024;
    private ProtocolSelector protocolSelector;
    private EventLoopGroup boss = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());
    private EventExecutorGroup worker = new DefaultEventExecutorGroup(5);
    private Channel channel;
    private boolean verbose = false;
    private final int port;

    public HydraServerTransport(final ProtocolSelector protocolSelector, int port) {
        this.protocolSelector = protocolSelector;
        this.port = port;
    }

    public CompletableFuture<Integer> start() {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(boss);
            bootstrap.channel(NioServerSocketChannel.class);
            bootstrap.childHandler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(final Channel ch) throws Exception {
                    ch.attr(ChannelAttributes.COMMAND_FUTURES).set(new ConcurrentHashMap<>());
                    ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
                    ch.pipeline().addLast("frameEncoder", new LengthFieldPrepender(4));
                    if (verbose) {
                        ch.pipeline().addLast(new LoggingHandler(HydraServerTransport.class, LogLevel.INFO));
                    }
                    ch.pipeline().addLast(ProtocolSelector.NAME, protocolSelector);
                }
            });
            channel = bootstrap.bind(port).sync().channel();
            future.complete(((InetSocketAddress)channel.localAddress()).getPort());
        } catch (InterruptedException ex) {
            future.completeExceptionally(ex);
        }
        return future;
    }

    public CompletableFuture<Void> stop() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            // TODO: more robust shutdown
            channel.close().sync();
            worker.shutdownGracefully().sync();
            boss.shutdownGracefully().sync();
            logger.info("Shut down");
            future.complete(null);
        } catch (InterruptedException e) {
            logger.error("Error shutting down", e);
            future.completeExceptionally(e);
        }
        return future;
    }
}
