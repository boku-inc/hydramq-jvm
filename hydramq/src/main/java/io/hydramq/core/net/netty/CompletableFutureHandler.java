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

package io.hydramq.core.net.netty;

import java.util.concurrent.CompletableFuture;

import io.hydramq.core.net.Command;
import io.hydramq.core.net.Response;
import io.hydramq.exceptions.HydraRuntimeException;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jfulton
 */
@Sharable
public class CompletableFutureHandler extends SimpleChannelInboundHandler<Command> {

    private static final Logger logger = LoggerFactory.getLogger(CompletableFutureHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command command) throws Exception {
        if (command instanceof Response) {
            CompletableFuture<Command> future = ctx.channel().attr(ChannelAttributes.COMMAND_FUTURES).get().remove(command.correlationId());
            if (future != null) {
                future.complete(command);
            } else {
                throw new HydraRuntimeException("Unexpected Response " + command + ". correlationId: " + command.correlationId());
            }
        } else {
            ctx.fireChannelRead(command);
        }
    }
}
