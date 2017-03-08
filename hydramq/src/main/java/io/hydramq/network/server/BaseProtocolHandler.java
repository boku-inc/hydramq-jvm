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

import io.hydramq.core.net.Command;
import io.hydramq.core.type.ConversionContext;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author jfulton
 */
@Sharable
public abstract class BaseProtocolHandler extends ChannelDuplexHandler implements ProtocolHandler {

    private ConversionContext conversionContext = initializeConversionContext();

    @Override
    public ConversionContext getConversionContext() {
        return conversionContext;
    }

    protected abstract ConversionContext initializeConversionContext();

    @Override
    public boolean accept(final ByteBuf buffer) {
        return getConversionContext().readable(buffer);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf buffer = (ByteBuf) msg;
            Command command = getConversionContext().read((ByteBuf) msg);
            buffer.release();
            onCommand(ctx, command);
        } else if (msg instanceof Command) {
            onCommand(ctx, (Command)msg);
        }
    }

    public void send(final ChannelHandlerContext ctx, final Command command) {
        ByteBuf buffer = ctx.alloc().buffer();
        conversionContext.write(command, buffer);
        ctx.writeAndFlush(buffer, ctx.voidPromise());
    }

    public abstract void onCommand(ChannelHandlerContext ctx, Command command) throws Exception;
}
