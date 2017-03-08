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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import io.hydramq.core.net.Acknowledgement;
import io.hydramq.core.net.Command;
import io.hydramq.internal.util.AsyncUtils;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jfulton
 */
public class ChannelUtils {

    private static final Logger logger = LoggerFactory.getLogger(ChannelUtils.class);

    public static void send(Channel channel, Command command) {
        channel.writeAndFlush(command);
    }

    public static CompletableFuture<Command> sendForReply(Channel channel, Command command) {
        CompletableFuture<Command> future = new CompletableFuture<>();
        channel.attr(ChannelAttributes.COMMAND_FUTURES).get().put(command.correlationId(), future);
        channel.writeAndFlush(command).addListener(writeFuture -> {
            if (!writeFuture.isSuccess()) {
                channel.attr(ChannelAttributes.COMMAND_FUTURES).get().remove(command.correlationId());
                logger.info("Error sending command", writeFuture.cause());
                future.completeExceptionally(writeFuture.cause());
            }

        });
        return future;
    }

    public static CompletableFuture<Command> sendForReply(Channel channel, Command command, Duration within) {
        return AsyncUtils.within(sendForReply(channel, command), within);
    }

    public static void ack(Channel channel, Command command) {
        send(channel, Acknowledgement.replyTo(command));
    }
}
