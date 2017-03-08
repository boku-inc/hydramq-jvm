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

package io.hydramq;

import java.net.InetSocketAddress;

import io.hydramq.client.ReplyCoordinator;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
public class ReplyMain {

    private static final Logger logger = getLogger(ReplyMain.class);

    public static void main(String[] args) {
        TopicManager topicManager = TopicManagers.network(new InetSocketAddress("localhost", 7070));
        ReplyCoordinator replyCoordinator = new ReplyCoordinator(topicManager, "Requests");
        replyCoordinator.start(message -> {
            logger.info("Message received!");
            if (message.hasInteger("delay")) {
                int delay = message.getInteger("delay");
                try {
                    if (delay > 0) {
                        logger.info("Sleeping for {}ms", delay);
                        Thread.sleep(message.getInteger("delay"));
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return Message.withBodyAsString("Ack - " + message.bodyAsString()).withLong("created", message.getLong("created")).build();
        });
    }
}
