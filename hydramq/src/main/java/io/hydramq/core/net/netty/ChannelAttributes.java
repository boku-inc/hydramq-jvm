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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.hydramq.core.net.Command;
import io.hydramq.core.net.protocols.topic.NetworkLockListener;
import io.hydramq.listeners.DiscoverTopicsListener;
import io.hydramq.listeners.PartitionListener;
import io.netty.util.AttributeKey;

/**
 * @author jfulton
 */
public class ChannelAttributes {

    public static AttributeKey<Map<Integer, CompletableFuture<Command>>> COMMAND_FUTURES =
            AttributeKey.newInstance("correlatedFutures");

    public static AttributeKey<DiscoverTopicsListener> DISCOVER_TOPICS_LISTENER =
            AttributeKey.newInstance("discoverTopicsListener");

    public static AttributeKey<PartitionListener> DISCOVER_PARTITIONS_LISTENER =
            AttributeKey.newInstance("discoverPartitionsListener");

    public static AttributeKey<Map<UUID, NetworkLockListener>> LOCK_LISTENERS =
            AttributeKey.newInstance("subscriptions");
}
