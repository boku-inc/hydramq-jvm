package io.hydramq.core.net.protocols.topic;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import io.hydramq.PartitionId;
import io.hydramq.core.net.netty.ChannelUtils;
import io.hydramq.subscriptions.LockListener;
import io.hydramq.subscriptions.LockState;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jfulton
 */
public class NetworkLockListener implements LockListener {

    private static final Logger logger = LoggerFactory.getLogger(NetworkLockListener.class);
    private final UUID key;
    private final String lockGroup;
    private final Channel channel;
    private AtomicInteger lockRequests = new AtomicInteger();
    private AtomicInteger lockAcks = new AtomicInteger();
    private AtomicInteger releaseRequests = new AtomicInteger();
    private AtomicInteger releaseAcks = new AtomicInteger();

    public NetworkLockListener(UUID key, String lockGroup, Channel channel) {
        this.key = key;
        this.lockGroup = lockGroup;
        this.channel = channel;
    }

    public UUID getKey() {
        return key;
    }

    public String getSubscriptionGroup() {
        return lockGroup;
    }

    public Channel getChannel() {
        return channel;
    }

    @Override
    public CompletableFuture<Void> onLockState(PartitionId partitionId, LockState state) {
        LockListenerNotification notification = new LockListenerNotification(key, state == LockState.LOCKED, partitionId);
        if (state == LockState.LOCKED) {
            lockRequests.incrementAndGet();
        } else {
            releaseRequests.incrementAndGet();
        }
        return ChannelUtils.sendForReply(getChannel(), notification).thenAccept(aVoid -> {
            logger.debug("Client acked");
            if (state == LockState.LOCKED) {
                lockAcks.incrementAndGet();
            } else {
                releaseAcks.incrementAndGet();
            }
            logger.info("Channel:{}, Lock Reqs: {}, Lock Acks: {}, Rel Reqs: {}, Rel Acks: {}", channel, lockRequests.get(), lockAcks.get(), releaseRequests.get(), releaseAcks.get());
        }).exceptionally(throwable -> {
            logger.warn("Oh fuck!", throwable);
            return null;
        });
    }

    @Override
    public int hashCode() {
        return getKey().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return !(obj instanceof NetworkLockListener) && getKey().equals(obj);
    }
}
