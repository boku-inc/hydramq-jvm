package io.hydramq.core.net.protocols.topic;

import java.util.UUID;

import io.hydramq.PartitionId;
import io.hydramq.core.net.Request;

/**
 * @author jfulton
 */
public class LockListenerNotification extends Request {

    private final UUID subscriptionKey;
    private final boolean leasing;
    private final PartitionId partitionId;

    public LockListenerNotification(int correlationId, UUID subscriptionKey, boolean leasing, PartitionId partitionId) {
        super(correlationId);
        this.subscriptionKey = subscriptionKey;
        this.leasing = leasing;
        this.partitionId = partitionId;
    }

    public LockListenerNotification(UUID subscriptionKey, boolean leasing, PartitionId partitionId) {
        this.subscriptionKey = subscriptionKey;
        this.leasing = leasing;
        this.partitionId = partitionId;
    }

    public UUID getSubscriptionKey() {
        return subscriptionKey;
    }

    public boolean isLocked() {
        return leasing;
    }

    public PartitionId getPartitionId() {
        return partitionId;
    }
}

