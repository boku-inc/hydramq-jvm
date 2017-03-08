package io.hydramq.core.net.protocols.topic;

import io.hydramq.PartitionId;
import io.hydramq.core.net.Request;
import io.hydramq.listeners.PartitionFlags;

/**
 * @author jfulton
 */
public class PartitionsDiscoveredNotification extends Request {

    private final PartitionId partitionId;
    private final PartitionFlags flags;

    public PartitionsDiscoveredNotification(PartitionId partitionId, PartitionFlags flags) {
        this.partitionId = partitionId;
        this.flags = flags;
    }

    public PartitionsDiscoveredNotification(int correlationId, PartitionId partitionId, PartitionFlags flags) {
        super(correlationId);
        this.partitionId = partitionId;
        this.flags = flags;
    }

    public PartitionId getPartitionId() {
        return partitionId;
    }

    public PartitionFlags getFlags() {
        return flags;
    }
}
