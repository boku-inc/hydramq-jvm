package io.hydramq.core.net.protocols.topic;

import io.hydramq.PartitionId;
import io.hydramq.core.net.Request;

/**
 * @author jfulton
 */
public class PartitionIdReadRequest extends Request {

    private final long messageOffset;
    private final int maxMessages;
    private final PartitionId partitionId;

    public PartitionIdReadRequest(PartitionId partitionId, long messageOffset, int maxMessages) {
        this.messageOffset = messageOffset;
        this.maxMessages = maxMessages;
        this.partitionId = partitionId;
    }

    public PartitionIdReadRequest(int correlationId, PartitionId partitionId, long messageOffset, int maxMessages) {
        super(correlationId);
        this.messageOffset = messageOffset;
        this.maxMessages = maxMessages;
        this.partitionId = partitionId;
    }

    public long getMessageOffset() {
        return messageOffset;
    }

    public int getMaxMessages() {
        return maxMessages;
    }

    public PartitionId getPartitionId() {
        return partitionId;
    }
}
