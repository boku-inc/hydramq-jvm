package io.hydramq.core.net.protocols.topic;

import io.hydramq.Message;
import io.hydramq.PartitionId;
import io.hydramq.core.net.Request;

/**
 * @author jfulton
 */
public class PartitionIdWriteRequest extends Request {

    private final PartitionId partitionId;
    private final Message message;

    public PartitionIdWriteRequest(final PartitionId partitionId, final Message message) {
        this.partitionId = partitionId;
        this.message = message;
    }

    public PartitionIdWriteRequest(final int correlationId, final PartitionId partitionId, final Message message) {
        super(correlationId);
        this.partitionId = partitionId;
        this.message = message;
    }

    public PartitionId getPartitionId() {
        return partitionId;
    }

    public Message getMessage() {
        return message;
    }
}
