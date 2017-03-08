package io.hydramq.core.net.protocols.topic;

import java.util.Map;

import io.hydramq.PartitionId;
import io.hydramq.core.net.Handshake;
import io.hydramq.listeners.PartitionFlags;

/**
 * @author jfulton
 */
public class TopicHandshake extends Handshake {

    private final int version;
    private final String topicName;
    private final Map<PartitionId, PartitionFlags> partitions;

    public TopicHandshake(final int version, final String topicName, Map<PartitionId, PartitionFlags> partitions) {
        super();
        this.version = version;
        this.topicName = topicName;
        this.partitions = partitions;
    }

    public TopicHandshake(final int correlationId, final int version, final String topicName, Map<PartitionId, PartitionFlags> partitions) {
        super(correlationId);
        this.version = version;
        this.topicName = topicName;
        this.partitions = partitions;
    }

    public int getVersion() {
        return version;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getPartitionCount() {
        return partitions.size();
    }

    public Map<PartitionId, PartitionFlags> getPartitions() {
        return partitions;
    }

    public TopicHandshake reply(int version, Map<PartitionId, PartitionFlags> partitions) {
        return new TopicHandshake(correlationId(), version, topicName, partitions);
    }
}
