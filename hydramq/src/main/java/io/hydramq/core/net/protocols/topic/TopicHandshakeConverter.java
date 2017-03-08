package io.hydramq.core.net.protocols.topic;

import java.util.HashMap;
import java.util.Map;

import io.hydramq.PartitionId;
import io.hydramq.core.type.CommandTypeConverter;
import io.hydramq.core.type.ConversionContext;
import io.hydramq.listeners.PartitionFlags;
import io.netty.buffer.ByteBuf;

/**
 * @author jfulton
 */
public class TopicHandshakeConverter extends CommandTypeConverter<TopicHandshake> {

    public TopicHandshakeConverter() {
        super(100);
    }

    @Override
    protected TopicHandshake readObject(final ConversionContext context, final int correlationId, final ByteBuf buffer) {
        int version = buffer.readInt();
        String topicName = context.read(String.class, buffer);
        int partitionCount = buffer.readInt();
        Map<PartitionId, PartitionFlags> partitions = new HashMap<>();
        for (int i = 0; i < partitionCount; i++) {
            partitions.put(context.read(PartitionId.class, buffer), PartitionFlags.fromInt(buffer.readInt()));
        }
        return new TopicHandshake(correlationId, version, topicName, partitions);
    }

    @Override
    protected void writeObject(final ConversionContext context, final TopicHandshake producerHandshake,
            final ByteBuf buffer) {
        buffer.writeInt(producerHandshake.getVersion());
        context.write(String.class, producerHandshake.getTopicName(), buffer);
        buffer.writeInt(producerHandshake.getPartitionCount());
        producerHandshake.getPartitions().forEach((partitionId, flags) -> {
            context.write(PartitionId.class, partitionId, buffer);
            buffer.writeInt(flags.toInt());
        });
    }
}
