package io.hydramq.core.net.protocols.topic;

import io.hydramq.PartitionId;
import io.hydramq.core.type.CommandTypeConverter;
import io.hydramq.core.type.ConversionContext;
import io.hydramq.listeners.PartitionFlags;
import io.netty.buffer.ByteBuf;

/**
 * @author jfulton
 */
public class PartitionsDiscoveredNotificationConverter extends CommandTypeConverter<PartitionsDiscoveredNotification> {

    public PartitionsDiscoveredNotificationConverter() {
        super(205);
    }

    @Override
    protected PartitionsDiscoveredNotification readObject(ConversionContext context, int correlationId, ByteBuf buffer) {
        return new PartitionsDiscoveredNotification(correlationId, context.read(PartitionId.class, buffer), PartitionFlags.fromInt(buffer.readInt()));
    }

    @Override
    protected void writeObject(ConversionContext context, PartitionsDiscoveredNotification notification, ByteBuf buffer) {
        context.write(PartitionId.class, notification.getPartitionId(), buffer);
        buffer.writeInt(notification.getFlags().toInt());
    }
}
