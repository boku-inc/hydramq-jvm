package io.hydramq.core.net.protocols.topic;

import java.util.UUID;

import io.hydramq.PartitionId;
import io.hydramq.core.type.CommandTypeConverter;
import io.hydramq.core.type.ConversionContext;
import io.netty.buffer.ByteBuf;

/**
 * @author jfulton
 */
public class SubscriptionStatusNotificationConverter extends CommandTypeConverter<LockListenerNotification> {


    public SubscriptionStatusNotificationConverter() {
        super(601);
    }

    @Override
    protected LockListenerNotification readObject(ConversionContext context, int correlationId, ByteBuf buffer) {
        return new LockListenerNotification(correlationId, context.read(UUID.class, buffer), buffer.readBoolean(), context.read(PartitionId.class, buffer));
    }

    @Override
    protected void writeObject(ConversionContext context, LockListenerNotification notification, ByteBuf buffer) {
        context.write(UUID.class, notification.getSubscriptionKey(), buffer);
        buffer.writeBoolean(notification.isLocked());
        context.write(PartitionId.class, notification.getPartitionId(), buffer);
    }
}
