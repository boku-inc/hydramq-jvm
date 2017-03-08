package io.hydramq.core.net.protocols.topicmanager;

import io.hydramq.core.type.CommandTypeConverter;
import io.hydramq.core.type.ConversionContext;
import io.netty.buffer.ByteBuf;

/**
 * @author jfulton
 */
public class TopicDiscoveredNotificationConverter extends CommandTypeConverter<TopicDiscoveredNotification> {

    public TopicDiscoveredNotificationConverter() {
        super(501);
    }

    @Override
    protected TopicDiscoveredNotification readObject(ConversionContext context, int correlationId, ByteBuf buffer) {
        return new TopicDiscoveredNotification(correlationId, context.read(String.class, buffer));
    }

    @Override
    protected void writeObject(ConversionContext context, TopicDiscoveredNotification notification, ByteBuf buffer) {
        context.write(String.class, notification.getTopicName(), buffer);
    }
}
