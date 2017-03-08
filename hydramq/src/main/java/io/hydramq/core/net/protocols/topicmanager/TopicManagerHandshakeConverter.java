package io.hydramq.core.net.protocols.topicmanager;

import java.util.HashSet;
import java.util.Set;

import io.hydramq.core.type.CommandTypeConverter;
import io.hydramq.core.type.ConversionContext;
import io.netty.buffer.ByteBuf;

/**
 * @author jfulton
 */
public class TopicManagerHandshakeConverter extends CommandTypeConverter<TopicManagerHandshake> {

    public TopicManagerHandshakeConverter() {
        super(500);
    }

    @Override
    protected TopicManagerHandshake readObject(ConversionContext context, int correlationId, ByteBuf buffer) {
        int version = buffer.readInt();
        int topicsCount = buffer.readInt();
        Set<String> topicNames = new HashSet<>();
        for (int i = 0; i < topicsCount; i++) {
            topicNames.add(context.read(String.class, buffer));
        }
        return new TopicManagerHandshake(correlationId, version, topicNames);
    }

    @Override
    protected void writeObject(ConversionContext context, TopicManagerHandshake handshake, ByteBuf buffer) {
        buffer.writeInt(handshake.getVersion());
        buffer.writeInt(handshake.getTopicNames().size());
        for (String topicName : handshake.getTopicNames()) {
            context.write(String.class, topicName, buffer);
        }
    }
}
