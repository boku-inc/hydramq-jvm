package io.hydramq.core.net.protocols.topic;

import io.hydramq.MessageSet;
import io.hydramq.core.type.CommandTypeConverter;
import io.hydramq.core.type.ConversionContext;
import io.netty.buffer.ByteBuf;

/**
 * @author jfulton
 */
public class ReadResponseConverter extends CommandTypeConverter<ReadResponse> {

    public ReadResponseConverter() {
        super(201);
    }

    @Override
    protected ReadResponse readObject(ConversionContext context, int correlationId, ByteBuf buffer) {
        return new ReadResponse(correlationId,context.read(MessageSet.class, buffer));
    }

    @Override
    protected void writeObject(ConversionContext context, ReadResponse response, ByteBuf buffer) {
        context.write(MessageSet.class, response.getMessageSet(), buffer);
    }
}
