package io.hydramq.core.net.protocols.topic;

import java.util.UUID;

import io.hydramq.core.type.CommandTypeConverter;
import io.hydramq.core.type.ConversionContext;
import io.netty.buffer.ByteBuf;

/**
 * @author jfulton
 */
public class LockListenerRequestRequestConverter extends CommandTypeConverter<LockListenerRequest> {

    public LockListenerRequestRequestConverter() {
        super(600);
    }

    @Override
    protected LockListenerRequest readObject(ConversionContext context, int correlationId, ByteBuf buffer) {
        return new LockListenerRequest(correlationId, context.read(UUID.class, buffer), context.read(String.class, buffer), buffer.readBoolean());
    }

    @Override
    protected void writeObject(ConversionContext context, LockListenerRequest request, ByteBuf buffer) {
        context.write(UUID.class, request.getClientKey(), buffer);
        context.write(String.class, request.getLockGroup(), buffer);
        buffer.writeBoolean(request.isRegistering());
    }
}
