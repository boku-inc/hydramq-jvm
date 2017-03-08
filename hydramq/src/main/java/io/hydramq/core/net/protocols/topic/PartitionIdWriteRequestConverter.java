package io.hydramq.core.net.protocols.topic;

import io.hydramq.Message;
import io.hydramq.PartitionId;
import io.hydramq.core.type.CommandTypeConverter;
import io.hydramq.core.type.ConversionContext;
import io.netty.buffer.ByteBuf;

/**
 * @author jfulton
 */
public class PartitionIdWriteRequestConverter extends CommandTypeConverter<PartitionIdWriteRequest> {

    public PartitionIdWriteRequestConverter() {
        super(102);
    }

    @Override
    protected PartitionIdWriteRequest readObject(ConversionContext context, int correlationId, ByteBuf buffer) {
        return new PartitionIdWriteRequest(correlationId, context.read(PartitionId.class, buffer), context.read(Message.class,buffer));
    }

    @Override
    protected void writeObject(ConversionContext context, PartitionIdWriteRequest request, ByteBuf buffer) {
        context.write(PartitionId.class, request.getPartitionId(), buffer);
        context.write(Message.class, request.getMessage(), buffer);
    }
}
