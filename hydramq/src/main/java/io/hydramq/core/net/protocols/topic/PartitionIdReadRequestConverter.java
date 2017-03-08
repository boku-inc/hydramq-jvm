package io.hydramq.core.net.protocols.topic;

import io.hydramq.PartitionId;
import io.hydramq.core.type.CommandTypeConverter;
import io.hydramq.core.type.ConversionContext;
import io.netty.buffer.ByteBuf;

/**
 * @author jfulton
 */
public class PartitionIdReadRequestConverter extends CommandTypeConverter<PartitionIdReadRequest> {

    public PartitionIdReadRequestConverter() {
        super(202);
    }

    @Override
    protected PartitionIdReadRequest readObject(ConversionContext context, int correlationId, ByteBuf buffer) {
        return new PartitionIdReadRequest(correlationId, context.read(PartitionId.class, buffer), buffer.readLong(), buffer.readInt());
    }

    @Override
    protected void writeObject(ConversionContext context, PartitionIdReadRequest request, ByteBuf buffer) {
        context.write(PartitionId.class, request.getPartitionId(), buffer);
        buffer.writeLong(request.getMessageOffset());
        buffer.writeInt(request.getMaxMessages());
    }
}
