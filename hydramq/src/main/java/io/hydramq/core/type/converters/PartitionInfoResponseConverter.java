/*
 * The MIT License (MIT)
 *
 * Copyright © 2016-, Boku Inc., Jimmie Fulton
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package io.hydramq.core.type.converters;

import io.hydramq.PartitionInfo;
import io.hydramq.core.net.commands.PartitionInfoResponse;
import io.hydramq.core.type.CommandTypeConverter;
import io.hydramq.core.type.ConversionContext;
import io.netty.buffer.ByteBuf;

/**
 * @author jfulton
 */
public class PartitionInfoResponseConverter extends CommandTypeConverter<PartitionInfoResponse> {

    public PartitionInfoResponseConverter() {
        super(204);
    }

    @Override
    protected PartitionInfoResponse readObject(final ConversionContext context, final int correlationId,
            final ByteBuf buffer) {
        return new PartitionInfoResponse(correlationId, context.read(PartitionInfo.class, buffer));
    }

    @Override
    protected void writeObject(final ConversionContext context, final PartitionInfoResponse instance,
            final ByteBuf buffer) {
        context.write(PartitionInfo.class, instance.getPartitionInfo(), buffer);
    }
}
