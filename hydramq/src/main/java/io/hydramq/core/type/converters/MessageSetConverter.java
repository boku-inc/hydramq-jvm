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

import io.hydramq.Message;
import io.hydramq.MessageSet;
import io.hydramq.core.type.ConversionContext;
import io.hydramq.core.type.TypeConverter;
import io.netty.buffer.ByteBuf;

/**
 * @author jfulton
 */
public class MessageSetConverter extends TypeConverter<MessageSet> {

    @Override
    public MessageSet read(ConversionContext context, ByteBuf buffer) {
        MessageSet messageSet = new MessageSet(buffer.readLong());
        int messageCount = buffer.readInt();
        for (int i = 0; i < messageCount; i++) {
            messageSet.add(context.read(Message.class, buffer));
        }
        return messageSet;
    }

    @Override
    public void write(ConversionContext context, MessageSet messageSet, ByteBuf buffer) {
        buffer.writeLong(messageSet.startOffset());
        buffer.writeInt(messageSet.size());
        messageSet.forEach(message -> context.write(Message.class, message, buffer));
    }
}
