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
import io.hydramq.MessageProperties;
import io.hydramq.core.type.ConversionContext;
import io.hydramq.core.type.TypeConverter;
import io.netty.buffer.ByteBuf;

/**
 * @author jfulton
 */
public class MessageConverter extends TypeConverter<Message> {

    @Override
    public Message read(final ConversionContext context, final ByteBuf buffer) {
        MessageProperties properties = context.read(MessageProperties.class, buffer);
        byte[] body = context.read(byte[].class, buffer);
        return new Message(body, properties);
    }

    @Override
    public void write(final ConversionContext context, final Message message, final ByteBuf buffer) {
        context.write(MessageProperties.class, message.properties(), buffer);
        context.write(byte[].class, message.body(), buffer);
    }
}
