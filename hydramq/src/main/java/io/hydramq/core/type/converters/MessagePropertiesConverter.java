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

import io.hydramq.MessageProperties;
import io.hydramq.core.type.ConversionContext;
import io.hydramq.core.type.TypeConverter;
import io.netty.buffer.ByteBuf;

/**
 * @author jfulton
 */
public class MessagePropertiesConverter extends TypeConverter<MessageProperties> {

    private static final int HAS_STRING_PROPERTIES = 1;
    private static final int HAS_BOOLEAN_PROPERTIES = 1 << 1;
    private static final int HAS_INTEGER_PROPERTIES = 1 << 2;
    private static final int HAS_LONG_PROPERTIES = 1 << 3;
    private static final int HAS_FLOAT_PROPERTIES = 1 << 4;
    private static final int HAS_DOUBLE_PROPERTIES = 1 << 5;
    private static final int HAS_BYTES_PROPERTIES = 1 << 6;
    private static final int HAS_SHORT_PROPERTIES = 1 << 7;
    private static final int HAS_BYTE_PROPERTIES = 1 << 8;

    @Override
    public MessageProperties read(final ConversionContext context, final ByteBuf buffer) {
        MessageProperties properties = new MessageProperties();
        int flags = buffer.readInt();
        if ((flags & HAS_STRING_PROPERTIES) == HAS_STRING_PROPERTIES) {
            int count = buffer.readInt();
            for (int i = 0; i < count; i++) {
                properties.setString(context.read(String.class, buffer), context.read(String.class, buffer));
            }
        }
        if ((flags & HAS_BOOLEAN_PROPERTIES) == HAS_BOOLEAN_PROPERTIES) {
            int count = buffer.readInt();
            for (int i = 0; i < count; i++) {
                properties.setBoolean(context.read(String.class, buffer), buffer.readBoolean());
            }
        }
        if ((flags & HAS_INTEGER_PROPERTIES) == HAS_INTEGER_PROPERTIES) {
            int count = buffer.readInt();
            for (int i = 0; i < count; i++) {
                properties.setInteger(context.read(String.class, buffer), buffer.readInt());
            }
        }
        if ((flags & HAS_LONG_PROPERTIES) == HAS_LONG_PROPERTIES) {
            int count = buffer.readInt();
            for (int i = 0; i < count; i++) {
                properties.setLong(context.read(String.class, buffer), buffer.readLong());
            }
        }
        if ((flags & HAS_FLOAT_PROPERTIES) == HAS_FLOAT_PROPERTIES) {
            int count = buffer.readInt();
            for (int i = 0; i < count; i++) {
                properties.setFloat(context.read(String.class, buffer), buffer.readFloat());
            }
        }
        if ((flags & HAS_DOUBLE_PROPERTIES) == HAS_DOUBLE_PROPERTIES) {
            int count = buffer.readInt();
            for (int i = 0; i < count; i++) {
                properties.setDouble(context.read(String.class, buffer), buffer.readDouble());
            }
        }
        if ((flags & HAS_BYTES_PROPERTIES) == HAS_BYTES_PROPERTIES) {
            int count = buffer.readInt();
            for (int i = 0; i < count; i++) {
                properties.setBytes(context.read(String.class, buffer), context.read(byte[].class, buffer));
            }
        }
        if ((flags & HAS_SHORT_PROPERTIES) == HAS_SHORT_PROPERTIES) {
            int count = buffer.readInt();
            for (int i = 0; i < count; i++) {
                properties.setShort(context.read(String.class, buffer), buffer.readShort());
            }
        }
        if ((flags & HAS_BYTE_PROPERTIES) == HAS_BYTE_PROPERTIES) {
            int count = buffer.readInt();
            for (int i = 0; i < count; i++) {
                properties.setByte(context.read(String.class, buffer), buffer.readByte());
            }
        }


        return properties;
    }

    @Override
    public void write(final ConversionContext context, final MessageProperties properties, final ByteBuf buffer) {
        int flags = 0;
        if (properties.getStringKeys().size() > 0) {
            flags = flags | HAS_STRING_PROPERTIES;
        }
        if (properties.getBooleanKeys().size() > 0) {
            flags = flags | HAS_BOOLEAN_PROPERTIES;
        }
        if (properties.getIntegerKeys().size() > 0) {
            flags = flags | HAS_INTEGER_PROPERTIES;
        }
        if (properties.getLongKeys().size() > 0) {
            flags = flags | HAS_LONG_PROPERTIES;
        }
        if (properties.getFloatKeys().size() > 0) {
            flags = flags | HAS_FLOAT_PROPERTIES;
        }
        if (properties.getDoubleKeys().size() > 0) {
            flags = flags | HAS_DOUBLE_PROPERTIES;
        }
        if (properties.getBytesKeys().size() > 0) {
            flags = flags | HAS_BYTES_PROPERTIES;
        }
        if (properties.getShortKeys().size() > 0) {
            flags = flags | HAS_SHORT_PROPERTIES;
        }
        if (properties.getByteKeys().size() > 0) {
            flags = flags | HAS_BYTE_PROPERTIES;
        }
        buffer.writeInt(flags);
        if (properties.getStringKeys().size() > 0) {
            buffer.writeInt(properties.getStringKeys().size());
            for (String key : properties.getStringKeys()) {
                context.write(String.class, key, buffer);
                context.write(String.class, properties.getString(key), buffer);
            }
        }
        if (properties.getBooleanKeys().size() > 0) {
            buffer.writeInt(properties.getBooleanKeys().size());
            for (String key : properties.getBooleanKeys()) {
                context.write(String.class, key, buffer);
                buffer.writeBoolean(properties.getBoolean(key));
            }
        }
        if (properties.getIntegerKeys().size() > 0) {
            buffer.writeInt(properties.getIntegerKeys().size());
            for (String key : properties.getIntegerKeys()) {
                context.write(String.class, key, buffer);
                buffer.writeInt(properties.getInteger(key));
            }
        }
        if (properties.getLongKeys().size() > 0) {
            buffer.writeInt(properties.getLongKeys().size());
            for (String key : properties.getLongKeys()) {
                context.write(String.class, key, buffer);
                buffer.writeLong(properties.getLong(key));
            }
        }
        if (properties.getFloatKeys().size() > 0) {
            buffer.writeInt(properties.getFloatKeys().size());
            for (String key : properties.getFloatKeys()) {
                context.write(String.class, key, buffer);
                buffer.writeFloat(properties.getFloat(key));
            }
        }
        if (properties.getDoubleKeys().size() > 0) {
            buffer.writeInt(properties.getDoubleKeys().size());
            for (String key : properties.getDoubleKeys()) {
                context.write(String.class, key, buffer);
                buffer.writeDouble(properties.getDouble(key));
            }
        }
        if (properties.getBytesKeys().size() > 0) {
            buffer.writeInt(properties.getBytesKeys().size());
            for (String key : properties.getBytesKeys()) {
                context.write(String.class, key, buffer);
                context.write(byte[].class, properties.getBytes(key), buffer);
            }
        }
        if (properties.getShortKeys().size() > 0) {
            buffer.writeInt(properties.getShortKeys().size());
            for (String key : properties.getShortKeys()) {
                context.write(String.class, key, buffer);
                buffer.writeShort(properties.getShort(key));
            }
        }
        if (properties.getByteKeys().size() > 0) {
            buffer.writeInt(properties.getByteKeys().size());
            for (String key : properties.getByteKeys()) {
                context.write(String.class, key, buffer);
                buffer.writeByte(properties.getByte(key));
            }
        }
    }
}
