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

package io.hydramq;

import java.nio.charset.Charset;
import java.util.Set;

import io.hydramq.MessageProperties.PropertyKey;

/**
 * @author jfulton
 */
public class Message {

    private final MessageProperties properties;
    private byte[] body;

    public Message(final byte[] body, MessageProperties properties) {
        this.properties = properties;
        this.body = body;
    }

    public Message(final byte[] body) {
        this(body, new MessageProperties());
    }

    public byte[] body() {
        return body;
    }

    public String bodyAsString() {
        return bodyAsString(Charset.forName("UTF8"));
    }

    public String bodyAsString(Charset charset) {
        return new String(body(), charset);
    }

    public MessageProperties properties() {
        return properties;
    }

    public static Builder empty() {
        return new Builder();
    }

    public static Builder withBody(byte[] body) {
        Builder builder = new Builder();
        return builder.withBody(body);
    }

    public static Builder withBodyAsString(String body) {
        Builder builder = new Builder();
        return builder.withBodyAsString(body);
    }

    public static Builder withBodyAsString(String body, Charset charset) {
        Builder builder = new Builder();
        return builder.withBodyAsString(body, charset);
    }


    public boolean getBoolean(String key) {
        return properties.getBoolean(key);
    }

    public boolean getBoolean(PropertyKey key) {
        return properties.getBoolean(key);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        return properties.getBoolean(key, defaultValue);
    }

    public boolean getBoolean(PropertyKey key, boolean defaultValue) {
        return properties.getBoolean(key, defaultValue);
    }

    public Message setBoolean(String key, boolean value) {
        properties().setBoolean(key, value);
        return this;
    }

    public Message setBoolean(PropertyKey key, boolean value) {
        properties().setBoolean(key, value);
        return this;
    }

    public Set<String> getBooleanKeys() {
        return properties.getBooleanKeys();
    }

    public boolean hasBoolean(String key) {
        return properties.hasBoolean(key);
    }

    public boolean hasBoolean(PropertyKey key) {
        return properties.hasBoolean(key);
    }

    public void removeBoolean(String key) {
        properties.removeBoolean(key);
    }

    public void removeBoolean(PropertyKey key) {
        properties.removeBoolean(key);
    }

    public byte getByte(String key) {
        return properties.getByte(key);
    }

    public byte getByte(PropertyKey key) {
        return properties.getByte(key);
    }

    public byte getByte(String key, byte defaultValue) {
        return properties.getByte(key, defaultValue);
    }

    public byte getByte(PropertyKey key, byte defaultValue) {
        return properties.getByte(key, defaultValue);
    }

    public Message setByte(String key, byte value) {
        properties().setByte(key, value);
        return this;
    }

    public Message setByte(PropertyKey key, byte value) {
        properties().setByte(key, value);
        return this;
    }

    public Set<String> getByteKeys() {
        return properties.getByteKeys();
    }

    public boolean hasByte(String key) {
        return properties.hasByte(key);
    }

    public boolean hasByte(PropertyKey key) {
        return properties.hasByte(key);
    }

    public void removeByte(String key) {
        properties.removeByte(key);
    }

    public void removeByte(PropertyKey key) {
        properties.removeByte(key);
    }

    public byte[] getBytes(String key) {
        return properties.getBytes(key);
    }

    public byte[] getBytes(PropertyKey key) {
        return properties.getBytes(key);
    }

    public byte[] getBytes(String key, byte[] defaultValue) {
        return properties.getBytes(key, defaultValue);
    }

    public byte[] getBytes(PropertyKey key, byte[] defaultValue) {
        return properties.getBytes(key, defaultValue);
    }

    public Message setBytes(String key, byte[] value) {
        properties().setBytes(key, value);
        return this;
    }

    public Message setBytes(PropertyKey key, byte[] value) {
        properties().setBytes(key, value);
        return this;
    }

    public Set<String> getBytesKeys() {
        return properties.getBytesKeys();
    }

    public boolean hasBytes(String key) {
        return properties.hasBytes(key);
    }

    public boolean hasBytes(PropertyKey key) {
        return properties.hasBytes(key);
    }

    public void removeBytes(String key) {
        properties.removeBytes(key);
    }

    public void removeBytes(PropertyKey key) {
        properties.removeBytes(key);
    }

    public double getDouble(String key) {
        return properties.getDouble(key);
    }

    public double getDouble(PropertyKey key) {
        return properties.getDouble(key);
    }

    public double getDouble(String key, double defaultValue) {
        return properties.getDouble(key, defaultValue);
    }

    public double getDouble(PropertyKey key, double defaultValue) {
        return properties.getDouble(key, defaultValue);
    }

    public Message setDouble(String key, double value) {
        properties().setDouble(key, value);
        return this;
    }

    public Message setDouble(PropertyKey key, double value) {
        properties().setDouble(key, value);
        return this;
    }

    public Set<String> getDoubleKeys() {
        return properties.getDoubleKeys();
    }

    public boolean hasDouble(String key) {
        return properties.hasDouble(key);
    }

    public boolean hasDouble(PropertyKey key) {
        return properties.hasDouble(key);
    }

    public void removeDouble(String key) {
        properties.removeDouble(key);
    }

    public void removeDouble(PropertyKey key) {
        properties.removeDouble(key);
    }

    public float getFloat(String key) {
        return properties.getFloat(key);
    }

    public float getFloat(PropertyKey key) {
        return properties.getFloat(key);
    }

    public float getFloat(String key, float drefaultValue) {
        return properties.getFloat(key, drefaultValue);
    }

    public float getFloat(PropertyKey key, float drefaultValue) {
        return properties.getFloat(key, drefaultValue);
    }

    public Message setFloat(String key, float value) {
        properties().setFloat(key, value);
        return this;
    }

    public Message setFloat(PropertyKey key, float value) {
        properties().setFloat(key, value);
        return this;
    }

    public Set<String> getFloatKeys() {
        return properties.getFloatKeys();
    }

    public boolean hasFloat(String key) {
        return properties.hasFloat(key);
    }

    public boolean hasFloat(PropertyKey key) {
        return properties.hasFloat(key);
    }

    public void removeFloat(String key) {
        properties.removeFloat(key);
    }

    public void removeFloat(PropertyKey key) {
        properties.removeFloat(key);
    }

    public short getShort(String key) {
        return properties.getShort(key);
    }

    public short getShort(PropertyKey key) {
        return properties.getShort(key);
    }

    public short getShort(String key, short defaultValue) {
        return properties.getShort(key, defaultValue);
    }

    public short getShort(PropertyKey key, short defaultValue) {
        return properties.getShort(key, defaultValue);
    }

    public Message setShort(String key, short value) {
        properties().setShort(key, value);
        return this;
    }

    public Message setShort(PropertyKey key, short value) {
        properties().setShort(key, value);
        return this;
    }

    public Set<String> getShortKeys() {
        return properties.getShortKeys();
    }

    public boolean hasShort(String key) {
        return properties.hasShort(key);
    }

    public boolean hasShort(PropertyKey key) {
        return properties.hasShort(key);
    }

    public void removeShort(String key) {
        properties.removeShort(key);
    }

    public void removeShort(PropertyKey key) {
        properties.removeShort(key);
    }

    public int getInteger(String key) {
        return properties.getInteger(key);
    }

    public int getInteger(PropertyKey key) {
        return properties.getInteger(key);
    }

    public int getInteger(String key, int defaultValue) {
        return properties.getInteger(key, defaultValue);
    }

    public int getInteger(PropertyKey key, int defaultValue) {
        return properties.getInteger(key, defaultValue);
    }

    public Message setInteger(String key, int value) {
        properties().setInteger(key, value);
        return this;
    }

    public Message setInteger(PropertyKey key, int value) {
        properties().setInteger(key, value);
        return this;
    }

    public Set<String> getIntegerKeys() {
        return properties.getIntegerKeys();
    }

    public boolean hasInteger(String key) {
        return properties.hasInteger(key);
    }

    public boolean hasInteger(PropertyKey key) {
        return properties.hasInteger(key);
    }

    public void removeInteger(String key) {
        properties.removeInteger(key);
    }

    public void removeInteger(PropertyKey key) {
        properties.removeInteger(key);
    }

    public long getLong(String key) {
        return properties.getLong(key);
    }

    public long getLong(PropertyKey key) {
        return properties.getLong(key);
    }

    public long getLong(String key, long defaultValue) {
        return properties.getLong(key, defaultValue);
    }

    public long getLong(PropertyKey key, long defaultValue) {
        return properties.getLong(key, defaultValue);
    }

    public Message setLong(String key, long value) {
        properties().setLong(key, value);
        return this;
    }

    public Message setLong(PropertyKey key, long value) {
        properties().setLong(key, value);
        return this;
    }

    public Set<String> getLongKeys() {
        return properties.getLongKeys();
    }

    public boolean hasLong(String key) {
        return properties.hasLong(key);
    }

    public boolean hasLong(PropertyKey key) {
        return properties.hasLong(key);
    }

    public void removeLong(String key) {
        properties.removeLong(key);
    }

    public void removeLong(PropertyKey key) {
        properties.removeLong(key);
    }

    public String getString(String key) {
        return properties.getString(key);
    }

    public String getString(PropertyKey key) {
        return properties.getString(key);
    }

    public String getString(String key, String defaultValue) {
        return properties.getString(key, defaultValue);
    }

    public String getString(PropertyKey key, String defaultValue) {
        return properties.getString(key, defaultValue);
    }

    public Message setString(String key, String value) {
        properties().setString(key, value);
        return this;
    }

    public Message setString(PropertyKey key, String value) {
        properties().setString(key, value);
        return this;
    }

    public Set<String> getStringKeys() {
        return properties.getStringKeys();
    }

    public boolean hasString(String key) {
        return properties.hasString(key);
    }

    public boolean hasString(PropertyKey key) {
        return properties.hasString(key);
    }

    public void removeString(String key) {
        properties.removeString(key);
    }

    public void removeString(PropertyKey key) {
        properties.removeString(key);
    }

    public static class Builder {

        private MessageProperties properties = new MessageProperties();
        private byte[] body;

        public Builder withBodyAsString(String body) {
            return withBodyAsString(body, Charset.forName("UTF-8"));
        }

        public Builder withBodyAsString(String body, Charset charset) {
            this.body = body.getBytes(charset);
            return this;
        }

        public Builder withBody(byte[] body) {
            this.body = body;
            return this;
        }

        public Builder withProperties(MessageProperties properties) {
            this.properties = properties;
            return this;
        }

        public Builder withString(String key, String value) {
            properties.setString(key, value);
            return this;
        }

        public Builder withString(PropertyKey key, String value) {
            properties.setString(key, value);
            return this;
        }

        public Builder withLong(String key, long value) {
            properties.setLong(key, value);
            return this;
        }

        public Builder withLong(PropertyKey key, long value) {
            properties.setLong(key, value);
            return this;
        }

        public Builder withInteger(String key, int value) {
            properties.setInteger(key, value);
            return this;
        }

        public Builder withInteger(PropertyKey key, int value) {
            properties.setInteger(key, value);
            return this;
        }
        
        public Builder withShort(String key, short value) {
            properties.setShort(key, value);
            return this;
        }

        public Builder withShort(PropertyKey key, short value) {
            properties.setShort(key, value);
            return this;
        }

        public Builder withBoolean(String key, boolean value) {
            properties.setBoolean(key, value);
            return this;
        }

        public Builder withBoolean(PropertyKey key, boolean value) {
            properties.setBoolean(key, value);
            return this;
        }

        public Builder withBytes(String key, byte[] value) {
            properties.setBytes(key, value);
            return this;
        }

        public Builder withBytes(PropertyKey key, byte[] value) {
            properties.setBytes(key, value);
            return this;
        }

        public Builder withByte(String key, byte value) {
            properties.setByte(key, value);
            return this;
        }

        public Builder withByte(PropertyKey key, byte value) {
            properties.setByte(key, value);
            return this;
        }

        public Builder withDouble(String key, double value) {
            properties.setDouble(key, value);
            return this;
        }

        public Builder withDouble(PropertyKey key, double value) {
            properties.setDouble(key, value);
            return this;
        }

        public Builder withFloat(String key, float value) {
            properties.setFloat(key, value);
            return this;
        }

        public Builder withFloat(PropertyKey key, float value) {
            properties.setFloat(key, value);
            return this;
        }

        public Message build() {
            return new Message(body != null ? body : new byte[]{}, properties);
        }
    }
}
