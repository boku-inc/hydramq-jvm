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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.hydramq.exceptions.HydraRuntimeException;

/**
 * @author jfulton
 */
public class MessageProperties {

    private Map<Class<?>, Map<String, Object>> propertyMaps = new HashMap<>();

    // Get values with defaults

    public String getString(String key, String defaultValue) {
        return getValue(String.class, key, defaultValue);
    }

    public String getString(PropertyKey key, String defaultValue) {
        return getValue(String.class, key.toString(), defaultValue);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        return getValue(Boolean.class, key, defaultValue);
    }

    public boolean getBoolean(PropertyKey key, boolean defaultValue) {
        return getValue(Boolean.class, key.toString(), defaultValue);
    }

    public short getShort(String key, short defaultValue) {
        return getValue(Short.class, key, defaultValue);
    }

    public short getShort(PropertyKey key, short defaultValue) {
        return getValue(Short.class, key.toString(), defaultValue);
    }

    public int getInteger(String key, int defaultValue) {
        return getValue(Integer.class, key, defaultValue);
    }

    public int getInteger(PropertyKey key, int defaultValue) {
        return getValue(Integer.class, key.toString(), defaultValue);
    }

    public long getLong(String key, long defaultValue) {
        return getValue(Long.class, key, defaultValue);
    }

    public long getLong(PropertyKey key, long defaultValue) {
        return getValue(Long.class, key.toString(), defaultValue);
    }

    public float getFloat(String key, float defaultValue) {
        return getValue(Float.class, key, defaultValue);
    }

    public float getFloat(PropertyKey key, float defaultValue) {
        return getValue(Float.class, key.toString(), defaultValue);
    }

    public double getDouble(String key, double defaultValue) {
        return getValue(Double.class, key, defaultValue);
    }

    public double getDouble(PropertyKey key, double defaultValue) {
        return getValue(Double.class, key.toString(), defaultValue);
    }

    public byte[] getBytes(String key, byte[] defaultValue) {
        return getValue(byte[].class, key, defaultValue);
    }

    public byte[] getBytes(PropertyKey key, byte[] defaultValue) {
        return getValue(byte[].class, key.toString(), defaultValue);
    }

    public byte getByte(String key, byte defaultValue) {
        return getValue(Byte.class, key, defaultValue);
    }

    public byte getByte(PropertyKey key, byte defaultValue) {
        return getValue(Byte.class, key.toString(), defaultValue);
    }

    // Get values, exception if not exists

    public String getString(String key) {
        return getValue(String.class, key);
    }

    public String getString(PropertyKey key) {
        return getValue(String.class, key.toString());
    }

    public boolean getBoolean(String key) {
        return getValue(Boolean.class, key);
    }

    public boolean getBoolean(PropertyKey key) {
        return getValue(Boolean.class, key.toString());
    }

    public short getShort(String key) {
        return getValue(Short.class, key);
    }

    public short getShort(PropertyKey key) {
        return getValue(Short.class, key.toString());
    }

    public int getInteger(String key) {
        return getValue(Integer.class, key);
    }

    public int getInteger(PropertyKey key) {
        return getValue(Integer.class, key.toString());
    }

    public long getLong(String key) {
        return getValue(Long.class, key);
    }

    public long getLong(PropertyKey key) {
        return getValue(Long.class, key.toString());
    }

    public float getFloat(String key) {
        return getValue(Float.class, key);
    }

    public float getFloat(PropertyKey key) {
        return getValue(Float.class, key.toString());
    }

    public double getDouble(String key) {
        return getValue(Double.class, key);
    }

    public double getDouble(PropertyKey key) {
        return getValue(Double.class, key.toString());
    }

    public byte[] getBytes(String key) {
        return getValue(byte[].class, key);
    }

    public byte[] getBytes(PropertyKey key) {
        return getValue(byte[].class, key.toString());
    }

    public byte getByte(String key) {
        return getValue(Byte.class, key);
    }

    public byte getByte(PropertyKey key) {
        return getValue(Byte.class, key.toString());
    }

    // Set values

    public MessageProperties setString(String key, String value) {
        setValue(String.class, key, value);
        return this;
    }

    public MessageProperties setString(PropertyKey key, String value) {
        setValue(String.class, key.toString(), value);
        return this;
    }

    public MessageProperties setBoolean(String key, boolean value) {
        setValue(Boolean.class, key, value);
        return this;
    }

    public MessageProperties setBoolean(PropertyKey key, boolean value) {
        setValue(Boolean.class, key.toString(), value);
        return this;
    }

    public MessageProperties setShort(String key, short value) {
        setValue(Short.class, key, value);
        return this;
    }

    public MessageProperties setShort(PropertyKey key, short value) {
        setValue(Short.class, key.toString(), value);
        return this;
    }


    public MessageProperties setInteger(String key, int value) {
        setValue(Integer.class, key, value);
        return this;
    }

    public MessageProperties setInteger(PropertyKey key, int value) {
        setValue(Integer.class, key.toString(), value);
        return this;
    }

    public MessageProperties setLong(String key, long value) {
        setValue(Long.class, key, value);
        return this;
    }

    public MessageProperties setLong(PropertyKey key, long value) {
        setValue(Long.class, key.toString(), value);
        return this;
    }

    public MessageProperties setFloat(String key, float value) {
        setValue(Float.class, key, value);
        return this;
    }

    public MessageProperties setFloat(PropertyKey key, float value) {
        setValue(Float.class, key.toString(), value);
        return this;
    }

    public MessageProperties setDouble(String key, double value) {
        setValue(Double.class, key, value);
        return this;
    }

    public MessageProperties setDouble(PropertyKey key, double value) {
        setValue(Double.class, key.toString(), value);
        return this;
    }

    public MessageProperties setBytes(String key, byte[] bytes) {
        setValue(byte[].class, key, bytes);
        return this;
    }

    public MessageProperties setBytes(PropertyKey key, byte[] bytes) {
        setValue(byte[].class, key.toString(), bytes);
        return this;
    }

    public MessageProperties setByte(String key, byte b) {
        setValue(Byte.class, key, b);
        return this;
    }

    public MessageProperties setByte(PropertyKey key, byte b) {
        setValue(Byte.class, key.toString(), b);
        return this;
    }

    // Get keys

    public Set<String> getStringKeys() {
        return getKeys(String.class);
    }

    public Set<String> getBooleanKeys() {
        return getKeys(Boolean.class);
    }

    public Set<String> getShortKeys() { return getKeys(Short.class); }

    public Set<String> getIntegerKeys() {
        return getKeys(Integer.class);
    }

    public Set<String> getLongKeys() {
        return getKeys(Long.class);
    }

    public Set<String> getFloatKeys() {
        return getKeys(Float.class);
    }

    public Set<String> getDoubleKeys() {
        return getKeys(Double.class);
    }

    public Set<String> getBytesKeys() {
        return getKeys(byte[].class);
    }

    public Set<String> getByteKeys() { return getKeys(Byte.class); }

    // Has value?

    public boolean hasString(String key) {
        return hasValue(String.class, key);
    }

    public boolean hasString(PropertyKey key) {
        return hasValue(String.class, key.toString());
    }

    public boolean hasBoolean(String key) {
        return hasValue(Boolean.class, key);
    }

    public boolean hasBoolean(PropertyKey key) {
        return hasValue(Boolean.class, key.toString());
    }

    public boolean hasShort(String key) { return hasValue(Short.class, key); }

    public boolean hasShort(PropertyKey key) { return hasValue(Short.class, key.toString()); }

    public boolean hasInteger(String key) {
        return hasValue(Integer.class, key);
    }

    public boolean hasInteger(PropertyKey key) {
        return hasValue(Integer.class, key.toString());
    }

    public boolean hasLong(String key) {
        return hasValue(Long.class, key);
    }

    public boolean hasLong(PropertyKey key) {
        return hasValue(Long.class, key.toString());
    }

    public boolean hasFloat(String key) {
        return hasValue(Float.class, key);
    }

    public boolean hasFloat(PropertyKey key) {
        return hasValue(Float.class, key.toString());
    }

    public boolean hasDouble(String key) {
        return hasValue(Double.class, key);
    }

    public boolean hasDouble(PropertyKey key) {
        return hasValue(Double.class, key.toString());
    }

    public boolean hasBytes(String key) {
        return hasValue(byte[].class, key);
    }

    public boolean hasBytes(PropertyKey key) {
        return hasValue(byte[].class, key.toString());
    }

    public boolean hasByte(String key) { return hasValue(Byte.class, key); }

    public boolean hasByte(PropertyKey key) { return hasValue(Byte.class, key.toString()); }


    // Remove value

    public MessageProperties removeString(String key) {
        removeValue(String.class, key);
        return this;
    }

    public MessageProperties removeString(PropertyKey key) {
        removeValue(String.class, key.toString());
        return this;
    }

    public MessageProperties removeBoolean(String key) {
        removeValue(Boolean.class, key);
        return this;
    }

    public MessageProperties removeBoolean(PropertyKey key) {
        removeValue(Boolean.class, key.toString());
        return this;
    }

    public MessageProperties removeShort(String key) {
        removeValue(Short.class, key);
        return this;
    }

    public MessageProperties removeShort(PropertyKey key) {
        removeValue(Short.class, key.toString());
        return this;
    }

    public MessageProperties removeInteger(String key) {
        removeValue(Integer.class, key);
        return this;
    }

    public MessageProperties removeInteger(PropertyKey key) {
        removeValue(Integer.class, key.toString());
        return this;
    }

    public MessageProperties removeLong(String key) {
        removeValue(Long.class, key);
        return this;
    }

    public MessageProperties removeLong(PropertyKey key) {
        removeValue(Long.class, key.toString());
        return this;
    }

    public MessageProperties removeFloat(String key) {
        removeValue(Float.class, key);
        return this;
    }

    public MessageProperties removeFloat(PropertyKey key) {
        removeValue(Float.class, key.toString());
        return this;
    }

    public MessageProperties removeDouble(String key) {
        removeValue(Double.class, key);
        return this;
    }

    public MessageProperties removeDouble(PropertyKey key) {
        removeValue(Double.class, key.toString());
        return this;
    }

    public MessageProperties removeBytes(String key) {
        removeValue(byte[].class, key);
        return this;
    }

    public MessageProperties removeBytes(PropertyKey key) {
        removeValue(byte[].class, key.toString());
        return this;
    }

    public MessageProperties removeByte(String key) {
        removeValue(Byte.class, key);
        return this;
    }

    public MessageProperties removeByte(PropertyKey key) {
        removeValue(Byte.class, key.toString());
        return this;
    }

    // Generic utilities

    private <T> Set<String> getKeys(Class<T> type) {
        if (propertyMaps.containsKey(type)) {
            return propertyMaps.get(type).keySet();
        } else {
            return Collections.emptySet();
        }
    }

    private <T> boolean hasValue(Class<T> type, String key) {
        return propertyMaps.containsKey(type) && propertyMaps.get(type).containsKey(key);
    }

    private <T> T getValue(Class<T> type, String key) {
        if (propertyMaps.containsKey(type) && propertyMaps.get(type).containsKey(key)) {
            return (T) propertyMaps.get(type).get(key);
        }
        throw new HydraRuntimeException("Missing " + type.getSimpleName() + " property with key '" + key + "'");
    }

    private <T> T getValue(Class<T> type, String key, T defaultValue) {
        if (hasValue(type, key)) {
            return getValue(type, key);
        } else {
            return defaultValue;
        }
    }

    private <T> void setValue(Class<T> type, String key, T value) {
        propertyMaps.computeIfAbsent(type, aClass -> new HashMap<>()).put(key, value);
    }

    private <T> void removeType(Class<T> type) {
        propertyMaps.remove(type);
    }

    private <T> void removeValue(Class<T> type, String key) {
        if (hasValue(type, key)) {
            propertyMaps.get(type).remove(key);
        }
    }

    /**
     * A marker interface for MessageProperty keys.  toString() is used, so implementors may use any object as a key.
     * Particularly useful is to use enums that inherit from this interface, and override the toString().
     *
     * @author jfulton
     */
    public interface PropertyKey {
    }
}
