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

import java.util.Set;

import io.hydramq.client.RetryProperties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */

// TODO: handle null keys
@SuppressWarnings("Duplicates")
public class MessagePropertiesTest {

    private MessageProperties properties;

    @BeforeMethod
    public void setup() {
        properties = new MessageProperties();
    }

    @Test
    public void testSetAndGetString() throws Exception {
        assertThat(properties.getString("Key", "foo"), is("foo"));
        properties.setString("key", "value");
        assertThat(properties.getString("key"), is("value"));
        assertThat(properties.getString("key", "foo"), is("value"));

        assertThat(properties.getString(RetryProperties.EXPIRES, "never"), is("never"));
        properties.setString(RetryProperties.EXPIRES, "tomorrow");
        assertThat(properties.getString(RetryProperties.EXPIRES), is("tomorrow"));
        properties.removeString(RetryProperties.EXPIRES);
        assertThat(properties.hasString(RetryProperties.EXPIRES), is(false));
    }

    @Test
    public void testSetAndGetBoolean() throws Exception {
        assertThat(properties.getBoolean("true", false), is(false));
        assertThat(properties.getBoolean("false", true), is(true));
        properties.setBoolean("true", true);
        properties.setBoolean("false", false);
        assertThat(properties.getBoolean("true"), is(true));
        assertThat(properties.getBoolean("true", false), is(true));
        assertThat(properties.getBoolean("false"), is(false));
        assertThat(properties.getBoolean("false", true), is(false));
    }

    @Test
    public void testSetInteger() throws Exception {
        properties.setInteger("key", 56);
        assertThat(properties.getInteger("key"), is(56));
        assertThat(properties.getInteger("key", 100), is(56));
    }

    @Test
    public void testFloat() throws Exception {
        MessageProperties properties = new MessageProperties();
        properties.setFloat("key", (float) 43.5);
        assertThat(properties.getFloat("key"), is((float)43.5));
    }

    @Test
    public void testRemove() throws Exception {
        MessageProperties properties = new MessageProperties()
                .setString("key", "value")
                .setBoolean("key", true)
                .setInteger("key", 1)
                .setLong("key", 1L)
                .setFloat("key", 1.0f)
                .setDouble("key", 1.0)
                .setBytes("key", new byte[0])
                .setShort("key", (short)5)
                .setByte("key", (byte)1);

        properties
                .removeString("key")
                .removeBoolean("key")
                .removeInteger("key")
                .removeLong("key")
                .removeFloat("key")
                .removeDouble("key")
                .removeBytes("key")
                .removeShort("key")
                .removeByte("key");
        assertKeysSize(properties, 0);
    }

    public static void assertEquivalent(MessageProperties input, MessageProperties output) {
        assertKeysSize(output.getStringKeys(), input.getStringKeys().size());
        for (String key : input.getStringKeys()) {
            assertThat(input.getString(key), is(output.getString(key)));
        }
        assertKeysSize(output.getBooleanKeys(), input.getBooleanKeys().size());
        for (String key : input.getBooleanKeys()) {
            assertThat(input.getBoolean(key), is(output.getBoolean(key)));
        }
        assertKeysSize(output.getIntegerKeys(), input.getIntegerKeys().size());
        for (String key : input.getIntegerKeys()) {
            assertThat(input.getInteger(key), is(output.getInteger(key)));
        }
        assertKeysSize(output.getLongKeys(), input.getLongKeys().size());
        for (String key : input.getLongKeys()) {
            assertThat(input.getLong(key), is(output.getLong(key)));
        }
        assertKeysSize(output.getFloatKeys(), input.getFloatKeys().size());
        for (String key : input.getFloatKeys()) {
            assertThat(input.getFloat(key), is(output.getFloat(key)));
        }
        assertKeysSize(output.getDoubleKeys(), input.getDoubleKeys().size());
        for (String key : input.getDoubleKeys()) {
            assertThat(input.getDouble(key), is(output.getDouble(key)));
        }
        assertKeysSize(output.getBytesKeys(), input.getBytesKeys().size());
        for (String key : input.getBytesKeys()) {
            assertThat(input.getBytes(key), is(output.getBytes(key)));
        }
    }

    public static void assertKeysSize(MessageProperties properties, int size) {
        assertKeysSize(properties.getStringKeys(), size);
        assertKeysSize(properties.getBooleanKeys(), size);
        assertKeysSize(properties.getIntegerKeys(), size);
        assertKeysSize(properties.getLongKeys(), size);
        assertKeysSize(properties.getFloatKeys(), size);
        assertKeysSize(properties.getDoubleKeys(), size);
        assertKeysSize(properties.getBytesKeys(), size);
    }

    public static void assertKeysSize(Set<String> keys, int size) {
        assertThat(keys.size(), is(size));
    }
}