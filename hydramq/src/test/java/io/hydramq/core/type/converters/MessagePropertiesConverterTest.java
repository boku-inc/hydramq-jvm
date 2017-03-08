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

import java.util.Set;

import io.hydramq.MessageProperties;
import io.hydramq.core.type.ConversionContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
@SuppressWarnings("Duplicates")
public class MessagePropertiesConverterTest {

    private static final Logger logger = LoggerFactory.getLogger(MessagePropertiesConverterTest.class);
    private ConversionContext conversions =
            ConversionContext.base().register(MessageProperties.class, new MessagePropertiesConverter());

    @Test
    public void testEmptyProperties() throws Exception {
        MessageProperties input = new MessageProperties();
        assertKeysSize(input, 0);
        MessageProperties output = marshall(input);
        assertKeysSize(output, 0);
    }

    @Test
    public void testWithValues() throws Exception {
        MessageProperties input = new MessageProperties();
        input.setString("message", "Hello World!");
        input.setBoolean("married", false);
        input.setBoolean("divorced", true);
        input.setInteger("age", 40);
        input.setLong("numMessages", 12345678900L);
        input.setFloat("temperature", (float) 45.3);
        input.setDouble("cost", 67.99);
        input.setBytes("opaque", "transparent".getBytes(CharsetUtil.UTF_8));
        input.setBytes("transparent", "opaque".getBytes(CharsetUtil.UTF_8));
        input.setShort("short", (short) 13);
        input.setByte("byte", (byte) 3);
        MessageProperties output = marshall(input);
        assertEquivalent(input, output);
    }

    public MessageProperties marshall(MessageProperties input) {
        ByteBuf buffer = Unpooled.buffer();
        conversions.write(MessageProperties.class, input, buffer);
        return conversions.read(MessageProperties.class, buffer);
    }

    public MessageProperties example() {
        return new MessageProperties().setString("message", "Hello World!")
                                      .setBoolean("married", false)
                                      .setBoolean("divorced", true)
                                      .setInteger("age", 40)
                                      .setLong("numMessages", 12345678900L)
                                      .setFloat("temperature", (float) 45.3)
                                      .setDouble("cost", 67.99)
                                      .setBytes("opaque", "transparent".getBytes(CharsetUtil.UTF_8))
                                      .setBytes("transparent", "opaque".getBytes(CharsetUtil.UTF_8))
                                      .setShort("short", (short) 13)
                                      .setByte("byte", (byte) 3);
    }

    @Test(invocationTimeOut = 5000)
    public void testCreatePerformance() throws Exception {
        for (int j = 0; j < 5; j++) {
            long start = System.currentTimeMillis();
            for (int i = 0; i < 1_000_000; i++) {
                example();
            }
            logger.info("{}", System.currentTimeMillis() - start);
        }
    }

    @Test(invocationTimeOut = 5000)
    public void testMarshallPerformance() throws Exception {
        for (int j = 0; j < 5; j++) {
            long start = System.currentTimeMillis();
            for (int i = 0; i < 100_000; i++) {
                marshall(example());
            }
            logger.info("{}", System.currentTimeMillis() - start);
        }
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
        assertKeysSize(output.getShortKeys(), input.getShortKeys().size());
        for (String key : input.getShortKeys()) {
            assertThat(input.getShort(key), is(output.getShort(key)));
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
        assertKeysSize(output.getByteKeys(), input.getByteKeys().size());
        for (String key : input.getByteKeys()) {
            assertThat(input.getByte(key), is(output.getByte(key)));
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