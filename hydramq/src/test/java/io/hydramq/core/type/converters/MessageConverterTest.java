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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class MessageConverterTest {

    @Test
    public void testConversion() throws Exception {
        Message input = new Message("Hello World!".getBytes(CharsetUtil.UTF_8));
        input.properties().setString("firstName", "Jimmie").setString("lastName", "Fulton").setInteger("age", 40);
        ConversionContext context = ConversionContext.base()
                .register(Message.class, new MessageConverter())
                .register(MessageProperties.class, new MessagePropertiesConverter());
        ByteBuf buffer = Unpooled.buffer();
        context.write(input, buffer);
        Message output = context.read(Message.class, buffer);
        assertEquals(input, output);
    }

    private void assertEquals(Message input, Message output) {
        assertThat(new String(output.body(), CharsetUtil.UTF_8), is(new String(input.body(), CharsetUtil.UTF_8)));
        assertThat(output.properties().getStringKeys().size(), is(input.properties().getStringKeys().size()));
        assertThat(output.properties().getIntegerKeys().size(), is(input.properties().getIntegerKeys().size()));
        assertThat(output.properties().getLongKeys().size(), is(input.properties().getLongKeys().size()));
        for (String key : output.properties().getStringKeys()) {
            assertThat(output.properties().getString(key), is(input.properties().getString(key)));
        }
        for (String key : output.properties().getIntegerKeys()) {
            assertThat(output.properties().getInteger(key), is(input.properties().getInteger(key)));
        }
        for (String key : output.properties().getLongKeys()) {
            assertThat(output.properties().getLong(key), is(input.properties().getLong(key)));
        }
    }
}