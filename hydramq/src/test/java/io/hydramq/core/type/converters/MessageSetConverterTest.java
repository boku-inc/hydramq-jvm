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

import java.util.Iterator;

import io.hydramq.Message;
import io.hydramq.MessageProperties;
import io.hydramq.MessageSet;
import io.hydramq.core.type.ConversionContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class MessageSetConverterTest {

    private ConversionContext context = ConversionContext.base()
            .register(Message.class, new MessageConverter())
            .register(MessageProperties.class, new MessagePropertiesConverter())
            .register(MessageSet.class, new MessageSetConverter());

    @Test
    public void testWriteAndRead() throws Exception {
        MessageSet input = new MessageSet(100);
        for (int i = 0; i < 100; i++) {
            input.add(Message.empty().withInteger("id", i).build());
        }
        ByteBuf buffer = Unpooled.buffer();
        context.write(MessageSet.class, input, buffer);
        MessageSet output = context.read(MessageSet.class, buffer);
        assertThat(output.size(), is(input.size()));
        assertThat(output.size(), is(100));
        Iterator<Message> outputIterator = output.iterator();
        Iterator<Message> inputIterator = input.iterator();
        while(outputIterator.hasNext()) {
            assertThat(outputIterator.next().getInteger("id"), is(inputIterator.next().getInteger("id")));
        }
    }
}