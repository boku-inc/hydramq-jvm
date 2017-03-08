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
import io.hydramq.core.type.ConversionContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class PartitionInfoResponseConverterTest {

    @Test
    public void testRoundTripMarshall() throws Exception {
        ConversionContext context = ConversionContext.topicProtocol();
        PartitionInfoResponse input = new PartitionInfoResponse(4, new PartitionInfo(9, 100000));
        ByteBuf buffer = Unpooled.buffer();
        context.write(PartitionInfoResponse.class, input, buffer);
        assertThat(buffer.getInt(0), is(204));
        PartitionInfoResponse output = context.read(PartitionInfoResponse.class, buffer);
        assertThat(output.getPartitionInfo().head(), is(input.getPartitionInfo().head()));
        assertThat(output.getPartitionInfo().tail(), is(input.getPartitionInfo().tail()));
    }
}