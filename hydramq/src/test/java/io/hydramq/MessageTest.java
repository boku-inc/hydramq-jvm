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

import org.testng.annotations.Test;

import static io.hydramq.client.RetryProperties.EXPIRES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class MessageTest {

    @Test
    public void testConvenienceMethodsParityWithProperties() throws Exception {
        Message message = Message.withBodyAsString("Foo")
                                 .withBoolean("key", true)
                                 .withBytes("key", "Hello".getBytes())
                                 .withDouble("key", 34.2)
                                 .withFloat("key", 12.5f)
                                 .withInteger("key", 25)
                                 .withLong("key", 100L)
                                 .withString("key", "value")
                                 .build();

        assertThat(message.getInteger("key"), is(25));
        assertThat(message.getInteger("missingKey", 26), is(26));
        // TODO: complete tests

        Message.empty().withString("foo", "foo")
                .withLong(EXPIRES, System.currentTimeMillis() + 1000);

    }
}