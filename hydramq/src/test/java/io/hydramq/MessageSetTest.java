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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class MessageSetTest {

    @Test
    public void testIterable() throws Exception {
        MessageSet messageSet = new MessageSet(0);
        for (int i = 0; i < 10; i++) {
            messageSet.add(Message.empty().withInteger("messageNumber", i).build());
        }
        int expectedMessageNumber = 0;
        for (Message message : messageSet) {
            assertThat(message.properties().getInteger("messageNumber"), is(expectedMessageNumber++));
        }
    }

    @Test
    public void testIsEmpty() throws Exception {
        assertThat(new MessageSet(0).isEmpty(), is(true));
        assertThat(new MessageSet(0).add(Message.empty().build()).isEmpty(), is(false));
    }

    @Test
    public void testNextOffset() throws Exception {
        MessageSet messageSet = new MessageSet(0);
        assertThat(messageSet.nextOffset(), is(0L));
        messageSet.add(Message.empty().build());
        assertThat(messageSet.nextOffset(), is(1L));
    }
}