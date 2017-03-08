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

package io.hydramq.topics;

import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class TopicWrapperTest {

    @Test
    public void testEqualsSameInstance() throws Exception {
        TopicWrapper wrapper = new TopicWrapper(new CompositeTopic("test"));
        assertThat(wrapper.equals(wrapper), is(true));
    }

    @Test
    public void testEqualsDifferentWrapperInstances() throws Exception {
        CompositeTopic wrapped = new CompositeTopic("test");
        TopicWrapper wrapper1 = new TopicWrapper(wrapped);
        TopicWrapper wrapper2 = new TopicWrapper(wrapped);
        assertThat(wrapper1.equals(wrapper2), is(true));
    }

    @Test
    public void testEqualsDifferentWrappedInstances() throws Exception {
        CompositeTopic wrapped1 = new CompositeTopic("test1");
        CompositeTopic wrapped2 = new CompositeTopic("test2");
        TopicWrapper wrapper1 = new TopicWrapper(wrapped1);
        TopicWrapper wrapper2 = new TopicWrapper(wrapped2);
        assertThat(wrapper1.equals(wrapper2), is(false));
    }

    @Test
    public void testWrapperEqualsWrapped() throws Exception {
        CompositeTopic wrapped = new CompositeTopic("test1");

        TopicWrapper wrapper = new TopicWrapper(wrapped);

        assertThat(wrapper.equals(wrapped), is(true));
        assertThat(wrapped.equals(wrapper), is(true));
    }
}