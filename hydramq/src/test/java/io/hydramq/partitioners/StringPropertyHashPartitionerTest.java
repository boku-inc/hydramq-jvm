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

package io.hydramq.partitioners;

import io.hydramq.Message;
import io.hydramq.Partitioner;
import io.hydramq.Partitioners;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class StringPropertyHashPartitionerTest {

    @Test
    public void testHashing() throws Exception {
        Partitioner partitioner = new StringPropertyHashPartitioner("customerId", Partitioners.roundRobin());
        assertThat(partitioner.partition(Message.empty().withString("customerId", "Johnson").build(), 4), is(0));
        assertThat(partitioner.partition(Message.empty().withString("customerId", "Zacharias").build(), 4), is(1));
        assertThat(partitioner.partition(Message.empty().withString("customerId", "Archimedes").build(), 4), is(2));
        assertThat(partitioner.partition(Message.empty().withString("customerId", "Montoya").build(), 4), is(3));

        // Increase partition count changes hashes
        assertThat(partitioner.partition(Message.empty().withString("customerId", "Montoya").build(), 5), is(2));

        // The following use the fallback RoundRobinPartitioner
        assertThat(partitioner.partition(Message.empty().withString("customerName", "Zacharias").build(), 4), is(0));
        assertThat(partitioner.partition(Message.empty().withString("customerName", "Zacharias").build(), 4), is(1));
        assertThat(partitioner.partition(Message.empty().withString("customerName", "Zacharias").build(), 4), is(2));
        assertThat(partitioner.partition(Message.empty().withString("customerName", "Zacharias").build(), 4), is(3));

        // Back to hash mechanism
        assertThat(partitioner.partition(Message.empty().withString("customerId", "Montoya").build(), 4), is(3));
    }
}