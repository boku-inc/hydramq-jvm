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

package io.hydramq.disk;

import java.util.concurrent.atomic.AtomicInteger;

import io.hydramq.Message;
import io.hydramq.Partition;
import io.hydramq.client.DiskPartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class DiskPartitionReaderTest extends PersistenceTestsBase {

    private static final Logger logger = LoggerFactory.getLogger(DiskPartitionReaderTest.class);

    @Test
    public void testName() throws Exception {
        try (Partition master = partition(1000); Partition slave = partition(1000)) {

            for (int i = 0; i < 1000; i++) {
                master.write(Message.empty().withInteger("count", i).build()).join();
            }

            AtomicInteger masterExpectedCount = new AtomicInteger();
            DiskPartitionReader.from(master).startingAtHead().readingToTail((messages, throwable) -> {
                messages.forEach(message -> {
                    assertThat(message.getInteger("count"), is(masterExpectedCount.getAndIncrement()));
                    slave.write(message).join();
                });
            }).join();
            assertThat(masterExpectedCount.get(), is(1000));

            AtomicInteger slaveMessageCount = new AtomicInteger();
            DiskPartitionReader.from(slave).startingAtHead().readingToTail((messages) -> {
                messages.forEach(message -> assertThat(message.getInteger("count"), is(slaveMessageCount.getAndIncrement())));
            }).join();
            assertThat(slaveMessageCount.get(), is(1000));
        }
    }
}