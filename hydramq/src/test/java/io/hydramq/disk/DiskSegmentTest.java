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

import java.nio.file.Files;
import java.nio.file.Path;

import io.hydramq.Message;
import io.hydramq.MessageSet;
import io.hydramq.exceptions.HydraRuntimeException;
import org.hamcrest.CoreMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.testng.Assert.fail;

/**
 * @author jfulton
 */
@SuppressWarnings("Duplicates")
public class DiskSegmentTest extends PersistenceTestsBase {

    private static final Logger logger = LoggerFactory.getLogger(DiskSegmentTest.class);


    @Test
    public void testConstruction() throws Exception {
        Path segmentDirectory = getOutputDirectory().resolve("segment1");
        Path segmentIndex = segmentDirectory.resolve("segment.idx");
        Path segmentData = segmentDirectory.resolve("segment.dat");
        try (DiskSegment segment = new DiskSegment(segmentDirectory)) {
            assertThat(segment.size(), is(0));
            assertThat(Files.exists(segmentDirectory), is(true));
            assertThat(Files.isDirectory(segmentDirectory), is(true));
            assertThat(Files.exists(segmentIndex), is(true));
            assertThat(Files.exists(segmentData), is(true));
            assertThat(Files.isRegularFile(segmentIndex), is(true));
            assertThat(Files.isRegularFile(segmentData), is(true));

            MessageSet batch = new MessageSet(0);
            segment.read(0, 0, batch);
            assertThat(batch.size(), is(0));
        }
        assertThat(Files.size(segmentIndex), is(0L));
        assertThat(Files.size(segmentData), is(0L));
    }

    @Test
    public void testOperationsWithSingleMessage() throws Exception {
        Path segmentDirectory = getOutputDirectory().resolve("segment2");
        Path segmentIndex = segmentDirectory.resolve("segment.idx");
        Path segmentData = segmentDirectory.resolve("segment.dat");
        try (DiskSegment segment = new DiskSegment(segmentDirectory)) {
            segment.write(Message.empty().build());
            assertThat(segment.size(), is(1));
        }
        assertThat(Files.size(segmentIndex), is(12L));
        assertThat(Files.size(segmentData), is(12L));
        try (DiskSegment segment = new DiskSegment(segmentDirectory)) {
            MessageSet messages = new MessageSet(0);
            assertThat(segment.size(), is(1));
            assertThat(segment.read(0), notNullValue());
            segment.read(0, 0, messages);
            assertThat(messages.size(), is(0));
            messages.clear();
            segment.read(0, 1, messages);
            assertThat(messages.size(), is(1));
            messages.clear();
            segment.read(0, 2, messages);
            assertThat(messages.size(), is(1));
            messages.clear();
            try {
                segment.read(1);
                failExpectedOutOfBounds();
            } catch (HydraRuntimeException ignored) {
            }
            messages.clear();
            segment.read(1, 0, messages);
            assertThat(messages.size(), is(0));
            messages.clear();
            segment.read(1, 1, messages);
            assertThat(messages.size(), is(0));
        }
    }

    @Test
    public void testWritesThenReads() throws Exception {
        try (DiskSegment segment = new DiskSegment(getOutputDirectory().resolve("segment3"))) {
            int writeCount = 10_000;
            for (int i = 0; i < writeCount; i++) {
                Message message = Message.withBodyAsString("Hello world!")
                                         .withString("firstName", "Jimmie" + i)
                                         .withInteger("messageNumber", i)
                                         .build();
                segment.write(message);
            }
            int readCount = 0;
            for (int i = 0; i < segment.size(); i++) {
                Message message = segment.read(i);
                assertThat(message.bodyAsString(), is("Hello world!"));
                assertThat(message.getString("firstName"), is("Jimmie" + i));
                assertThat(message.getInteger("messageNumber"), is(i));
                readCount++;
            }
            assertThat(readCount, is(writeCount));
            MessageSet messages = new MessageSet(0);
            segment.read(0, 10_000, messages);
            assertThat(messages.size(), is(10_000));
            int expectedMessageNumber = 0;
            for (Message message : messages) {
                assertThat(message.getInteger("messageNumber"), is(expectedMessageNumber++));
            }
        }
    }

    @Test
    public void testNegativeOffset() throws Exception {
        try (DiskSegment segment = new DiskSegment(segmentDirectory())) {
            MessageSet messageSet = new MessageSet(0);
            try {
                segment.read(-1, 1, messageSet);
                fail(HydraRuntimeException.class.getSimpleName() + " expected");
            } catch (HydraRuntimeException ex) {
                assertThat(ex.getCause(), CoreMatchers.instanceOf(IllegalArgumentException.class));
            }

        }
    }

    private void failExpectedOutOfBounds() {
        fail("HydraRuntimeException expected for index out of range");
    }
}