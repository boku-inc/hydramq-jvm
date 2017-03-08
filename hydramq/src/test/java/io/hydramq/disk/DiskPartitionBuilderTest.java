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
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


/**
 * @author jfulton
 */
public class DiskPartitionBuilderTest extends PersistenceTestsBase {

    @Test
    public void testCreateNewPartition() throws Exception {
        DiskPartitionBuilder builder = new DiskPartitionBuilder();
        Path partitionDirectory = partitionDirectory();
        try (DiskPartition partition = builder.build(partitionDirectory)) {
            partition.write(Message.empty().build()).join();
            assertThat(partition.tail(), is(1L));
        }

        assertThat(Files.exists(partitionDirectory.resolve("segments").resolve(SegmentUtils.getSegmentName(0))), is(true));
    }

    @Test
    public void testLoadExistingPartition() throws Exception {
        DiskPartitionBuilder builder = new DiskPartitionBuilder();
        Path partitionDirectory = partitionDirectory();
        try (DiskPartition partition = builder.build(partitionDirectory)) {
            partition.write(Message.empty().build()).join();
            assertThat(partition.tail(), is(1L));
        }
        try (DiskPartition partition = builder.build(partitionDirectory)) {
            assertThat(partition.tail(), is(1L));
        }
    }
}