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
import java.nio.file.Paths;

import io.hydramq.Message;
import io.hydramq.Segment;
import io.hydramq.disk.flushing.FlushStrategies;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class DiskSegmentBuilderTest extends PersistenceTestsBase {

    @Test
    public void testBasicConstruction() throws Exception {
        new DiskSegmentBuilder().flushStrategy(FlushStrategies::standard);
    }

    @Test(dependsOnMethods = "testBasicConstruction")
    public void testCreateNewSegment() throws Exception {
        DiskSegmentBuilder builder = new DiskSegmentBuilder();
        Path segmentDirectory = getOutputDirectory().resolve(Paths.get("topic1", "partition3", "segment0"));
        try(Segment segment = builder.build(segmentDirectory)) {
            assertThat(Files.exists(segmentDirectory.resolve("segment.dat")), is(true));
            assertThat(Files.exists(segmentDirectory.resolve("segment.idx")), is(true));
        }
    }

    @Test
    public void testLoadExistingSegment() throws Exception {
        DiskSegmentBuilder builder = new DiskSegmentBuilder();
        Path segmentDirectory = getOutputDirectory().resolve("segment1");
        try (Segment segment = builder.build(segmentDirectory)) {
            for (int i = 0; i < 100; i++) {
                segment.write(Message.empty().build());
            }
        }
        try (Segment segment = builder.build(segmentDirectory)) {
            assertThat(segment.size(), is(100));
        }
    }
}