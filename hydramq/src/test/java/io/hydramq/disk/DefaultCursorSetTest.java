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

import java.util.HashMap;
import java.util.Map;

import io.hydramq.Cursor;
import io.hydramq.CursorSet;
import io.hydramq.PartitionId;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class DefaultCursorSetTest extends PersistenceTestsBase {
    @Test
    public void testWriteNewCursorSet() throws Exception {
        PartitionId partitionId = PartitionId.create();
        try (CursorSet cursorSet = new DefaultCursorSet(getOutputDirectory().resolve("cursor1.cur"))) {
            cursorSet.set(partitionId, 100L);
        }
        try (CursorSet cursorSet = new DefaultCursorSet(getOutputDirectory().resolve("cursor1.cur"))) {
            assertThat(cursorSet.get(partitionId), is(100L));
        }
    }

    @Test
    public void testWriteMultiple() throws Exception {
        Map<PartitionId, Long> expected = new HashMap<>();
        expected.put(PartitionId.create(), 31L);
        expected.put(PartitionId.create(), 1000L);
        try (CursorSet cursorSet = new DefaultCursorSet(getOutputDirectory().resolve("cursor3.cur"))) {
            expected.forEach(cursorSet::set);
        }

        try (CursorSet cursorSet = new DefaultCursorSet(getOutputDirectory().resolve("cursor3.cur"))) {
            expected.forEach((partitionId, aLong) -> {
                assertThat(cursorSet.get(partitionId), is(aLong));
            });
        }

    }

    @Test
    public void testWriteThroughCursor() throws Exception {
        PartitionId partitionId = PartitionId.create();
        try (CursorSet cursorSet = new DefaultCursorSet(getOutputDirectory().resolve("cursor4.cur"))) {
            Cursor cursor = cursorSet.forPartition(partitionId);
            assertThat(cursor.get(), is(0L));
            cursor.set(123L);
        }
        try (CursorSet cursorSet = new DefaultCursorSet(getOutputDirectory().resolve("cursor4.cur"))) {
            assertThat(cursorSet.get(partitionId), is(123L));
        }
    }

    @Test
    public void testReadThroughCursor() throws Exception {
        PartitionId partitionId = PartitionId.create();
        try (CursorSet cursorSet = new DefaultCursorSet(getOutputDirectory().resolve("cursor4.cur"))) {
            assertThat(cursorSet.get(partitionId), is(0L));
            cursorSet.set(partitionId, 541L);
        }
        try (CursorSet cursorSet = new DefaultCursorSet(getOutputDirectory().resolve("cursor4.cur"))) {
            Cursor cursor = cursorSet.forPartition(partitionId);
            assertThat(cursor.get(), is(541L));
        }
    }

    @Test
    public void testReadFromEmptyCursor() throws Exception {
        try (CursorSet cursorSet = new DefaultCursorSet(getOutputDirectory().resolve("cursor6.cur"))) {
            assertThat(cursorSet.size(), is(0));
            for (int i = 9; i > 0; i--) {
                assertThat(cursorSet.get(PartitionId.create()), is(0L));
            }
            assertThat(cursorSet.size(), is(0));
        }
    }

    @Test
    public void testSize() throws Exception {
        try (CursorSet cursorSet = new DefaultCursorSet(getOutputDirectory().resolve("cursor7.cur"))) {
            assertThat(cursorSet.size(), is(0));
            PartitionId partitionOne = PartitionId.create();
            cursorSet.set(partitionOne, 100L);
            assertThat(cursorSet.size(), is(1));
            cursorSet.set(partitionOne, 200L);
            assertThat(cursorSet.size(), is(1));
            PartitionId partitionTwo = PartitionId.create();
            cursorSet.set(partitionTwo, 300L);
            assertThat(cursorSet.size(), is(2));
        }
    }


    @Test(timeOut = 20_000)
    public void testWriteSpeed() throws Exception {
        PartitionId partitionId = PartitionId.create();
        try (CursorSet cursorSet = new DefaultCursorSet(getOutputDirectory().resolve("cursor8.cur"))) {
            for (int i = 0; i < 1_000_000; i++) {
                cursorSet.set(partitionId, i);
            }
            assertThat(cursorSet.get(partitionId), is((long)1_000_000 - 1));
        }
    }

    @Test(timeOut = 1000)
    public void testReadSpeed() throws Exception {
        try (CursorSet cursorSet = new DefaultCursorSet(getOutputDirectory().resolve("cursor9.cur"))) {
            PartitionId partitionId = PartitionId.create();
            cursorSet.set(partitionId, 1_000_000);
            for (int i = 0; i < 1_000_000; i++) {
                cursorSet.get(partitionId);
            }
        }
    }
}