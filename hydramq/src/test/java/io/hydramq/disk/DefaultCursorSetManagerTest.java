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

import java.nio.file.Path;
import java.util.Set;

import io.hydramq.CursorSetManager;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class DefaultCursorSetManagerTest extends PersistenceTestsBase {

    @Test
    public void testCreateCursorSet() throws Exception {
        try (CursorSetManager cursorSetManager = new DefaultCursorSetManager(partitionDirectory())) {
            assertThat(cursorSetManager.hasCursorSet("Blah"), is(false));
            cursorSetManager.getCursorSet("Blah");
            assertThat(cursorSetManager.hasCursorSet("Blah"), is(true));
        }
    }

    @Test
    public void testCaseInsensitive() throws Exception {
        try (CursorSetManager cursorSetManager = new DefaultCursorSetManager(partitionDirectory())) {
            assertThat(cursorSetManager.hasCursorSet("Blah"), is(false));
            cursorSetManager.getCursorSet("Blah");
            assertThat(cursorSetManager.hasCursorSet("blah"), is(true));
            cursorSetManager.getCursorSet("foo");
            assertThat(cursorSetManager.hasCursorSet("Foo"), is(true));
            Set<String> cursorSetNames = cursorSetManager.getCursorSetNames();
            assertThat(cursorSetNames.size(), is(2));
            assertThat(cursorSetNames, contains("foo", "blah"));
        }
    }

    @Test
    public void testLoad() throws Exception {
        Path baseDirectory = partitionDirectory();
        try (CursorSetManager cursorSetManager = new DefaultCursorSetManager(baseDirectory)) {
            assertThat(cursorSetManager.getCursorSetNames().size(), is(0));
            cursorSetManager.getCursorSet("One");
            cursorSetManager.getCursorSet("Two");
        }

        try (CursorSetManager cursorSetManager = new DefaultCursorSetManager(baseDirectory)) {
            assertThat(cursorSetManager.getCursorSetNames().size(), is(2));
            assertThat(cursorSetManager.getCursorSetNames(), contains("one","two"));
        }
    }

    @Test
    public void testClose() throws Exception {

    }
}