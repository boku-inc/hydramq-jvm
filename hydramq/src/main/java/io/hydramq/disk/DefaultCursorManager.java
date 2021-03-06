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

import java.util.Set;

import io.hydramq.Cursor;
import io.hydramq.CursorManager;
import io.hydramq.CursorSetManager;
import io.hydramq.PartitionId;

/**
 * @author jfulton
 */
public class DefaultCursorManager implements CursorManager {

    private final PartitionId partitionId;
    private final CursorSetManager cursorSetManager;

    public DefaultCursorManager(CursorSetManager cursorSetManager, PartitionId partitionId) {
        this.cursorSetManager = cursorSetManager;
        this.partitionId = partitionId;
    }

    @Override
    public void set(String cursor, long messageOffset) {
        cursorSetManager.getCursorSet(cursor).set(partitionId, messageOffset);
    }

    @Override
    public long get(String cursor) {
        return cursorSetManager.getCursorSet(cursor).get(partitionId);
    }

    @Override
    public Cursor cursor(String cursorName) {
        return new DefaultCursor(cursorSetManager.getCursorSet(cursorName), partitionId);
    }

    @Override
    public boolean hasCursor(String name) {
        return cursorSetManager.getCursorSet(name).hasCursor(partitionId);
    }

    @Override
    public Set<String> cursorNames() {
        return cursorSetManager.getCursorSetNames();
    }

    public PartitionId getPartitionId() {
        return partitionId;
    }
}
