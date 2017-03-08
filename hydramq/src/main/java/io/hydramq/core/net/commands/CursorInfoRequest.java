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

package io.hydramq.core.net.commands;

import io.hydramq.CursorInfo;
import io.hydramq.PartitionId;
import io.hydramq.core.net.Request;

/**
 * @author jfulton
 */
public class CursorInfoRequest extends Request {

    private final PartitionId partitionId;
    private final String cursorName;

    public CursorInfoRequest(int correlationId, PartitionId partitionId, String cursorName) {
        super(correlationId);
        this.partitionId = partitionId;
        this.cursorName = cursorName;
    }

    public CursorInfoRequest(PartitionId partitionId, String cursorName) {
        this.partitionId = partitionId;
        this.cursorName = cursorName;
    }

    public PartitionId getPartitionId() {
        return partitionId;
    }

    public CursorInfoResponse reply(CursorInfo cursorInfo) {
        return new CursorInfoResponse(correlationId(), cursorInfo);
    }

    public String getCursorName() {
        return cursorName;
    }
}
