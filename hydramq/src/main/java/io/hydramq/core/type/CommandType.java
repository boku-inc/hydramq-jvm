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

package io.hydramq.core.type;

import io.hydramq.exceptions.HydraRuntimeException;

/**
 * @author jfulton
 */
public enum CommandType {
    HEARTBEAT(0),
    ACK(1),
    ERROR(2),
    WRITE_MESSAGE_SET(100),
    WRITE_MESSAGE_STREAM(101),
    READ_REQUEST(200),
    READ_RESPONSE(201),
    TOPIC_SUBSCRIPTION_REQUEST(500),
    PARTITION_LEASE_OFFER(501),
    PARTITION_LEASE_RESCIND(502),;
    private final int id;

    CommandType(final int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public static CommandType id(int id) {
        for (CommandType type : values()) {
            if (type.id() == id) {
                return type;
            }
        }
        throw new HydraRuntimeException("Invalid type '" + id + "'");
    }
}
