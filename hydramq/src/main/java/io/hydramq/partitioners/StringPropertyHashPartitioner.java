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
import io.hydramq.internal.util.PureJavaCrc32;

/**
 * @author jfulton
 */
public class StringPropertyHashPartitioner implements Partitioner {

    private final String propertyKey;
    private final Partitioner fallbackPartitioner;

    public StringPropertyHashPartitioner(final String propertyKey, Partitioner fallbackPartitioner) {
        this.propertyKey = propertyKey;
        this.fallbackPartitioner = fallbackPartitioner;
    }

    public StringPropertyHashPartitioner(String propertyKey) {
        this(propertyKey, Partitioners.random());
    }

    @Override
    public int partition(final Message message, final int partitions) {
        PureJavaCrc32 crc32 = new PureJavaCrc32();
        if (message.properties().hasString(propertyKey)) {
            byte[] bytes = message.properties().getString(propertyKey).getBytes();
            crc32.update(bytes, 0, bytes.length);
            return (int) (crc32.getValue() % partitions);
        } else {
            return fallbackPartitioner.partition(message, partitions);
        }
    }
}
