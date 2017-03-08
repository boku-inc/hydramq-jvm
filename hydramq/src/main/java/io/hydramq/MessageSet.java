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

package io.hydramq;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author jfulton
 */
public class MessageSet implements Iterable<Message> {

    private final long messageOffset;
    private List<Message> messages = new ArrayList<>();

    public MessageSet(final long startOffset) {
        this.messageOffset = startOffset;
    }

    public long startOffset() {
        return messageOffset;
    }

    public long nextOffset() {
        return messageOffset + messages.size();
    }

    @Override
    public Iterator<Message> iterator() {
        return messages.iterator();
    }

    public int size() {
        return messages.size();
    }

    public MessageSet add(Message message) {
        if (message != null) {
            messages.add(message);
        }
        return this;
    }

    public Stream<Message> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    public Stream<Message> parallelStream() {
        return StreamSupport.stream(spliterator(), true);
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public void clear() {
        messages.clear();
    }
}
