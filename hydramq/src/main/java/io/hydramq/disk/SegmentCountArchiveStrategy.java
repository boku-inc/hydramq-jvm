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

import io.hydramq.Segment;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

/**
 * @author jfulton
 */
public class SegmentCountArchiveStrategy implements SegmentArchiveStrategy {

    private final int maxSegments;

    public SegmentCountArchiveStrategy(int maxSegments) {
        this.maxSegments = maxSegments;
    }

    @Override
    public LongSet select(Long2ObjectSortedMap<Segment> segments) {
        LongSet results = new LongOpenHashSet();
        if (segments.size() > maxSegments) {
            int segmentsToArchive = segments.size() - maxSegments;
            LongBidirectionalIterator iterator = segments.keySet().iterator();
            // Move to last
            while (iterator.hasNext()) {
                iterator.next();
            }
            for (int i = segmentsToArchive; i > 0; i--) {
                results.add(iterator.previousLong());
            }
        }
        return results;
    }
}
