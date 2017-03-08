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

import java.time.Duration;

/**
 * @author jfulton
 */
public class DefaultSegmentationStrategy implements SegmentationStrategy {

    public static final int DEFAULT_MAX_MESSAGES_PER_SEGMENT = 1_000_000;
    public static final int DEFAULT_MAX_SEGMENTS_PER_PARTITION = 10;
    private final int maxMessages;
    private final int maxSegments;
    private final Duration maxSegmentTTL;
    private final SegmentRotationPolicy rotationPolicy;

    public DefaultSegmentationStrategy() {
        this(DEFAULT_MAX_MESSAGES_PER_SEGMENT);
    }

    public DefaultSegmentationStrategy(final int maxMessages) {
        this(maxMessages, DEFAULT_MAX_SEGMENTS_PER_PARTITION, Duration.ofDays(7), SegmentRotationPolicy.ARCHIVE);
    }

    public DefaultSegmentationStrategy(final int maxMessagesPerSegment, final int maxSegmentsPerPartition, final Duration maxSegmentTTL, SegmentRotationPolicy rotationPolicy) {
        this.maxMessages = maxMessagesPerSegment;
        this.maxSegments = maxSegmentsPerPartition;
        this.maxSegmentTTL = maxSegmentTTL;
        this.rotationPolicy = rotationPolicy;
    }

    @Override
    public SegmentRotationPolicy rotationPolicy() {
        return rotationPolicy;
    }

    @Override
    public int maxMessages() {
        return maxMessages;
    }

    @Override
    public int maxSegments() {
        return maxSegments;
    }

    @Override
    public Duration maxSegmentTTL() {
        return maxSegmentTTL;
    }
}
