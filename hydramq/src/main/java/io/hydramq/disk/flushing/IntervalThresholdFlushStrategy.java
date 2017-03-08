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

package io.hydramq.disk.flushing;

import java.time.Clock;
import java.time.Duration;

/**
 * Not Thread Safe.
 *
 * @author jfulton
 */
public class IntervalThresholdFlushStrategy implements FlushStrategy {

    protected Clock clock;
    private long lastFlush;
    private Duration interval;

    public IntervalThresholdFlushStrategy() {
        this(Duration.ofMillis(1000));
    }

    public IntervalThresholdFlushStrategy(Duration interval) {
        setClock(Clock.systemUTC());
        this.interval = interval;
    }

    @Override
    public boolean requiresFlush(final int byteCount) {
        long now = clock.millis();
        if (now - lastFlush >= interval.toMillis()) {
            lastFlush = now;
            return true;
        } else {
            return false;
        }
    }

    protected void setClock(final Clock clock) {
        this.clock = clock;
        this.lastFlush = clock.millis();
    }
}
