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
import java.time.temporal.ChronoUnit;

import org.hamcrest.MatcherAssert;
import org.hamcrest.core.Is;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * @author jfulton
 */
public class IntervalThresholdFlushStrategyTest {

    @Test
    public void testWithMillis() throws Exception {
        IntervalThresholdFlushStrategy flushStrategy = new IntervalThresholdFlushStrategy(Duration.of(1000, ChronoUnit.MILLIS));
        assertFlushes(flushStrategy);
    }

    @Test
    public void testWithSeconds() throws Exception {
        IntervalThresholdFlushStrategy flushStrategy = new IntervalThresholdFlushStrategy(Duration.of(1, ChronoUnit.SECONDS));
        assertFlushes(flushStrategy);
    }

    private void assertFlushes(IntervalThresholdFlushStrategy flushStrategy) {
        Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.millis()).thenReturn(0L, 0L, 500L, 1000L, 1001L, 1500L, 2000L);
        flushStrategy.setClock(clock);
        MatcherAssert.assertThat(flushStrategy.requiresFlush(0), Is.is(false));
        MatcherAssert.assertThat(flushStrategy.requiresFlush(0), Is.is(false));
        MatcherAssert.assertThat(flushStrategy.requiresFlush(0), Is.is(true));
        MatcherAssert.assertThat(flushStrategy.requiresFlush(0), Is.is(false));
        MatcherAssert.assertThat(flushStrategy.requiresFlush(0), Is.is(false));
        MatcherAssert.assertThat(flushStrategy.requiresFlush(0), Is.is(true));
    }
}