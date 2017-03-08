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

package io.hydramq.internal.util;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import com.codahale.metrics.InstrumentedThreadFactory;
import com.codahale.metrics.MetricRegistry;

/**
 * @author jfulton
 */
public class AsyncUtils {

    public static <T> CompletableFuture<T> failAfter(Duration duration) {
        final CompletableFuture<T> promise = new CompletableFuture<>();
        final TimeoutException ex = new TimeoutException("Timeout after " + duration.toMillis() + "ms");
        scheduler.schedule(() -> promise.completeExceptionally(ex), duration.toMillis(), TimeUnit.MILLISECONDS);
        return promise;
    }

    public static <T> CompletableFuture<T> returnAfter(Duration duration, T value) {
        final CompletableFuture<T> promise = new CompletableFuture<>();
        scheduler.schedule(() -> promise.complete(value), duration.toMillis(), TimeUnit.MILLISECONDS);
        return promise;
    }

    public static <T> CompletableFuture<T> within(CompletableFuture<T> future, Duration duration) {
        final CompletableFuture<T> timeout = failAfter(duration);
        return future.applyToEither(timeout, Function.identity());
    }

    public static <T> CompletableFuture<T> within(CompletableFuture<T> future, Duration duration, T defaultValue) {
        final CompletableFuture<T> timeout = returnAfter(duration, defaultValue);
        return future.applyToEither(timeout, Function.identity());
    }

    private static final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1, new InstrumentedThreadFactory(Executors.defaultThreadFactory(),
                    new MetricRegistry()));
}
