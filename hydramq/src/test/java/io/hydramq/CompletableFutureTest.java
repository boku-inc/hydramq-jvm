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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import io.hydramq.core.net.Command;
import io.hydramq.core.net.Error;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
@SuppressWarnings("Duplicates")
public class CompletableFutureTest {

    private static final Logger logger = getLogger(CompletableFutureTest.class);

    @Test
    public void testExceptions() throws Exception {
        AtomicBoolean thenAcceptCalled = new AtomicBoolean(false);
        AtomicBoolean exceptionallyCalled = new AtomicBoolean(false);
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.exceptionally(throwable -> {
            exceptionallyCalled.set(true);
            return null;
        }).thenAccept(aVoid -> {
            if (!future.isCompletedExceptionally()) {
                thenAcceptCalled.set(true);
            }
        });

        future.completeExceptionally(new RuntimeException());
        assertThat(thenAcceptCalled.get(), is(false));
        assertThat(exceptionallyCalled.get(), is(true));
    }

    @Test
    public void testExceptionallyCalledAfterThenAccept() throws Exception {
        AtomicBoolean thenAcceptCalled = new AtomicBoolean(false);
        AtomicBoolean exceptionallyCalled = new AtomicBoolean(false);
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.thenAccept(aVoid -> {
            if (!future.isCompletedExceptionally()) {
                thenAcceptCalled.set(true);
            }
        }).exceptionally(throwable -> {
            exceptionallyCalled.set(true);
            return null;
        });
        //        future.complete(null);
        future.completeExceptionally(new RuntimeException());
        assertThat(thenAcceptCalled.get(), is(false));
        assertThat(exceptionallyCalled.get(), is(true));
    }

    @Test
    public void testThenAcceptAndExceptionallyWithoutStatusCheck() throws Exception {
        AtomicBoolean thenAcceptCalled = new AtomicBoolean(false);
        AtomicBoolean exceptionallyCalled = new AtomicBoolean(false);
        CompletableFuture<String> future = new CompletableFuture<>();
        future.thenAccept(value -> {
            thenAcceptCalled.set(true);
        }).exceptionally(throwable -> {
            exceptionallyCalled.set(true);
            return null;
        });
        future.completeExceptionally(new RuntimeException());
        assertThat(thenAcceptCalled.get(), is(false));
        assertThat(exceptionallyCalled.get(), is(true));
    }

    @Test
    public void testExceptionallyAndThenAcceptWithoutStatusCheck() throws Exception {
        // thenAccept gets called regardless, if called after exceptionally
        AtomicBoolean thenAcceptCalled = new AtomicBoolean(false);
        AtomicBoolean exceptionallyCalled = new AtomicBoolean(false);
        CompletableFuture<String> future = new CompletableFuture<>();
        future.exceptionally(throwable -> {
            exceptionallyCalled.set(true);
            return null;
        }).thenAccept(value -> {
            thenAcceptCalled.set(true);
        });
        future.completeExceptionally(new RuntimeException());
        assertThat(thenAcceptCalled.get(), is(true));
        assertThat(exceptionallyCalled.get(), is(true));
    }

    @Test
    public void testComposedExceptionally() throws Exception {
        AtomicBoolean thenAcceptCalled = new AtomicBoolean(false);
        AtomicBoolean exceptionallyCalled = new AtomicBoolean(false);
        CompletableFuture<String> firstFuture = new CompletableFuture<>();
        CompletableFuture<String> secondFuture = new CompletableFuture<>();
        CompletableFuture<String> combinedFuture =
                firstFuture.applyToEither(secondFuture, Function.identity());
        combinedFuture.thenAccept(value -> {
            thenAcceptCalled.set(true);
        });
        combinedFuture.exceptionally(throwable -> {
            exceptionallyCalled.set(true);
            return null;
        });
        secondFuture.completeExceptionally(new RuntimeException());

        assertThat(thenAcceptCalled.get(), is(false));
        assertThat(exceptionallyCalled.get(), is(true));
    }


    @Test
    public void testConversion() throws Exception {
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicBoolean excepted = new AtomicBoolean(false);
        CompletableFuture<Command> commandFuture = new CompletableFuture<>();
        CompletableFuture<Void> replyFuture = commandFuture.thenCompose(command -> {
            CompletableFuture<Void> f = new CompletableFuture<>();
            if (command instanceof Error) {
                f.completeExceptionally(new RuntimeException("Error code {}" + ((Error) command).code()));
            } else {
                f.complete(null);
            }
            return f;
        });
        replyFuture.thenAccept(aVoid -> {
            completed.set(true);
        }).exceptionally(throwable -> {
            excepted.set(true);
            return null;
        });
        commandFuture.completeExceptionally(new RuntimeException());
        assertThat(completed.get(), is(false));
        assertThat(excepted.get(), is(true));
    }

    @Test
    public void testComposedCancel() throws Exception {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            AtomicInteger counter = new AtomicInteger(5);
            while (!future.isCancelled() && counter.getAndDecrement() > 0) {
                try {
                    logger.info("Processing");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (counter.get() == 0) {
                future.complete(null);
            }
        });

        future.thenAccept(aVoid -> {
            logger.info("Complete");
        });
        future.exceptionally(throwable -> {
            logger.info("Error!", throwable);
            return null;
        });

        Thread.sleep(2500);
        future.cancel(true);
    }

    @Test
    public void testCancel() throws Exception {
        AtomicInteger counter = new AtomicInteger(5);
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            while (counter.getAndDecrement() > 0) {
                try {
                    logger.info("Processing");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread.sleep(2500);
        future.cancel(true);
        Thread.sleep(2500);
    }

    @Test
    public void testCompleteBlocksOnListeners() throws Exception {
        List<String> results = new ArrayList<>();
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.thenAccept(aVoid -> {
            results.add("one");
            sleep(100);
        });
        future.thenAccept(aVoid -> {
            results.add("two");
            sleep(100);
        });
        future.thenAccept(aVoid -> {
            results.add("three");
            sleep(100);
        });
        future.complete(null);
        results.add("done");

        assertThat(results.get(0), is("three"));
        assertThat(results.get(1), is("two"));
        assertThat(results.get(2), is("one"));
        assertThat(results.get(3), is("done"));
    }

    private void sleep(int value) {
        try {
            Thread.sleep(value);
        } catch (InterruptedException e) {
            logger.error("Error sleeping", e);
        }
    }
}
