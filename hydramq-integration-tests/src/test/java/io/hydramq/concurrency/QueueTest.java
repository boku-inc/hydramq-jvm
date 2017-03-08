package io.hydramq.concurrency;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.hydramq.core.net.Command;
import io.hydramq.core.net.Error;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * @author jfulton
 */
public class QueueTest {
    private static final Logger logger = LoggerFactory.getLogger(QueueTest.class);

    @Test
    public void testBlockingQueue() throws Exception {
        for (int i = 0; i < 20; i++) {

            run(1_000_000, Executors.newSingleThreadExecutor(), new ArrayBlockingQueue<>(10));
//            run(1_000_000, Executors.newSingleThreadExecutor(), new LinkedBlockingQueue<>(1000));

            Thread.sleep(100);
        }
    }

    @Test
    public void testName() throws Exception {
        int iterations = 1_000_000;
        CountDownLatch latch = new CountDownLatch(iterations);
        ExecutorService executorService = Executors.newCachedThreadPool();

        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            executorService.execute(latch::countDown);
        }

        latch.await();
        logger.info("{} commands in {}ms", iterations, (System.nanoTime() - start) / 1_000_000f);

    }

    public void run(int iterations, Executor executor, BlockingQueue<Command> queue) throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(iterations);

        executor.execute(() -> {
            for (; ; ) {
                try {
                    queue.take();
                    latch.countDown();
                } catch (InterruptedException e) {
                    logger.info("An error occurred", e);
                }
            }
        });

        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            queue.offer(new Error(0, 0), 1000, TimeUnit.MILLISECONDS);
        }

        latch.await();
        logger.info("{} commands in {}ms with {}", iterations, (System.nanoTime() - start) / 1_000_000f, queue.getClass());
    }
}
