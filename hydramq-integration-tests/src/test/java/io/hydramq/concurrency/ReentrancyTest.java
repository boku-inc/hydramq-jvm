package io.hydramq.concurrency;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * @author jfulton
 */
public class ReentrancyTest {
    private static final Logger logger = LoggerFactory.getLogger(ReentrancyTest.class);

    @Test
    public void testChangeState() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Example example = new Example(e -> {
            if (!e.isState()) {
                e.changeState(true);
                latch.countDown();
            }
        });

        Executors.newCachedThreadPool().execute(() -> {
            example.changeState(false);
        });

        latch.await();
    }

    private static class Example {
        private Lock lock = new ReentrantLock();
        private boolean state = true;
        private StateChangeListener listener;

        public Example(StateChangeListener listener) {
            this.listener = listener;
        }

        public void changeState(boolean state) {
            try {
                lock.lock();
                    if (this.state != state) {
                        logger.info("Changing state to {}", state);
                        this.state = state;
                        listener.onChanged(this); // must come last
//                        ForkJoinPool.commonPool().execute(() -> {
//                        });
                        logger.info("State changed");
                    }
            } finally {
                lock.unlock();
            }
        }

        public boolean isState() {
            return state;
        }
    }

    private interface StateChangeListener {

        void onChanged(Example example);
    }
}
