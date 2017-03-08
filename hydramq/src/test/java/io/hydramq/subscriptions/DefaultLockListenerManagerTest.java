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

package io.hydramq.subscriptions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import io.hydramq.Topic;
import io.hydramq.TopicManager;
import io.hydramq.TopicManagers;
import io.hydramq.disk.PersistenceTestsBase;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class DefaultLockListenerManagerTest extends PersistenceTestsBase {

    @Test
    public void testSingleSubscriber() throws Exception {
        AtomicInteger locks1Acquired = new AtomicInteger();
        AtomicInteger locks1Releases = new AtomicInteger();

        try (TopicManager topicManager = TopicManagers.disk(messageStoreDirectory())) {
            Topic topic = topicManager.topic("topic");
            LockManager manager = new DefaultLockManager(topic);
            LockListener lockListener = (partitionId, state) -> {
                if (state == LockState.LOCKED) {
                    locks1Acquired.incrementAndGet();
                } else {
                    locks1Releases.incrementAndGet();
                }
                return CompletableFuture.completedFuture(null);
            };
            manager.subscribe("foo", lockListener);
            Thread.sleep(100);

            assertThat(locks1Acquired.get(), is(4));
            assertThat(locks1Releases.get(), is(0));

            manager.unsubscribe("foo", lockListener);
            Thread.sleep(100);

            // Unsubscribing assumes the client has stopped using resources on the topic
            assertThat(locks1Acquired.get(), is(4));
            assertThat(locks1Releases.get(), is(0));
        }
    }

    @Test
    public void testTwoSubscribersSameGroup() throws Exception {
        AtomicInteger locks1Acquired = new AtomicInteger();
        AtomicInteger locks1Released = new AtomicInteger();
        AtomicInteger lock2Acquired = new AtomicInteger();
        AtomicInteger lock2Released = new AtomicInteger();

        try (TopicManager topicManager = TopicManagers.disk(messageStoreDirectory())) {
            Topic topic = topicManager.topic("topic");
            LockManager manager = new DefaultLockManager(topic);
            LockListener lockListener1 = (partitionId, state) -> {
                if (state == LockState.LOCKED) {
                    locks1Acquired.incrementAndGet();
                } else {
                    locks1Released.incrementAndGet();
                }
                return CompletableFuture.completedFuture(null);
            };
            manager.subscribe("foo", lockListener1);
            Thread.sleep(100);

            assertThat(locks1Acquired.get(), is(4));
            assertThat(locks1Released.get(), is(0));

            LockListener lockListener2 = (partitionId, state) -> {
                if (state == LockState.LOCKED) {
                    lock2Acquired.incrementAndGet();
                } else {
                    lock2Released.incrementAndGet();
                }
                return CompletableFuture.completedFuture(null);
            };
            manager.subscribe("foo", lockListener2);
            Thread.sleep(100);

            assertThat(locks1Acquired.get(), is(4));
            assertThat(locks1Released.get(), is(2));
            assertThat(lock2Acquired.get(), is(2));
            assertThat(lock2Released.get(), is(0));

            manager.unsubscribe("foo", lockListener1);
            Thread.sleep(100);

            assertThat(locks1Acquired.get(), is(4));
            assertThat(locks1Released.get(), is(2));
            assertThat(lock2Acquired.get(), is(4));
            assertThat(lock2Released.get(), is(0));
        }
    }

    @Test
    public void testTwoSubscribersDifferentGroups() throws Exception {
        AtomicInteger locks1Acquired = new AtomicInteger();
        AtomicInteger locks1Releases = new AtomicInteger();
        AtomicInteger locks2Acquired = new AtomicInteger();
        AtomicInteger locks2Releases = new AtomicInteger();

        try (TopicManager topicManager = TopicManagers.disk(messageStoreDirectory())) {
            Topic topic = topicManager.topic("topic");
            LockManager manager = new DefaultLockManager(topic);
            LockListener lockListener1 = (partitionId, state) -> {
                if (state == LockState.LOCKED) {
                    locks1Acquired.incrementAndGet();
                } else {
                    locks1Releases.incrementAndGet();
                }
                return CompletableFuture.completedFuture(null);
            };
            manager.subscribe("foo", lockListener1);
            Thread.sleep(100);

            assertThat(locks1Acquired.get(), is(4));
            assertThat(locks1Releases.get(), is(0));

            LockListener lockListener2 = (partitionId, state) -> {
                if (state == LockState.LOCKED) {
                    locks2Acquired.incrementAndGet();
                } else {
                    locks2Releases.incrementAndGet();
                }
                return CompletableFuture.completedFuture(null);
            };
            manager.subscribe("bar", lockListener2);
            Thread.sleep(100);

            assertThat(locks1Acquired.get(), is(4));
            assertThat(locks1Releases.get(), is(0));
            assertThat(locks2Acquired.get(), is(4));
            assertThat(locks2Releases.get(), is(0));
        }
    }

    @Test
    public void testExpandPartitionCount() throws Exception {
        // TODO: implement
//        AtomicInteger locks1Acquired = new AtomicInteger();
//        AtomicInteger locks1Releases = new AtomicInteger();
//
//        try (TopicManager topicManager = TopicManagers.disk(messageStoreDirectory())) {
//            Topic topic = topicManager.topic("topic");
//            DefaultLockManager manager = new DefaultLockManager(topic);
//            LockListener subscription = new LockListener() {
//                @Override
//                public void onLend(PartitionId partitionId) {
//                    super.onLend(partitionId);
//                    locks1Acquired.incrementAndGet();
//                }
//
//                @Override
//                public void onRescind(PartitionId partitionId) {
//                    locks1Releases.incrementAndGet();
//                }
//            };
//            manager.acquirePartitionLocks("foo", subscription);
//
//            assertThat(locks1Acquired.get(), is(4));
//            assertThat(locks1Releases.get(), is(0));
//
//            topic.partitions(topic.partitions() + 2);
//
//            // Unsubscribing assumes the client has stopped using resources on the topic
//            assertThat(locks1Acquired.get(), is(6));
//            assertThat(locks1Releases.get(), is(0));
//
//            topic.partitions(topic.partitions() - 2);
//            assertThat(locks1Acquired.get(), is(6));
//            assertThat(locks1Releases.get(), is(2));
//        }
    }
}