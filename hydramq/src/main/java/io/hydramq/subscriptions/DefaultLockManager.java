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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.hydramq.PartitionId;
import io.hydramq.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hydramq.listeners.Listen.CONTINUOUSLY;

/**
 * @author jfulton
 */
public class DefaultLockManager implements LockManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultLockManager.class);
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Topic topic;
    private Map<String, LockGroup> lockGroups = new HashMap<>();

    public DefaultLockManager(final Topic topic) {
        this.topic = topic;
        topic.discoverPartitions((partitionId, flags) -> lockGroups.values().forEach(this::rebalance), CONTINUOUSLY);
    }

    @Override
    public void subscribe(String lockGroupName, LockListener lockListener) {
        LockGroup lockGroup;
        try {
            lock.writeLock().lock();
            lockGroup = lockGroups.computeIfAbsent(lockGroupName, sg -> new LockGroup());
            lockGroup.addListener(lockListener);
            rebalance(lockGroup);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void unsubscribe(String lockGroupName, LockListener lockListener) {
        LockGroup lockGroup;
        try {
            lock.writeLock().lock();
            lockGroup = lockGroups.get(lockGroupName);
            if (lockGroup != null) {
                lockGroup.removeListener(lockListener);
                if (lockGroup.getSubscriptions().size() == 0) {
                    lockGroups.remove(lockGroupName, lockGroup);
                }
            }
            rebalance(lockGroup);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private synchronized void rebalance(LockGroup lockGroup) {
        Map<LockListener, Map<PartitionId, LockListener>> toRelease = new HashMap<>();
        Map<LockListener, Set<PartitionId>> toLock = new HashMap<>();
        try {
            lock.writeLock().lock();

            logger.info("Rebalancing {} with {} partitions over {} clients", topic, topic.partitionIds().size(), lockGroup.getSubscriptions().size());

            // Get map where subscriptions SHOULD be assigned
            Map<LockListener, Set<PartitionId>> distribution = lockGroup.getDistributor().distribute(topic.partitionIds());
            distribution.forEach((lockListener, partitionIds) -> {
                partitionIds.forEach(partitionId -> {
                    LockListener currentSubscriber = lockGroup.subscriber(partitionId);
                    // Partition does not have an existing subscriber
                    if (currentSubscriber == null) {
                        toLock.computeIfAbsent(lockListener, s -> new HashSet<>()).add(partitionId);
                        lockGroup.getSubscriptions().get(lockListener).put(partitionId, false); // Set to subscribed, pending client ack
                    } else {
                        if (currentSubscriber != lockListener) {
                            if (lockGroup.getSubscriptions().get(currentSubscriber).get(partitionId)) { // The client marked as accepted, so send a release request
                                lockGroup.getSubscriptions().get(currentSubscriber).put(partitionId, false); // Set to pending
                                toRelease.computeIfAbsent(currentSubscriber, s -> new HashMap<>())
                                        .put(partitionId, lockListener);
                            }
                        }
                    }
                });
            });

        } finally {
            lock.writeLock().unlock();
        }

        toRelease.forEach((subscription, partitionIds) -> notifyReleasing(lockGroup, subscription, partitionIds));
        toLock.forEach((subscription, partitionIds) -> notifyLocked(lockGroup, subscription, partitionIds));
    }

    private void notifyReleasing(LockGroup lockGroup, LockListener lockListener, Map<PartitionId, LockListener> partitions) {
        logger.info("Releasing {} partitions for {}", partitions.size(), lockListener);
        partitions.forEach((partitionId, assignedListener) -> {
            lockListener.onLockState(partitionId, LockState.RELEASING).whenComplete((aVoid, throwable) -> {
                try {
                    lock.writeLock().lock();
                    lockGroup.getSubscriptions().get(lockListener).remove(partitionId);
                } finally {
                    lock.writeLock().unlock();
                }
                rebalance(lockGroup);
            });
        });
    }

    private void notifyLocked(LockGroup lockGroup, LockListener lockListener, Set<PartitionId> partitions) {
        logger.info("Locking {} partitions for {}", partitions.size(), lockListener);
        partitions.forEach(partitionId -> {
            lockListener.onLockState(partitionId, LockState.LOCKED).whenComplete((aVoid, throwable) -> {
                try {
                    lock.writeLock().lock();
                    lockGroup.getSubscriptions().get(lockListener).put(partitionId, true);
                } finally {
                    lock.writeLock().unlock();
                }
            });
        });
    }

    private static class LockGroup {
        public Distributor<LockListener, PartitionId> distributor = new ModuloDistributor<>();
        public Map<LockListener, Map<PartitionId, Boolean>> subscriptions = new HashMap<>();

        public void addListener(LockListener lockListener) {
            distributor.add(lockListener);
            subscriptions.putIfAbsent(lockListener, new HashMap<>());
        }

        public void removeListener(LockListener lockListener) {
            distributor.remove(lockListener);
            subscriptions.remove(lockListener);
        }


        public Map<LockListener, Map<PartitionId, Boolean>> getSubscriptions() {
            return subscriptions;
        }

        public Distributor<LockListener, PartitionId> getDistributor() {
            return distributor;
        }

        public LockListener subscriber(PartitionId partitionId) {
            for (Map.Entry<LockListener, Map<PartitionId, Boolean>> set : subscriptions.entrySet()) {
                if (set.getValue().containsKey(partitionId)) {
                    return set.getKey();
                }
            }
            return null;
        }
    }
}
