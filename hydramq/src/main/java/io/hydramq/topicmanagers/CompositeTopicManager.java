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

package io.hydramq.topicmanagers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.hydramq.Topic;
import io.hydramq.TopicManager;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.internal.apis.TopicManagerInternal;
import io.hydramq.internal.util.Assert;
import io.hydramq.listeners.DiscoverTopicsListener;
import io.hydramq.listeners.Listen;
import io.hydramq.listeners.TopicStateListener;
import io.hydramq.topics.CompositeTopic;

import static io.hydramq.listeners.Listen.CONTINUOUSLY;
import static io.hydramq.listeners.Listen.ONCE;

/**
 * @author jfulton
 */
public class CompositeTopicManager implements TopicManager, TopicManagerInternal {

    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Set<TopicManager> topicManagers = new HashSet<>();
    private Map<String, Set<TopicManager>> topicLocations = new HashMap<>();
    private Map<String, CompositeTopic> topics = new HashMap<>();
    private Set<DiscoverTopicsListener> discoverTopicsListeners = new HashSet<>();

    public CompositeTopicManager add(TopicManager topicManager) {
        if (topicManagers.add(topicManager)) {
            topicManager.discoverTopics(topicName -> {
                List<DiscoverTopicsListener> listeners = new ArrayList<>();
                discoverTopicsListeners.forEach(listeners::add);
                String notifyTopic = null;
                try {
                    lock.writeLock().lock();
                    if (!topicLocations.containsKey(topicName)) {
                        topicLocations.put(topicName, new HashSet<>());
                        notifyTopic = topicName;
                    }
                    if (topicLocations.get(topicName).add(topicManager)) {
                        if (topics.containsKey(topicName)) {
                            topics.get(topicName).add(topicManager.topic(topicName));
                        }
                    }
                } finally {
                    lock.writeLock().unlock();
                }
                if (notifyTopic != null) {
                    String finalNotifyTopic = notifyTopic;
                    listeners.forEach(listener -> listener.onTopicsDiscovered(finalNotifyTopic));
                }
            }, CONTINUOUSLY);
        }
        return this;
    }

    @Override
    public Topic topic(String topicName) throws HydraRuntimeException {
        try {
            lock.readLock().lock();
            String topicKey = topicName.toLowerCase();
            if (topicLocations.containsKey(topicKey)) {
                return topics.computeIfAbsent(topicName, s -> {
                    CompositeTopic topic = new CompositeTopic(topicKey);
                    for (TopicManager tm : topicLocations.get(topicKey)){
                        topic.add(tm.topic(topicName));
                    }
                    return topic;
                });
            }
        } finally {
            lock.readLock().unlock();
        }
        throw new HydraRuntimeException("Topic not found: " + topicName);
    }

    @Override
    public void discoverTopics(DiscoverTopicsListener listener) {
        discoverTopics(listener, ONCE);
    }

    @Override
    public void discoverTopics(DiscoverTopicsListener listener, Listen listen) {
        List<String> results = new ArrayList<>();
        try {
            lock.writeLock().lock();
            if (listen == ONCE || listen == CONTINUOUSLY) {
                if (listen == CONTINUOUSLY) {
                    discoverTopicsListeners.add(listener);
                }
                topicLocations.keySet().forEach(results::add);
            } else {
                discoverTopicsListeners.remove(listener);
            }
        } finally {
            lock.writeLock().unlock();
        }
        results.forEach(listener::onTopicsDiscovered);
    }

    @Override
    public void discoverTopics(DiscoverTopicsListener listener, Listen listen, Set<String> knownTopicNames) {
        Assert.argumentNotNull(listener, "listener");
        Assert.argumentNotNull(listen, "listen");
        List<String> results = new ArrayList<>();
        try {
            lock.writeLock().lock();
            if (listen == Listen.ONCE || listen == Listen.CONTINUOUSLY) {
                if (listen == Listen.CONTINUOUSLY) {
                    discoverTopicsListeners.add(listener);
                }
                topicLocations.keySet().forEach(topicName -> {
                    if (!knownTopicNames.contains(topicName)) {
                        results.add(topicName);
                    }
                });
            } else if (listen == Listen.REMOVE) {
                discoverTopicsListeners.remove(listener);
            }
        } finally {
            lock.writeLock().unlock();
        }
        results.forEach(listener::onTopicsDiscovered);
    }

    @Override
    public void topicStatuses(TopicStateListener listener) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void topicStatuses(TopicStateListener listener, Listen status) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void close() throws IOException {
        // Ignore
    }
}
