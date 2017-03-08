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

package io.hydramq.disk;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

/**
 * @author jfulton
 */
public class DiskTopicManager implements TopicManager, TopicManagerInternal {

    private ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    private final Path storeDirectory;

    private final DiskTopicBuilder diskTopicBuilder;
    private final TopicCreationStrategy topicCreationStrategy;
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Set<DiscoverTopicsListener> topicsDiscoveredListeners = new HashSet<>();

    public DiskTopicManager(final Path storeDirectory, final DiskTopicBuilder diskTopicBuilder,
                            final TopicCreationStrategy topicCreationStrategy) throws HydraRuntimeException {
        this.storeDirectory = storeDirectory;
        this.diskTopicBuilder = diskTopicBuilder;
        this.topicCreationStrategy = topicCreationStrategy;
        try {
            Files.createDirectories(storeDirectory);
        } catch (IOException e) {
            throw new HydraRuntimeException("Error creating TopicManager directory at " + storeDirectory);
        }
        loadTopics(storeDirectory, topics);
    }

    public DiskTopicManager(Path storeDirectory, DiskTopicBuilder diskTopicBuilder) {
        this(storeDirectory, diskTopicBuilder, new DefaultTopicCreationStrategy(true));
    }

    public DiskTopicManager(Path storeDirectory) {
        this(storeDirectory, new DiskTopicBuilder());
    }

    public Topic topic(String topicName) throws HydraRuntimeException {
        Assert.argumentNotNull(topicName, "topicName");
        String topicKey = topicName.toLowerCase();
        if (topics.containsKey(topicKey)) {
            return topics.get(topicKey);
        } else {
            if (topicCreationStrategy.createTopicsOnDemand()) {
                try {
                    lock.writeLock().lock();
                    // Ensure a topicProtocol hasn't been created by a different thread
                    if (!topics.containsKey(topicKey)) {
                        Topic topic = diskTopicBuilder.build(storeDirectory.resolve(topicName));
                        topics.put(topicKey, topic);
                        topicsDiscoveredListeners.forEach(listener -> listener.onTopicsDiscovered(topicKey));
                    }
                    return topics.get(topicKey);
                } finally {
                    lock.writeLock().unlock();
                }
            } else {
                throw new HydraRuntimeException("'" + topicName + "' does not exist.  It must be created before use.");
            }
        }
    }

    @Override
    public void discoverTopics(DiscoverTopicsListener listener) {
        discoverTopics(listener, Listen.ONCE);
    }

    @Override
    public void discoverTopics(DiscoverTopicsListener listener, Listen listen) {
        Assert.argumentNotNull(listener, "listener");
        Assert.argumentNotNull(listen, "listen");
        List<String> results = new ArrayList<>();
        try {
            lock.writeLock().lock();
            if (listen == Listen.ONCE || listen == Listen.CONTINUOUSLY) {
                if (listen == Listen.CONTINUOUSLY) {
                    topicsDiscoveredListeners.add(listener);
                }
                topics.keySet().forEach(results::add);
            } else if (listen == Listen.REMOVE) {
                topicsDiscoveredListeners.remove(listener);
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
                    topicsDiscoveredListeners.add(listener);
                }
                topics.keySet().forEach(topicName -> {
                    if (!knownTopicNames.contains(topicName)) {
                        results.add(topicName);
                    }
                });
            } else if (listen == Listen.REMOVE) {
                topicsDiscoveredListeners.remove(listener);
            }
        } finally {
            lock.writeLock().unlock();
        }
        results.forEach(listener::onTopicsDiscovered);
    }

    @Override
    public void topicStatuses(TopicStateListener listener) {

    }

    @Override
    public void topicStatuses(TopicStateListener listener, Listen status) {

    }

    @Override
    public void close() throws IOException {
        for (Topic topic : topics.values()) {
            topic.close();
        }
    }

    public void loadTopics(final Path baseDirectory, final Map<String, Topic> topics) throws HydraRuntimeException {
        try {
            Files.walkFileTree(baseDirectory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs)
                        throws IOException {
                    if (baseDirectory == dir) {
                        return FileVisitResult.CONTINUE;
                    }
                    try {
                        String topicKey = dir.getFileName().toString().toLowerCase();
                        Topic topic = diskTopicBuilder.build(dir);
                        topics.put(topicKey, topic);
                    } catch (HydraRuntimeException e) {
                        if (e.getCause() instanceof IOException) {
                            throw (IOException)e.getCause();
                        } else {
                            throw new HydraRuntimeException("Unexpected HydraRuntimeException", e);
                        }
                    }
                    return FileVisitResult.SKIP_SUBTREE;
                }
            });

        } catch (IOException e) {
            throw new HydraRuntimeException("Error loading segments contained in " + baseDirectory.toString(), e);
        }
    }
}
