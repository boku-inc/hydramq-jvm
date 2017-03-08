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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import io.hydramq.TopicManager;
import io.hydramq.internal.apis.TopicManagerInternal;
import io.hydramq.listeners.DiscoverTopicsListener;
import io.hydramq.listeners.Listen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static io.hydramq.listeners.Listen.CONTINUOUSLY;
import static io.hydramq.listeners.Listen.ONCE;
import static io.hydramq.listeners.Listen.REMOVE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
@SuppressWarnings("Duplicates")
public class DiskTopicManagerTest extends PersistenceTestsBase {

    private static final Logger logger = LoggerFactory.getLogger(DiskTopicManagerTest.class);

    @Test
    public void testTopicsDiscoveredOnce() throws Exception {
        AtomicInteger topicsDiscoveredCount = new AtomicInteger();
        TopicManager topicManager = new DiskTopicManager(messageStoreDirectory());
        topicManager.topic("topic1");
        List<String> discoveredTopics = new ArrayList<>();
        topicManager.discoverTopics((e) -> {
            discoveredTopics.add(e);
            topicsDiscoveredCount.incrementAndGet();
        }, Listen.ONCE);
        assertThat(discoveredTopics, contains("topic1"));
        topicManager.topic("topic2");
        assertThat(discoveredTopics.size(), is(1));
    }

    @Test
    public void testTopicsDiscoveredContinuous() throws Exception {
        TopicManager topicManager = new DiskTopicManager(messageStoreDirectory());
        List<String> discoveredTopics = new ArrayList<>();
        topicManager.topic("topic1");
        topicManager.discoverTopics(discoveredTopics::add, CONTINUOUSLY);
        topicManager.topic("topic2");
        topicManager.topic("topic3");
        assertThat(discoveredTopics, contains("topic1", "topic2", "topic3"));

    }

    @Test
    public void testOnTopicDiscoveredDuplicate() throws Exception {
        AtomicInteger discoveryCount = new AtomicInteger();
        TopicManager topicManager = new DiskTopicManager(messageStoreDirectory());
        topicManager.discoverTopics(topicName -> discoveryCount.incrementAndGet(), CONTINUOUSLY);
        topicManager.topic("topic4");
        topicManager.topic("topic4");
        topicManager.topic("Topic4");
        assertThat(discoveryCount.get(), is(1));
    }

    @Test
    public void testTopicDiscoveryRemove() throws Exception {
        AtomicInteger discoveryCount = new AtomicInteger();
        TopicManager topicManager = new DiskTopicManager(messageStoreDirectory());
        DiscoverTopicsListener discoveryListener = topicName -> discoveryCount.incrementAndGet();
        topicManager.discoverTopics(discoveryListener, CONTINUOUSLY);
        topicManager.topic("topic5");
        assertThat(discoveryCount.get(), is(1));
        topicManager.discoverTopics(discoveryListener, Listen.REMOVE);
        topicManager.topic("topic6");
        assertThat(discoveryCount.get(), is(1));
        discoveryCount.set(0);
        topicManager.discoverTopics(discoveryListener);
        assertThat(discoveryCount.get(), is(2));
    }

    @Test
    public void testTopicDiscoveryDefaultStatus() throws Exception {
        // Listen.ONCE is the current default, so test that behavior exists
        TopicManager topicManager = new DiskTopicManager(messageStoreDirectory());
        topicManager.topic("topic1");
        List<String> discoveredTopics = new ArrayList<>();
        topicManager.discoverTopics(discoveredTopics::add);
        assertThat(discoveredTopics, contains("topic1"));
        topicManager.topic("topic2");
        assertThat(discoveredTopics.size(), is(1));
    }

    @Test
    public void testDiscoverTopicsInternalContinuouslyAndStop() throws Exception {
        TopicManager topicManager = new DiskTopicManager(messageStoreDirectory());
        topicManager.topic("topic1");
        topicManager.topic("topic2");

        Set<String> knownTopics = new HashSet<>();
        knownTopics.add("topic1");
        knownTopics.add("topic2");

        Set<String> discoveredTopics = new HashSet<>();
        DiscoverTopicsListener listener = discoveredTopics::add;
        ((TopicManagerInternal) topicManager).discoverTopics(listener, CONTINUOUSLY, knownTopics);

        assertThat(discoveredTopics.size(), is(0));

        topicManager.topic("topic3");
        topicManager.topic("topic4");
        assertThat(discoveredTopics.size(), is(2));
        assertThat(discoveredTopics, contains("topic3", "topic4"));

        ((TopicManagerInternal) topicManager).discoverTopics(listener, REMOVE, knownTopics);
        topicManager.topic("topic5");
        assertThat(discoveredTopics.size(), is(2));

        discoveredTopics.clear();
        ((TopicManagerInternal) topicManager).discoverTopics(listener, CONTINUOUSLY, knownTopics);
        assertThat(discoveredTopics.size(), is(3));
        assertThat(discoveredTopics, containsInAnyOrder("topic3", "topic4", "topic5"));

        topicManager.discoverTopics(listener, REMOVE);
        topicManager.topic("topic6");
        assertThat(discoveredTopics.size(), is(3));
        assertThat(discoveredTopics, containsInAnyOrder("topic3", "topic4", "topic5"));
    }

    @Test
    public void testDiscoverTopicsInternalOnce() throws Exception {
        TopicManager topicManager = new DiskTopicManager(messageStoreDirectory());
        topicManager.topic("topic1");
        topicManager.topic("topic2");
        topicManager.topic("topic3");

        Set<String> knownTopics = new HashSet<>();
        knownTopics.add("topic1");
        knownTopics.add("topic2");

        Set<String> discoveredTopics = new HashSet<>();
        ((TopicManagerInternal) topicManager).discoverTopics(discoveredTopics::add, ONCE, knownTopics);

        assertThat(discoveredTopics.size(), is(1));
        assertThat(discoveredTopics, contains("topic3"));

        topicManager.topic("topic4");
        assertThat(discoveredTopics.size(), is(1));
    }
}