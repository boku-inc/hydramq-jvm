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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import io.hydramq.Topic;
import io.hydramq.TopicManager;
import io.hydramq.TopicManagers;
import io.hydramq.disk.PersistenceTestsBase;
import io.hydramq.listeners.Listen;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class CompositeTopicManagerTest extends PersistenceTestsBase {

    @Test
    public void testDiscoverExistingTopics() throws Exception {
        TopicManager tp1 = TopicManagers.disk(messageStoreDirectory());
        TopicManager tp2 = TopicManagers.disk(messageStoreDirectory());

        tp1.topic("foo");

        TopicManager composite = TopicManagers.composite(tp1, tp2);
        List<String> discoveredTopics = new ArrayList<>();
        composite.discoverTopics(discoveredTopics::add);
        assertThat(discoveredTopics, contains("foo"));
    }

    @Test
    public void testDiscoverTopicsContinuously() throws Exception {
        TopicManager tp1 = TopicManagers.disk(messageStoreDirectory());
        TopicManager tp2 = TopicManagers.disk(messageStoreDirectory());

        TopicManager composite = TopicManagers.composite(tp1, tp2);
        List<String> discoveredTopics = new ArrayList<>();
        composite.discoverTopics(discoveredTopics::add, Listen.CONTINUOUSLY);

        tp1.topic("blah");
        tp2.topic("blah");

        assertThat(discoveredTopics, contains("blah"));
    }

    @Test
    public void testGetCompositeTopicOnce() throws Exception {
        TopicManager tm1 = TopicManagers.disk(messageStoreDirectory());
        TopicManager tm2 = TopicManagers.disk(messageStoreDirectory());

        TopicManager composite = TopicManagers.composite(tm1, tm2);

        Topic t1 = tm1.topic("blah");
        Topic t2 = tm2.topic("blah");

        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        composite.discoverTopics(topicName -> {
            assertThat(composite.topic(topicName).readablePartitions(), is(t1.readablePartitions() + t2.readablePartitions()));
            listenerCalled.set(true);
        });

        assertThat(listenerCalled.get(), is(true));
    }


    @Test
    public void testGetCompositeTopicContinuously() throws Exception {
        TopicManager tm1 = TopicManagers.disk(messageStoreDirectory());
        TopicManager tm2 = TopicManagers.disk(messageStoreDirectory());
        TopicManager tm3 = TopicManagers.disk(messageStoreDirectory());

        TopicManager composite = TopicManagers.composite(tm1, tm2, tm3);

        Topic t1 = tm1.topic("blah");
        Topic t2 = tm2.topic("blah");

        assertThat(composite.topic("blah").readablePartitions(), is(t1.readablePartitions() + t2.readablePartitions()));

        Topic t3 = tm3.topic("blah");

        assertThat(composite.topic("blah").readablePartitions(),
                is(t1.readablePartitions() + t2.readablePartitions() + t3.readablePartitions()));
    }
}