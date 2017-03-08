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

import java.net.InetSocketAddress;
import java.nio.file.Path;

import io.hydramq.disk.DiskTopicManager;
import io.hydramq.network.NetworkTopicManager;
import io.hydramq.topicmanagers.CompositeTopicManager;

/**
 * @author jfulton
 */
public class TopicManagers {

    public static TopicManager network(InetSocketAddress endpoint) {
        NetworkTopicManager topicManager = new NetworkTopicManager();
        topicManager.connect(endpoint).join();
        return topicManager;
    }

    public static TopicManager disk(Path topicsDirectory) {
        return new DiskTopicManager(topicsDirectory);
    }

    public static TopicManager composite(TopicManager topicManager, TopicManager... topicManagers) {
        CompositeTopicManager composite = new CompositeTopicManager();
        composite.add(topicManager);
        for (TopicManager tm : topicManagers) {
            composite.add(tm);
        }
        return composite;
    }
}
