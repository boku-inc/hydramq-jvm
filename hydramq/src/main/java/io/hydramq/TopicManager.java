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

import java.io.Closeable;

import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.listeners.DiscoverTopicsListener;
import io.hydramq.listeners.Listen;
import io.hydramq.listeners.TopicStateListener;

/**
 * @author jfulton
 */
public interface TopicManager extends Closeable {

    Topic topic(String topicName) throws HydraRuntimeException;

    void discoverTopics(DiscoverTopicsListener listener);

    void discoverTopics(DiscoverTopicsListener listener, Listen listen);

    void topicStatuses(TopicStateListener listener);

    void topicStatuses(TopicStateListener listener, Listen status);

}
