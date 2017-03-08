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

package io.hydramq.readers;

import java.util.concurrent.atomic.AtomicBoolean;

import io.hydramq.Message;
import io.hydramq.Topic;
import io.hydramq.TopicManager;
import io.hydramq.TopicManagers;
import io.hydramq.TopicReaders;
import io.hydramq.TopicWriter;
import io.hydramq.TopicWriters;
import io.hydramq.disk.PersistenceTestsBase;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class MulticastTopicReaderTest extends PersistenceTestsBase {

    @Test
    public void testName() throws Exception {
        TopicManager tm = TopicManagers.disk(messageStoreDirectory());

        Topic foo = tm.topic("Foo");

        AtomicBoolean called = new AtomicBoolean(false);
        TopicReaders.multicast(foo).read((partitionId, messageOffset, messageSet) -> {
            called.set(true);
        });

        TopicWriter writer = TopicWriters.simple(foo);
        writer.write(Message.empty().build());
        MulticastTopicReader multicastTopicReader = new MulticastTopicReader(foo);
        multicastTopicReader.read((partitionId, messageOffset, message) -> {
            called.set(true);
        });
        Thread.sleep(1000);

        assertThat(called.get(), is(true));

    }
}