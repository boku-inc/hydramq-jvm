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

package io.hydramq.network;

import java.net.InetSocketAddress;

import io.hydramq.HydraServer;
import io.hydramq.Message;
import io.hydramq.PartitionId;
import io.hydramq.PartitionInfo;
import io.hydramq.TopicManager;
import io.hydramq.disk.DiskTopicManager;
import io.hydramq.disk.PersistenceTestsBase;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class NetworkPartitionTest extends PersistenceTestsBase {

    @Test
    public void testPartitionInfo() throws Exception {
        TopicManager topicManager = new DiskTopicManager(messageStoreDirectory());
        HydraServer hydraServer = new HydraServer(topicManager, 0);
        Integer port = hydraServer.start().join();
        InetSocketAddress endpoint = new InetSocketAddress("localhost", port);
        try {
            NetworkTopic topic = new NetworkTopic("foo");
            topic.connect(endpoint).join();
            PartitionId partitionId = getPartitionId(topic);
            PartitionInfo partitionInfo = topic.partitionInfo(partitionId).get();
            assertThat(partitionInfo.head(), is(0L));
            assertThat(partitionInfo.tail(), is(0L));

            topic.write(partitionId, Message.empty().build()).join();
            partitionInfo = topic.partitionInfo(partitionId).get();
            assertThat(partitionInfo.head(), is(0L));
            assertThat(partitionInfo.tail(), is(1L));

            partitionInfo = topic.partitionInfo(getPartitionId(topic, 1)).get();
            assertThat(partitionInfo.head(), is(0L));
            assertThat(partitionInfo.tail(), is(0L));
        } finally {
            hydraServer.stop().join();
        }
    }
}