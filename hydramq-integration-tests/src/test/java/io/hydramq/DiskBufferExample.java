package io.hydramq;

import java.net.InetSocketAddress;
import java.nio.file.Paths;

import org.testng.annotations.Test;

/**
 * @author jfulton
 */
public class DiskBufferExample {

    @Test
    public void test() throws Exception {
        Topic diskTopic = TopicManagers.disk(Paths.get("/tmp/topics")).topic("Transactions");
        Topic networkTopic = TopicManagers.network(new InetSocketAddress("localhost", 9090)).topic("Transactions");

        TopicReaders.multicast(diskTopic).read((partitionId, messageOffset, message) -> {
            networkTopic.write(partitionId, message);
        });

        TopicWriter topicWriter = TopicWriters.simple(diskTopic);

        topicWriter.write(Message.empty().withString("TransactionId", "1234").build());
    }
}
