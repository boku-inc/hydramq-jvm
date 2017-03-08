package io.hydramq;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.testng.annotations.Test;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
public class ExamplesIT extends PersistenceTestsBase {

    private static final Logger logger = getLogger(ExamplesIT.class);

    /**
     * Serves as an example of using the high-level features.  This code currently doesn't handle management of
     * start/stop/closing as appropriate, and is just an example.
     */
    @Test
    public void testName() throws Exception {
        Message message = Message.empty()
                .withString("firstName", "Ramesh")
                .withInteger("age", 28)
                .withString("clientId", "Facebook")
                .build();

        TopicManager diskTM = TopicManagers.disk(messageStoreDirectory());

        Topic mts = diskTM.topic("Example.Messages");

        TopicWriter writer = TopicWriters.simple(mts, Partitioners.hashedOn("clientId"));
        writer.write(message);

        HydraServer server = new HydraServer(diskTM, 7070);
        server.start().join();

        TopicManager networkTM = TopicManagers.network(new InetSocketAddress("localhost", 7070));


    }
}
