package io.hydramq;

import java.net.InetSocketAddress;

import io.hydramq.client.Router;

/**
 * @author jfulton
 */
public class RoutingExample extends PersistenceTestsBase {

    public void test() {
        TopicManager topicManager = TopicManagers.network(new InetSocketAddress("localhost", 9090));

        Topic mts = topicManager.topic("Example.Messages");

        mts.write(mts.partitionIds().first(), Message.withBodyAsString("Hello").build());

        Router router = new Router(topicManager, Partitioners.random(), message -> {
            return message.getString("MerchantId");
        });

        TopicReaders.locking(mts, "router").read((partitionId, messageSet) -> {
            for (Message message : messageSet) {
                router.write(message);
            }
        });
    }
}
