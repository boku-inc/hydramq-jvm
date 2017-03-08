package io.hydramq.cases;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import io.hydramq.Message;
import io.hydramq.PersistenceTestsBase;
import io.hydramq.TopicManager;
import io.hydramq.client.ReplyCoordinator;
import io.hydramq.client.RequestCoordinator;
import io.hydramq.disk.DiskTopicManager;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import static io.hydramq.PropertyKeys.RequestReply.CLIENT_ID;
import static io.hydramq.PropertyKeys.RequestReply.CORRELATION_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author jfulton
 */
public class RequestCoordinationIT extends PersistenceTestsBase {

    private static final Logger logger = getLogger(RequestCoordinationIT.class);
    public static final String REQUESTS = "Requests";
    public static final String RESPONSES = "Responses";

    @Test
    public void testName() throws Exception {
        AtomicInteger requests1Count = new AtomicInteger();
        AtomicInteger requests2Count = new AtomicInteger();

        try (TopicManager disk = new DiskTopicManager(messageStoreDirectory())) {

            // This component goes into client that processes requests and supplies replies
            ReplyCoordinator replyCoordinator = new ReplyCoordinator(disk, REQUESTS);
            replyCoordinator.start(message -> Message.withBodyAsString("Ack - " + message.bodyAsString()).build());


            // This component goes into a synchronous client, is the source of requests, and expects replies
            RequestCoordinator requestCoordinator1 = new RequestCoordinator(disk, REQUESTS, RESPONSES);
            requestCoordinator1.start();


            // This component goes into a synchronous client, is the source of requests, and expects replies
            RequestCoordinator requestCoordinator2 = new RequestCoordinator(disk, REQUESTS, RESPONSES);
            requestCoordinator2.start();


            requestCoordinator1.sendForReply(Message.withBodyAsString("Request 1").build(), Duration.ofMillis(1000)).thenAccept(message -> {
                logger.info("Response: {}, LockId: {}, CorrelationId:{}", message.bodyAsString(), message.getString(CORRELATION_ID), message.getString(CLIENT_ID));
                requests1Count.incrementAndGet();
            }).exceptionally(throwable -> null).join();

            requestCoordinator2.sendForReply(Message.withBodyAsString("Request 2").build(), Duration.ofMillis(1000)).thenAccept(message -> {
                logger.info("Response: {}, LockId: {}, CorrelationId:{}", message.bodyAsString(), message.getString(CORRELATION_ID), message.getString(CLIENT_ID));
                requests2Count.incrementAndGet();
            }).join();

            requestCoordinator1.sendForReply(Message.withBodyAsString("Request 3").build(), Duration.ofMillis(1000)).thenAccept(message -> {
                logger.info("Response: {}, LockId: {}, CorrelationId:{}", message.bodyAsString(), message.getString(CORRELATION_ID), message.getString(CLIENT_ID));
                requests1Count.incrementAndGet();
            }).join();

            Thread.sleep(100);

            assertThat(requests1Count.get(), is(2));
            assertThat(requests2Count.get(), is(1));
        }
    }
}
