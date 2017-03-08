package io.hydramq.core.net.protocols.topicmanager;

import io.hydramq.core.net.Request;

/**
 * @author jfulton
 */
public class TopicDiscoveredNotification extends Request {

    private String topicName;

    public TopicDiscoveredNotification(String topicName) {
        this.topicName = topicName;
    }

    public TopicDiscoveredNotification(int correlationId, final String topicName) {
        super(correlationId);
        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }
}
