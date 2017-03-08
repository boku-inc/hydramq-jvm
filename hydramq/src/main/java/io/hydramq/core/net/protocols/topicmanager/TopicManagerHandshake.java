package io.hydramq.core.net.protocols.topicmanager;

import java.util.Set;

import io.hydramq.core.net.Handshake;

/**
 * @author jfulton
 */
public class TopicManagerHandshake extends Handshake {

    private final int version;
    private final Set<String> topicNames;

    public TopicManagerHandshake(final int version, Set<String> topicNames) {
        this.version = version;
        this.topicNames = topicNames;
    }

    public TopicManagerHandshake(int correlationId, int version, Set<String> topicNames) {
        super(correlationId);
        this.version = version;
        this.topicNames = topicNames;
    }

    public int getVersion() {
        return version;
    }

    public TopicManagerHandshake reply(int version, Set<String> topicNames) {
        return new TopicManagerHandshake(correlationId(), version, topicNames);
    }

    public Set<String> getTopicNames() {
        return topicNames;
    }
}
