package io.hydramq.core.net.protocols.topic;

import io.hydramq.MessageSet;
import io.hydramq.core.net.Response;

/**
 * @author jfulton
 */
public class ReadResponse extends Response {

    private final MessageSet messageSet;

    public ReadResponse(int correlationId, MessageSet messageSet) {
        super(correlationId);
        this.messageSet = messageSet;
    }

    public MessageSet getMessageSet() {
        return messageSet;
    }
}
