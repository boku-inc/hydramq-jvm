package io.hydramq.core.net.protocols.topic;

import java.util.UUID;

import io.hydramq.core.net.Request;

/**
 * @author jfulton
 */
public class LockListenerRequest extends Request {

    private final UUID clientKey;
    private final String lockGroup;
    private final boolean subscribe;

    public LockListenerRequest(int correlationId, UUID clientKey, String lockGroup, boolean subscribe) {
        super(correlationId);
        this.clientKey = clientKey;
        this.lockGroup = lockGroup;
        this.subscribe = subscribe;
    }

    public LockListenerRequest(UUID clientKey, String lockGroup, boolean subscribe) {
        this.clientKey = clientKey;
        this.lockGroup = lockGroup;
        this.subscribe = subscribe;
    }

    public UUID getClientKey() {
        return clientKey;
    }

    public String getLockGroup() {
        return lockGroup;
    }

    public boolean isRegistering() {
        return subscribe;
    }
}
