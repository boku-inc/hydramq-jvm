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

package io.hydramq.topics;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;

import io.hydramq.CursorInfo;
import io.hydramq.Message;
import io.hydramq.MessageSet;
import io.hydramq.PartitionId;
import io.hydramq.PartitionInfo;
import io.hydramq.Topic;
import io.hydramq.common.AbstractTopic;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.listeners.Listen;
import io.hydramq.listeners.PartitionListener;
import io.hydramq.subscriptions.LockListener;

/**
 * @author jfulton
 */
public class TopicWrapper extends AbstractTopic implements Topic {

    private final Topic wrapped;

    public TopicWrapper(Topic wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public String getName() {
        return wrapped.getName();
    }

    @Override
    public CompletableFuture<Void> write(PartitionId partitionId, Message message) {
        return wrapped.write(partitionId, message);
    }

    @Override
    public CompletableFuture<Void> write(PartitionId partitionId, MessageSet messageSet) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<Void> write(Map<PartitionId, List<Message>> messageBatch) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<MessageSet> read(PartitionId partitionId, long messageOffset, int maxMessages) {
        return wrapped.read(partitionId, messageOffset, maxMessages);
    }

    @Override
    public void discoverPartitions(PartitionListener listener) {
        wrapped.discoverPartitions(listener);
    }

    @Override
    public void discoverPartitions(PartitionListener listener, Listen listen) {
        wrapped.discoverPartitions(listener, listen);
    }

    @Override
    public CompletableFuture<PartitionInfo> partitionInfo(PartitionId partitionId) {
        return wrapped.partitionInfo(partitionId);
    }

    @Override
    public SortedSet<PartitionId> partitionIds() {
        return wrapped.partitionIds();
    }

    @Override
    public void acquirePartitionLocks(String lockGroup, LockListener lockListener) {
        wrapped.acquirePartitionLocks(lockGroup, lockListener);
    }

    @Override
    public void acquirePartitionLocks(String lockGroup, LockListener lockListener, Listen listen) {
        wrapped.acquirePartitionLocks(lockGroup, lockListener, listen);
    }

    @Override
    public CompletableFuture<CursorInfo> cursor(PartitionId partitionId, String cursorName) {
        return wrapped.cursor(partitionId, cursorName);
    }

    @Override
    public CompletableFuture<Void> cursor(PartitionId partitionId, String cursorName, long messageOffset) {
        return wrapped.cursor(partitionId, cursorName, messageOffset);
    }

    @Override
    public int partitions() {
        return wrapped.partitions();
    }

    @Override
    public void partitions(int targetPartitionCount) throws HydraRuntimeException {
        wrapped.partitions(targetPartitionCount);
    }

    @Override
    public int writablePartitions() {
        return wrapped.writablePartitions();
    }

    @Override
    public void writablePartitions(int targetWritablePartitionCount) {
        wrapped.writablePartitions(targetWritablePartitionCount);
    }

    @Override
    public int readablePartitions() {
        return wrapped.readablePartitions();
    }

    @Override
    public void readablePartitions(int targetReadablePartitionCount) {
        wrapped.readablePartitions(targetReadablePartitionCount);
    }

    @Override
    public void close() throws IOException {
        wrapped.close();
    }

    @Override
    public int hashCode() {
        return wrapped.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TopicWrapper) {
            return getWrapped().equals(((TopicWrapper) obj).getWrapped());
        } else {
            return wrapped.equals(obj);
        }
    }

    public Topic getWrapped() {
        return wrapped;
    }

    @Override
    public CompletableFuture<Void> closeFuture() {
        return wrapped.closeFuture();
    }
}
