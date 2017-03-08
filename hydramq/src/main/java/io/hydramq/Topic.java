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

package io.hydramq;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;

import io.hydramq.listeners.Listen;
import io.hydramq.listeners.PartitionListener;
import io.hydramq.subscriptions.LockListener;

/**
 * @author jfulton
 */
public interface Topic extends Closeable, ClosingAware {

    String getName();

    CompletableFuture<Void> write(PartitionId partitionId, Message message);

    CompletableFuture<Void> write(PartitionId partitionId, MessageSet messageSet);

    CompletableFuture<Void> write(Map<PartitionId, List<Message>> messageBatch);

    CompletableFuture<MessageSet> read(PartitionId partitionId, long messageOffset, int maxMessages);

    void discoverPartitions(PartitionListener listener);

    void discoverPartitions(PartitionListener listener, Listen listen);

    void acquirePartitionLocks(String lockGroup, LockListener lockListener);

    void acquirePartitionLocks(String lockGroup, LockListener lockListener, Listen listen);

    CompletableFuture<CursorInfo> cursor(PartitionId partitionId, String cursorName);

    CompletableFuture<Void> cursor(PartitionId partitionId, String cursorName, long messageOffset);

    CompletableFuture<PartitionInfo> partitionInfo(PartitionId partitionId);

    SortedSet<PartitionId> partitionIds();

    int partitions();

    void partitions(int targetPartitionCount);

    int writablePartitions();

    void writablePartitions(int targetWritablePartitionCount);

    int readablePartitions();

    void readablePartitions(int targetReadablePartitionCount);
}
