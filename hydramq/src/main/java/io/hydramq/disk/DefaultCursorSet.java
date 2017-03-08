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

package io.hydramq.disk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.hydramq.Cursor;
import io.hydramq.CursorSet;
import io.hydramq.PartitionId;
import io.hydramq.disk.flushing.FlushStrategies;
import io.hydramq.disk.flushing.FlushStrategy;
import io.hydramq.exceptions.HydraRuntimeException;
import it.unimi.dsi.fastutil.objects.Object2LongLinkedOpenHashMap;

/**
 * @author jfulton
 */
public class DefaultCursorSet implements CursorSet {

    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private static final int CURSOR_KEY_SIZE = 16;
    private static final int CURSOR_VALUE_SIZE = 8;
    private FileChannel cursorSetData;
    private Path cursorSetFile;
    private final FlushStrategy flushStrategy;
    private final Object2LongLinkedOpenHashMap<PartitionId> cursors = new Object2LongLinkedOpenHashMap<>();

    public DefaultCursorSet(final Path cursorSetFile, final FlushStrategy flushStrategy) {
        this.cursorSetFile = cursorSetFile;
        this.flushStrategy = flushStrategy;
        try {
            cursorSetData = FileChannel.open(cursorSetFile, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            if (!(cursorSetData.size() % (CURSOR_KEY_SIZE + CURSOR_VALUE_SIZE) == 0)) {
                throw new HydraRuntimeException("Invalid sized CursorSet at " + cursorSetFile);
            }
            load();
        } catch (IOException e) {
            throw new HydraRuntimeException("Error creating CursorSet at " + cursorSetFile, e);
        }
    }

    public DefaultCursorSet(Path cursorSetFile) {
        this(cursorSetFile, FlushStrategies.standard());
    }

    @Override
    public void set(PartitionId partitionId, long messageOffset) {
        try {
            lock.writeLock().lock();
            cursors.put(partitionId, messageOffset);
            write();
        } catch (IOException e) {
            throw new HydraRuntimeException("Error writing cursor files at " + cursorSetFile, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long get(PartitionId partitionId) {
        try {
            lock.readLock().lock();
            return cursors.getLong(partitionId);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Cursor forPartition(PartitionId partitionId) {
        return new DefaultCursor(this, partitionId);
    }

    @Override
    public boolean hasCursor(PartitionId partitionId) {
        try {
            lock.readLock().lock();
            return cursors.containsKey(partitionId);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int size() {
        return cursors.size();
    }

    @Override
    public void close() throws IOException {
        cursorSetData.close();
    }

    private void load() throws IOException {
        int readPosition = 0;
        for (int i = 0; i < cursorSetData.size() / (CURSOR_KEY_SIZE + CURSOR_VALUE_SIZE); i++) {
            ByteBuffer buffer = ByteBuffer.allocate(CURSOR_KEY_SIZE + CURSOR_VALUE_SIZE);
            cursorSetData.read(buffer, readPosition);
            buffer.flip();
            PartitionId partitionId = PartitionId.create(new UUID(buffer.getLong(), buffer.getLong()));
            long offset = buffer.getLong();
            cursors.put(partitionId, offset);
            readPosition += CURSOR_KEY_SIZE + CURSOR_VALUE_SIZE;
        }
    }

    private void write() throws IOException {
        cursorSetData.truncate(0);
        for (Map.Entry<PartitionId, Long> cursor : cursors.entrySet()) {
            ByteBuffer buffer = ByteBuffer.allocate(CURSOR_KEY_SIZE + CURSOR_VALUE_SIZE);
            buffer.putLong(cursor.getKey().getUUID().getMostSignificantBits())
                    .putLong(cursor.getKey().getUUID().getLeastSignificantBits())
                    .putLong(cursor.getValue());
            buffer.flip();
            cursorSetData.write(buffer);
            if (flushStrategy.requiresFlush(CURSOR_KEY_SIZE + CURSOR_VALUE_SIZE)) {
                cursorSetData.force(true);
            }
        }
    }
}
