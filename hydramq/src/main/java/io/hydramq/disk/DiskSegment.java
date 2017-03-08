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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;

import io.hydramq.Message;
import io.hydramq.MessageProperties;
import io.hydramq.MessageSet;
import io.hydramq.Segment;
import io.hydramq.core.type.ConversionContext;
import io.hydramq.core.type.converters.MessageConverter;
import io.hydramq.core.type.converters.MessagePropertiesConverter;
import io.hydramq.disk.flushing.FlushStrategy;
import io.hydramq.disk.flushing.IntervalThresholdFlushStrategy;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.internal.util.DiskUtils;
import io.hydramq.internal.util.Throwables;
import io.hydramq.listeners.MessageIOListener;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

/**
 * @author jfulton
 */
// TODO: Make this class thread safe(ish), with performance testing?
public class DiskSegment implements Segment {

    public static final int INDEX_ENTRY_SIZE = 12;
    private int size = 0;
    private final Path segmentDirectory;
    private final FlushStrategy flushStrategy;
    private static PooledByteBufAllocator allocator = new PooledByteBufAllocator();
    private ByteBuffer indexWriteBuffer = ByteBuffer.allocateDirect(INDEX_ENTRY_SIZE);
    private ByteBuffer indexReadBuffer = ByteBuffer.allocateDirect(INDEX_ENTRY_SIZE);
    private FileChannel index;
    private FileChannel data;
    private ConversionContext conversionContext = ConversionContext.base()
                                                                   .register(Message.class, new MessageConverter())
                                                                   .register(MessageProperties.class,
                                                                           new MessagePropertiesConverter());

    public DiskSegment(final Path segmentDirectory) throws HydraRuntimeException {
        this(segmentDirectory, new IntervalThresholdFlushStrategy());
    }

    public DiskSegment(final Path segmentDirectory, final FlushStrategy flushStrategy) throws HydraRuntimeException {
        this.segmentDirectory = segmentDirectory;
        try {
            Files.createDirectories(this.segmentDirectory);
        } catch (IOException e) {
            throw new HydraRuntimeException(
                    "Error creating container directory for segment" + this.segmentDirectory.toString(), e);
        }
        try {
            this.index = FileChannel.open(this.segmentDirectory.resolve("segment.idx"), StandardOpenOption.READ,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            this.index.position(index.size());
            this.data = FileChannel.open(this.segmentDirectory.resolve("segment.dat"), StandardOpenOption.READ,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            this.data.position(data.size());
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        this.flushStrategy = flushStrategy;
        try {
            this.size = (int) (index.position() / INDEX_ENTRY_SIZE);
        } catch (IOException e) {
            throw new HydraRuntimeException("Error creating segment", e);
        }
    }

    @Override
    public int size() {
        return size;
    }


    @Override
    public void read(final int messageOffset, final int maxMessages, final MessageSet messages) throws HydraRuntimeException {
        int sizeSnapshot = size();
        if (!(messageOffset < sizeSnapshot)) {
            return;
        }

        for (int i = messageOffset; i < Math.min(sizeSnapshot, messageOffset + maxMessages); i++) {
            messages.add(read(i));
        }
    }

    @Override
    public void delete() throws IOException {
        close();
        DiskUtils.deleteDirectory(segmentDirectory);
    }

    public Message read(final int messageOffset) throws HydraRuntimeException {
        if (!(messageOffset < size())) {
            throw outOfBoundsException();
        }
        try {
            // First, use the index to find the starting byte of the message in the data file
            ByteBuffer indexReadBuffer = ByteBuffer.allocate(4);
            indexReadBuffer.clear();
            while (indexReadBuffer.hasRemaining()) {
                index.read(indexReadBuffer, messageOffset * INDEX_ENTRY_SIZE + indexReadBuffer.position());
            }
            indexReadBuffer.flip();
            int dataOffset = indexReadBuffer.getInt();
            // Second, get the message size from the first integer of the message
            indexReadBuffer.clear();
            while (indexReadBuffer.hasRemaining()) {
                data.read(indexReadBuffer, dataOffset + indexReadBuffer.position());
            }
            indexReadBuffer.flip();
            int dataSize = indexReadBuffer.getInt();
            ByteBuffer buffer = ByteBuffer.allocateDirect(dataSize);
            while (buffer.hasRemaining()) {
                data.read(buffer, dataOffset + 4 + buffer.position());
            }
            buffer.flip();
            return conversionContext.read(Message.class, Unpooled.wrappedBuffer(buffer));
        } catch (Exception ex) {
            throw new HydraRuntimeException(
                    "Error reading message in segment " + segmentDirectory.toString() + " for message offset " +
                    messageOffset, ex);
        }
    }

    private HydraRuntimeException outOfBoundsException() {
        return new HydraRuntimeException("An attempt was made to read past the segment size");
    }

    public void write(Message message, MessageIOListener messageIOListener) throws HydraRuntimeException {
        try {
            ByteBuf buffer = allocator.directBuffer();
            buffer.writeInt(0);
            conversionContext.write(message, buffer);
            int messageSize = buffer.readableBytes() - 4;
            buffer.setInt(0, messageSize);
            boolean shouldFlush = flushStrategy.requiresFlush(buffer.readableBytes());
            indexWriteBuffer.clear();
            indexWriteBuffer.putInt((int) data.size());
            indexWriteBuffer.putLong(Clock.systemUTC().millis());
            indexWriteBuffer.flip();
            ByteBuffer nioBuffer = buffer.nioBuffer();
            while(nioBuffer.hasRemaining()) {
                data.write(nioBuffer);
            }
            while (indexWriteBuffer.hasRemaining()) {
                index.write(indexWriteBuffer);
            }
            buffer.release();
            if (shouldFlush) {
                data.force(true);
                index.force(true);
            }
            size += 1;
            if (messageIOListener != null) {
                messageIOListener.onMessage(1, messageSize);
            }
        } catch (IOException ex) {
            throw new HydraRuntimeException("Error writing message to segment " + segmentDirectory.toString());
        }
    }

    @Override
    public void write(Message message) throws HydraRuntimeException {
        write(message, null);
    }

    @Override
    public void close() throws IOException {
        this.data.close();
        this.index.close();
    }
}
