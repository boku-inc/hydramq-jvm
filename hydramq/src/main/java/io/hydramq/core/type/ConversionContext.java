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

package io.hydramq.core.type;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import io.hydramq.CursorInfo;
import io.hydramq.Message;
import io.hydramq.MessageProperties;
import io.hydramq.MessageSet;
import io.hydramq.PartitionId;
import io.hydramq.PartitionInfo;
import io.hydramq.core.net.Acknowledgement;
import io.hydramq.core.net.Command;
import io.hydramq.core.net.Error;
import io.hydramq.core.net.commands.CursorInfoRequest;
import io.hydramq.core.net.commands.CursorInfoResponse;
import io.hydramq.core.net.commands.PartitionInfoRequest;
import io.hydramq.core.net.commands.PartitionInfoResponse;
import io.hydramq.core.net.commands.WriteCursorRequest;
import io.hydramq.core.net.protocols.topic.LockListenerNotification;
import io.hydramq.core.net.protocols.topic.LockListenerRequest;
import io.hydramq.core.net.protocols.topic.LockListenerRequestRequestConverter;
import io.hydramq.core.net.protocols.topic.PartitionIdReadRequest;
import io.hydramq.core.net.protocols.topic.PartitionIdReadRequestConverter;
import io.hydramq.core.net.protocols.topic.PartitionIdWriteRequest;
import io.hydramq.core.net.protocols.topic.PartitionIdWriteRequestConverter;
import io.hydramq.core.net.protocols.topic.PartitionsDiscoveredNotification;
import io.hydramq.core.net.protocols.topic.PartitionsDiscoveredNotificationConverter;
import io.hydramq.core.net.protocols.topic.ReadResponse;
import io.hydramq.core.net.protocols.topic.ReadResponseConverter;
import io.hydramq.core.net.protocols.topic.SubscriptionStatusNotificationConverter;
import io.hydramq.core.net.protocols.topic.TopicHandshake;
import io.hydramq.core.net.protocols.topic.TopicHandshakeConverter;
import io.hydramq.core.net.protocols.topicmanager.TopicDiscoveredNotification;
import io.hydramq.core.net.protocols.topicmanager.TopicDiscoveredNotificationConverter;
import io.hydramq.core.net.protocols.topicmanager.TopicManagerHandshake;
import io.hydramq.core.net.protocols.topicmanager.TopicManagerHandshakeConverter;
import io.hydramq.core.type.converters.AcknowledgementConverter;
import io.hydramq.core.type.converters.ByteArrayConverter;
import io.hydramq.core.type.converters.CursorInfoConverter;
import io.hydramq.core.type.converters.CursorInfoRequestConverter;
import io.hydramq.core.type.converters.CursorInfoResponseConverter;
import io.hydramq.core.type.converters.ErrorConverter;
import io.hydramq.core.type.converters.MessageConverter;
import io.hydramq.core.type.converters.MessagePropertiesConverter;
import io.hydramq.core.type.converters.MessageSetConverter;
import io.hydramq.core.type.converters.PartitionIdConverter;
import io.hydramq.core.type.converters.PartitionInfoConverter;
import io.hydramq.core.type.converters.PartitionInfoRequestConverter;
import io.hydramq.core.type.converters.PartitionInfoResponseConverter;
import io.hydramq.core.type.converters.PartitionStateConverter;
import io.hydramq.core.type.converters.StringConverter;
import io.hydramq.core.type.converters.UUIDConverter;
import io.hydramq.core.type.converters.WriteCursorRequestConverter;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.listeners.PartitionFlags;
import io.netty.buffer.ByteBuf;

/**
 * @author jfulton
 */
public class ConversionContext {

    private Map<Integer, Class<?>> correlatedConverters = new HashMap<>();
    private Map<Class<?>, TypeConverter<?>> converters = new HashMap<>();

    public <T> ConversionContext register(Class<T> type, TypeConverter<T> converter) {
        converters.put(type, converter);
        if (converter instanceof CommandTypeConverter<?>) {
            CommandTypeConverter<?> commandTypeConverter = (CommandTypeConverter) converter;
            correlatedConverters.put(commandTypeConverter.typeId(), type);
        }
        return this;
    }

    private <T> TypeConverter<T> lookup(Class<T> type) {
        if (converters.containsKey(type)) {
            return (TypeConverter<T>) converters.get(type);
        }
        throw new HydraRuntimeException("No " + TypeConverter.class.getName() + " registered for " + type.getName());
    }

    private TypeConverter<?> lookup(int typeId) {
        if (correlatedConverters.containsKey(typeId)) {
            return converters.get(correlatedConverters.get(typeId));
        }
        throw new HydraRuntimeException("No " + CommandTypeConverter.class.getName() + " registered for typeId " + typeId);
    }

    public Command read(int typeId, ByteBuf buffer) {
        return (Command) lookup(typeId).read(this, buffer);
    }

    public Command read(ByteBuf buffer) {
        if (!readable(buffer.getInt(0))) {
            return null;
        }
        return read(buffer.readInt(), buffer);
    }

    public <T> T read(Class<T> type, ByteBuf buffer) {
        if (Command.class.isAssignableFrom(type)) {
            buffer.skipBytes(Integer.BYTES); // swallow type field
        }
        return lookup(type).read(this, buffer);
    }

    public <T> ConversionContext write(Class<T> type, T instance, ByteBuf buffer) {
        lookup(type).write(this, instance, buffer);
        return this;
    }

    public ConversionContext write(Object instance, ByteBuf buffer) {
        lookup(instance.getClass()).writeObject(this, instance, buffer);
        return this;
    }

    public boolean readable(int typeId) {
        return correlatedConverters.containsKey(typeId);
    }

    public boolean readable(ByteBuf buffer) {
        return readable(buffer.getInt(0));
    }

    public boolean writable(Object object) {
        return converters.containsKey(object.getClass());
    }

    public static ConversionContext base() {
        return new ConversionContext()
                .register(String.class, new StringConverter())
                .register(byte[].class, new ByteArrayConverter())
                .register(Acknowledgement.class, new AcknowledgementConverter())
                .register(Error.class, new ErrorConverter())
                .register(UUID.class, new UUIDConverter())
                ;
    }

    public static ConversionContext topicProtocol() {
        return base()
                .register(TopicHandshake.class, new TopicHandshakeConverter())
                .register(Message.class, new MessageConverter())
                .register(MessageProperties.class, new MessagePropertiesConverter())
                .register(MessageSet.class, new MessageSetConverter())
                .register(PartitionInfo.class, new PartitionInfoConverter())
                .register(PartitionInfoRequest.class, new PartitionInfoRequestConverter())
                .register(PartitionInfoResponse.class, new PartitionInfoResponseConverter())
                .register(ReadResponse.class, new ReadResponseConverter())
                .register(PartitionIdWriteRequest.class, new PartitionIdWriteRequestConverter())
                .register(PartitionId.class, new PartitionIdConverter())
                .register(PartitionFlags.Flag.class, new PartitionStateConverter())
                .register(PartitionIdReadRequest.class, new PartitionIdReadRequestConverter())
                .register(PartitionsDiscoveredNotification.class, new PartitionsDiscoveredNotificationConverter())
                .register(LockListenerRequest.class, new LockListenerRequestRequestConverter())
                .register(LockListenerNotification.class, new SubscriptionStatusNotificationConverter())
                .register(CursorInfo.class, new CursorInfoConverter())
                .register(CursorInfoRequest.class, new CursorInfoRequestConverter())
                .register(CursorInfoResponse.class, new CursorInfoResponseConverter())
                .register(WriteCursorRequest.class, new WriteCursorRequestConverter())
                ;
    }

    public static ConversionContext topicManagerProtocol() {
        return base()
                .register(TopicManagerHandshake.class, new TopicManagerHandshakeConverter())
                .register(TopicDiscoveredNotification.class, new TopicDiscoveredNotificationConverter())
                ;
    }
}
