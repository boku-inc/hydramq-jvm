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

import java.util.UUID;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import com.fasterxml.uuid.UUIDComparator;
import io.hydramq.exceptions.InvalidPartitionIdFormat;
import io.hydramq.internal.util.Assert;

/**
 * @author jfulton
 */
public final class PartitionId implements Comparable<PartitionId> {

    private static final NoArgGenerator uuidGenerator = Generators.timeBasedGenerator();
    private static final UUIDComparator uuidComparator = new UUIDComparator();
    private final UUID uuid;

    private PartitionId(UUID uuid) {
        this.uuid = uuid;
    }

    public UUID getUUID() {
        return uuid;
    }

    public static PartitionId create(UUID uuid) {
        Assert.argumentIsTrue(uuid != null, "A PartitionId cannot be created with a null value");
        return new PartitionId(uuid);
    }

    public static PartitionId create() {
        return create(uuidGenerator.generate());
    }

    public static PartitionId create(String uuid) {
        Assert.argumentIsTrue(uuid != null, "A PartitionId cannot be created with a null value");
        try {
            return create(UUID.fromString(uuid));
        } catch (IllegalArgumentException ex) {
            throw new InvalidPartitionIdFormat(uuid);
        }
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PartitionId)) {
            return false;
        }
        return uuid.equals(((PartitionId)obj).getUUID());
    }

    @Override
    public int compareTo(PartitionId other) {
        return uuidComparator.compare(uuid, other.getUUID());
    }

    @Override
    public String toString() {
        return uuid.toString();
    }
}
