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

package io.hydramq.listeners;

import java.util.EnumSet;

import io.hydramq.exceptions.HydraRuntimeException;

/**
 * @author jfulton
 */
public final class PartitionFlags {

    protected EnumSet<Flag> flags;

    public PartitionFlags(EnumSet<Flag> flags) {
        this.flags = flags;
    }

    public boolean isWritable() {
        return flags.contains(Flag.WRITE);
    }

    public boolean isReadable() {
        return flags.contains(Flag.READ);
    }

    public boolean hasFlag(Flag flag) {
        return flags.contains(flag);
    }

    public PartitionFlags setFlag(Flag flag) {
        flags.add(flag);
        return this;
    }

    public PartitionFlags removeFlag(Flag flag) {
        flags.remove(flag);
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PartitionFlags)) {
            return false;
        }
        @SuppressWarnings("ConstantConditions") PartitionFlags other = (PartitionFlags) obj;
        return flags.equals(other.flags);
    }

    @Override
    public int hashCode() {
        return flags.hashCode();
    }

    public static PartitionFlags fromInt(int flags) {
        EnumSet<Flag> results = EnumSet.noneOf(Flag.class);
        for (Flag flag : Flag.values()) {
            if ((flags & flag.flag()) == flag.flag()) {
                results.add(flag);
            }
        }
        return new PartitionFlags(results);
    }

    @Override
    public String toString() {
        return flags.toString();
    }

    public static PartitionFlags of(Flag flag, Flag... flags) {
        return new PartitionFlags(EnumSet.of(flag, flags));
    }

    public int toInt() {
        int intFlags = 0;
        for (Flag state : flags) {
            intFlags = intFlags | state.flag();
        }
        return intFlags;
    }

    public EnumSet<Flag> toEnumSet() {
        return EnumSet.copyOf(flags);
    }

    /**
     * @author jfulton
     */
    public enum Flag {
        READ(1),
        WRITE(1 << 1),
        CREATED(1 << 2),
        REMOVED(1 << 3),
        ;

        private int id;

        Flag(int id) {
            this.id = id;
        }

        public static Flag valueOf(int id) {
            for (Flag state : Flag.values()) {
                if (state.flag() == id) {
                    return state;
                }
            }
            throw new HydraRuntimeException("No Flag for id " + id);
        }

        public int flag() {
            return id;
        }
    }
}
