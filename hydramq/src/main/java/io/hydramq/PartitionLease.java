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

/**
 * @author jfulton
 */
public class PartitionLease {

    private final String topicName;
    private final String subscriptionName;
    private final int partitionId;

    public PartitionLease(final String topicName, final String subscriptionName, final int partitionId) {
        this.topicName = topicName;
        this.subscriptionName = subscriptionName;
        this.partitionId = partitionId;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String toString() {
        return "PartitionLease{" +
               "topicProtocol='" + topicName + '\'' +
               ", subscription='" + subscriptionName + '\'' +
               ", partitionId=" + partitionId +
               '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PartitionLease that = (PartitionLease) o;
        if (partitionId != that.partitionId) {
            return false;
        }
        if (!topicName.equals(that.topicName)) {
            return false;
        }
        return subscriptionName.equals(that.subscriptionName);
    }

    @Override
    public int hashCode() {
        int result = topicName.hashCode();
        result = 31 * result + subscriptionName.hashCode();
        result = 31 * result + partitionId;
        return result;
    }
}
