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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import io.hydramq.exceptions.InvalidPartitionIdFormat;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.slf4j.LoggerFactory.getLogger;
import static org.testng.Assert.fail;

/**
 * @author jfulton
 */
public class PartitionIdTest {

    private static final Logger logger = getLogger(PartitionIdTest.class);

    @Test
    public void testCreateFromUUID() throws Exception {
        UUID uuid = UUID.randomUUID();
        PartitionId partitionId = PartitionId.create(uuid);
        assertThat(partitionId.getUUID(), is(uuid));
    }

    @Test
    public void testEquals() throws Exception {
        PartitionId partitionId1 = PartitionId.create();

        PartitionId partitionId2 = PartitionId.create(partitionId1.getUUID());
        assertThat(partitionId1.equals(partitionId2), is(true));
        assertThat(partitionId1, is(partitionId2));

        assertThat(partitionId1, not("Blah"));
    }

    @Test
    public void testHashCode() throws Exception {
        PartitionId partitionId1 = PartitionId.create();

        PartitionId partitionId2 = PartitionId.create(partitionId1.getUUID());
        assertThat(partitionId1.equals(partitionId2), is(true));
        assertThat(partitionId1.hashCode(), is(partitionId2.hashCode()));
    }

    @Test
    public void testFromString() throws Exception {
        PartitionId partitionId = PartitionId.create();
        assertThat(PartitionId.create(partitionId.toString()), is(partitionId));
    }

    @Test
    public void testCreateFromInvalidString() throws Exception {
        try {
            PartitionId.create("122baa5b-2888-48be-a99d-");
            fail(InvalidPartitionIdFormat.class.getSimpleName() + " expected");
        } catch (InvalidPartitionIdFormat ex) {
            assertThat(ex.getMessage(), containsString("122baa5b-2888-48be-a99d-"));
        }
    }

    @Test
    public void testCreateFromNullStringValue() throws Exception {
        try {
            PartitionId.create((String) null);
            fail(IllegalArgumentException.class.getSimpleName() + " expected");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), containsString("PartitionId cannot be created"));
        }
    }

    @Test
    public void testCreateFromNullUUIDValue() throws Exception {
        try {
            PartitionId.create((UUID) null);
            fail(IllegalArgumentException.class.getSimpleName() + " expected");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), containsString("PartitionId cannot be created"));
        }
    }

    @Test
    public void testCompareTo() throws Exception {
        PartitionId partitionId1 = PartitionId.create("122baa5b-2888-48be-a99d-b3fb92f6703b");
        PartitionId partitionId2 = PartitionId.create("81a278fe-96e3-4226-9b10-90b551500951");
        PartitionId partitionId3 = PartitionId.create("fe8673a7-4a54-4d7a-9e56-4d33ec5d0015");
        PartitionId partitionId4 = PartitionId.create("122baa5b-2888-48be-a99d-b3fb92f6703b");

        assertThat(partitionId1, comparesEqualTo(partitionId4));
        assertThat(partitionId1.compareTo(partitionId1), is(0));
        assertThat(partitionId1.compareTo(partitionId2), lessThan(0));
        assertThat(partitionId1.compareTo(partitionId3), lessThan(0));
        assertThat(partitionId3.compareTo(partitionId2), greaterThan(0));
    }

    @Test
    public void testCompareToTimeBased() throws Exception {
        List<PartitionId> reversed = new ArrayList<>();
        for (int i = 0; i < 100_000; i++) {
            PartitionId first = PartitionId.create();
            PartitionId second = PartitionId.create();
            assertThat(first.compareTo(second), lessThan(0));
            reversed.add(0, PartitionId.create(first.toString()));
        }

        List<PartitionId> results = new ArrayList<>();
        results.addAll(reversed);
        Collections.sort(results);

        for (int i = 0; i < 100_000; i++) {
            assertThat(reversed.get((100_000 - 1) - i), is(results.get(i)));
        }
    }

    @Test
    public void testTimeBasedSorting() throws Exception {
        List<PartitionId> partitionIds = new ArrayList<>();
        PartitionId first = PartitionId.create("339666ba-08e0-11e6-907f-9f2f1571079e");
        PartitionId second = PartitionId.create("a65540ae-08e0-11e6-8a16-21067980b55f");
        PartitionId third = PartitionId.create("f31ab969-08e0-11e6-8845-49f743d18885");
        PartitionId fourth = PartitionId.create("086f6310-08e1-11e6-a4c6-5b1e790c9a27");
        partitionIds.add(fourth);
        partitionIds.add(second);
        partitionIds.add(first);
        partitionIds.add(third);
        Collections.sort(partitionIds);
        assertThat(partitionIds.get(0), is(first));
        assertThat(partitionIds.get(1), is(second));
        assertThat(partitionIds.get(2), is(third));
        assertThat(partitionIds.get(3), is(fourth));
    }
}