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

package io.hydramq.subscriptions;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author jfulton
 */
public class ModuloDistributorTest {

    @Test
    public void testDistributeOneForOne() throws Exception {
        Resource r1 = new Resource(1);
        Resource r2 = new Resource(2);
        Resource r3 = new Resource(3);
        Resource r4 = new Resource(4);

        Subscriber s1 = new Subscriber(1);
        Subscriber s2 = new Subscriber(2);
        Subscriber s3 = new Subscriber(3);
        Subscriber s4 = new Subscriber(4);

        Distributor<Subscriber, Resource> distributor = create();
        distributor.add(s1);
        distributor.add(s2);
        distributor.add(s3);
        distributor.add(s4);

        Set<Resource> resources = new LinkedHashSet<>();
        resources.add(r1);
        resources.add(r2);
        resources.add(r3);
        resources.add(r4);

        Map<Subscriber, Set<Resource>> distributions = distributor.distribute(resources);
        assertThat(distributions.get(s1), containsInAnyOrder(r1));
        assertThat(distributions.get(s1).size(), is(1));
        assertThat(distributions.get(s2), containsInAnyOrder(r2));
        assertThat(distributions.get(s2).size(), is(1));
        assertThat(distributions.get(s3), containsInAnyOrder(r3));
        assertThat(distributions.get(s3).size(), is(1));
        assertThat(distributions.get(s4), containsInAnyOrder(r4));
        assertThat(distributions.get(s4).size(), is(1));
    }

    @Test
    public void testDistributeMoreResourcesThanSubscribers() throws Exception {
        Subscriber s1 = new Subscriber(1);
        Subscriber s2 = new Subscriber(2);
        Subscriber s3 = new Subscriber(3);
        Subscriber s4 = new Subscriber(4);

        Resource r1 = new Resource(1);
        Resource r2 = new Resource(2);
        Resource r3 = new Resource(3);
        Resource r4 = new Resource(4);
        Resource r5 = new Resource(5);
        Resource r6 = new Resource(6);
        Resource r7 = new Resource(7);
        Resource r8 = new Resource(8);
        Resource r9 = new Resource(9);

        Distributor<Subscriber, Resource> distributor = create();
        distributor.add(s1);
        distributor.add(s2);
        distributor.add(s3);
        distributor.add(s4);

        Set<Resource> resources = new LinkedHashSet<>();
        resources.add(r1);
        resources.add(r2);
        resources.add(r3);
        resources.add(r4);
        resources.add(r5);
        resources.add(r6);
        resources.add(r7);
        resources.add(r8);
        resources.add(r9);

        Map<Subscriber, Set<Resource>> distributions = distributor.distribute(resources);
        assertThat(distributions.get(s1), containsInAnyOrder(r1, r2, r3));
        assertThat(distributions.get(s1).size(), is(3));
        assertThat(distributions.get(s2), containsInAnyOrder(r4, r5));
        assertThat(distributions.get(s2).size(), is(2));
        assertThat(distributions.get(s3), containsInAnyOrder(r6, r7));
        assertThat(distributions.get(s3).size(), is(2));
        assertThat(distributions.get(s4), containsInAnyOrder(r8, r9));
        assertThat(distributions.get(s4).size(), is(2));
    }

    @Test
    public void testDistributeMoreSubscribersThanResources() throws Exception {
        Subscriber s1 = new Subscriber(1);
        Subscriber s2 = new Subscriber(2);
        Subscriber s3 = new Subscriber(3);
        Subscriber s4 = new Subscriber(4);
        Subscriber s5 = new Subscriber(5);

        Resource r1 = new Resource(1);
        Resource r2 = new Resource(2);
        Resource r3 = new Resource(3);
        Resource r4 = new Resource(4);

        Set<Resource> resources = new LinkedHashSet<>();
        resources.add(r1);
        resources.add(r2);
        resources.add(r3);
        resources.add(r4);

        Distributor<Subscriber, Resource> distributor = create();
        distributor.add(s1);
        distributor.add(s2);
        distributor.add(s3);
        distributor.add(s4);
        distributor.add(s5);


        Map<Subscriber, Set<Resource>> distributions = distributor.distribute(resources);
        assertThat(distributions.get(s1), containsInAnyOrder(r1));
        assertThat(distributions.get(s1).size(), is(1));
        assertThat(distributions.get(s2), containsInAnyOrder(r2));
        assertThat(distributions.get(s2).size(), is(1));
        assertThat(distributions.get(s3), containsInAnyOrder(r3));
        assertThat(distributions.get(s3).size(), is(1));
        assertThat(distributions.get(s4), containsInAnyOrder(r4));
        assertThat(distributions.get(s4).size(), is(1));
        assertThat(distributions.containsKey(s5), is(false));
    }


    @Test
    public void testDistributeZeroSubscribers() throws Exception {
        Set<Resource> resources = new LinkedHashSet<>();
        resources.add(new Resource(1));
        resources.add(new Resource(2));
        resources.add(new Resource(3));
        resources.add(new Resource(4));

        Distributor<Subscriber, Resource> distributor = create();


        Map<Subscriber, Set<Resource>> distributions = distributor.distribute(resources);
        assertThat(distributions.size(), is(0));
    }


    @Test
    public void testDistributeZeroResources() throws Exception {
        Subscriber s1 = new Subscriber(1);
        Subscriber s2 = new Subscriber(2);
        Subscriber s3 = new Subscriber(3);
        Subscriber s4 = new Subscriber(4);
        Subscriber s5 = new Subscriber(5);
        Distributor<Subscriber, Resource> distributor = create();
        distributor.add(s1);
        distributor.add(s2);
        distributor.add(s3);
        distributor.add(s4);
        distributor.add(s5);


        Map<Subscriber, Set<Resource>> distributions = distributor.distribute(new HashSet<>());
        assertThat(distributions.size(), is(0));
    }

    private Distributor<Subscriber, Resource> create() {
        return new ModuloDistributor<>();
    }

    private static class Resource {
        private int id;

        private Resource(int id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return String.valueOf(id);
        }
    }

    private static class Subscriber {
        private int id;

        private Subscriber(int id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return String.valueOf(id);
        }
    }

}