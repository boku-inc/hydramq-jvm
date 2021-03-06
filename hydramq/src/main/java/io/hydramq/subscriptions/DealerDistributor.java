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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author jfulton
 */
public class DealerDistributor<SUBSCRIBER, RESOURCE> implements Distributor<SUBSCRIBER, RESOURCE> {

    private Set<SUBSCRIBER> subscribers = new LinkedHashSet<>();

    @Override
    public void add(SUBSCRIBER subscriber) {
        subscribers.add(subscriber);
    }

    @Override
    public void remove(SUBSCRIBER subscriber) {
        subscribers.remove(subscriber);
    }

    @Override
    public Map<SUBSCRIBER, Set<RESOURCE>> distribute(Set<RESOURCE> resources) {
        HashMap<SUBSCRIBER, Set<RESOURCE>> results = new HashMap<>();
        if (this.subscribers.size() == 0 || resources.size() == 0) {
            return results;
        }
        Iterator<SUBSCRIBER> resourceIterator = this.subscribers.iterator();
        Iterator<RESOURCE> subscriberIterator = resources.iterator();
        for (int i = 0; i < resources.size(); i++) {
            if (!resourceIterator.hasNext()) {
                resourceIterator = this.subscribers.iterator();
            }
            if (!subscriberIterator.hasNext()) {
                subscriberIterator = resources.iterator();
            }
            results.computeIfAbsent(resourceIterator.next(), resource -> new HashSet<>())
                    .add(subscriberIterator.next());
        }
        return results;
    }
}
