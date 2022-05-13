/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service.accord.txn;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;

import accord.api.Key;
import accord.txn.Keys;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;

/**
 * Immutable collection of items, sorted first by their partition key
 * @param <T>
 */
public abstract class AbstractKeySorted<T> implements Iterable<T>
{
    final T[] items;

    // items are expected to be sorted
    public AbstractKeySorted(T[] items)
    {
        this.items = items;
    }

    public AbstractKeySorted(List<T> items)
    {
        T[] arr = newArray(items.size());
        items.toArray(arr);
        Arrays.sort(arr, this::compare);
        this.items = arr;
    }

    @Override
    public Iterator<T> iterator()
    {
        return Iterators.forArray(items);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + Arrays.stream(items)
                                                  .map(Objects::toString)
                                                  .collect(Collectors.joining(", ", "{", "}"));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractKeySorted<?> that = (AbstractKeySorted<?>) o;
        return Arrays.equals(items, that.items);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(items);
    }

    @VisibleForTesting
    public Keys createKeys()
    {
        Set<PartitionKey> keysSet = new HashSet<>();
        this.forEach(i -> keysSet.add(getKey(i)));

        Key[] keys = keysSet.toArray(Key[]::new);
        Arrays.sort(keys, AccordKey::compareKeys);
        return new Keys(keys);
    }

    /**
     * Compare the non-key component of items (since this class handles sorting by key)
     */
    abstract int compareNonKeyFields(T left, T right);
    abstract PartitionKey getKey(T item);
    abstract T[] newArray(int size);

    private int compare(T left, T right)
    {
        int cmp = getKey(left).compareTo(getKey(right));
        return cmp != 0 ? cmp : compareNonKeyFields(left, right);
    }

    @VisibleForTesting
    void validateOrder()
    {
        for (int i=1; i< items.length; i++)
        {
            T prev = items[i-1];
            T next = items[i];
            if (getKey(prev).compareTo(getKey(next)) > 0)
                throw new IllegalStateException(String.format("Items are out of order ([%s] %s >= [%s] %s)", i-1, prev, i, next));
            if (compare(prev, next) >= 0)
                throw new IllegalStateException(String.format("Items are out of order ([%s] %s >= [%s] %s)", i-1, prev, i, next));
        }
    }

    public int size()
    {
        return items.length;
    }

    private int firstPossibleKeyIdx(PartitionKey key)
    {
        int idx = Arrays.binarySearch(items, key, (l, r) -> {
            PartitionKey lk = getKey((T) l);
            PartitionKey rk = (PartitionKey) r;
            int cmp = lk.compareTo(rk);
            return cmp != 0 ? cmp : 1;
        });

        return -1 - idx;
    }

    public void forEachForKey(PartitionKey key, Consumer<T> consumer)
    {
        for (int i=firstPossibleKeyIdx(key); i<items.length && getKey(items[i]).equals(key); i++)
            consumer.accept(items[i]);
    }
}
