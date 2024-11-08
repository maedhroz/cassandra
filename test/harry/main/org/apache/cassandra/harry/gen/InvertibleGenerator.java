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

package org.apache.cassandra.harry.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import accord.utils.Invariants;
import org.agrona.collections.IntHashSet;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.gen.rng.PCGFastPure;
import org.apache.cassandra.harry.gen.rng.SeedableEntropySource;

/**
 * Invertible generator allows you to provide _any_ data type. Harry is based on the idea that descriptors
 * can be inflated into values, and values can be turned back into descriptors. Descriptors follow the sorting
 * order of the values they were generated from. This makes _writing_ these generators a bit more complex.
 * There is a library of lightweight generators available for simple cases.
 *
 * InvertibleGenerator decouples descriptor order from value order, and allows descriptor to be used simply as
 * a seed for generating values. Since it tracks all descriptors it generated values from in a sorted order,
 * it can always turn the given value back into a descriptor by inflating log(population) values and comparing them
 * to the searched value. In other words, it trades memory required for storing map of values to CPU required
 * to re-compute the value order.
 *
 * TODO: custom invertible generator for bool, u8, u16, u32, etc, for efficiency.
 */
public class InvertibleGenerator<T> implements Bijections.IndexedBijection<T>
{
    public static long MAX_ENTROPY = 1L << 63;

    private static final boolean PARANOIA = true;

    // TODO (required): switch to use a primitive array; will need to implement a sort comparator for primitive types
    private final List<Long> allocatedDescriptors;

    private final SeedableEntropySource rngSupplier;
    private final Generator<T> gen;
    private final Comparator<T> comparator;

    // To avoid <?> erased types
    public static <T> InvertibleGenerator<T> fromType(long seed, int population, ColumnSpec<T> spec)
    {
        return new InvertibleGenerator<>(seed, spec.type.typeEntropy(), population, spec.gen, spec.type.comparator());
    }

    public InvertibleGenerator(long seed,
                               /* unsigned */ long typeEntropy,
                               int population,
                               Generator<T> gen,
                               // TODO (expected): tuple/vector/udt/etc.
                               Comparator<T> comparator)
    {
        Invariants.checkState(population > 0,
                              "Population should be strictly positive %d", population);
        Invariants.checkState(Long.compareUnsigned(typeEntropy, 0) > 0,
                              "Type entropy should be strictly positive, but was %d: %s", typeEntropy, gen);

        // We can / will generate at most that many values
        if (Long.compareUnsigned(typeEntropy, Integer.MAX_VALUE) > 0)
            typeEntropy = Integer.MAX_VALUE;

        population = (int) Math.min(typeEntropy, population);

        this.gen = gen;
        this.comparator = comparator;
        this.allocatedDescriptors = new ArrayList<>();
        this.rngSupplier = new SeedableEntropySource();
        IdxToDescriptor idxToDescriptor = (v) -> PCGFastPure.shuffle(PCGFastPure.advanceState(seed, v, seed));

        // Generate a population of _unique_ values. We do not want to store all values, only their hashes.
        IntHashSet hashes = new IntHashSet(population);
        int idx = 0;
        while (allocatedDescriptors.size() < population)
        {
            long candidate = idxToDescriptor.descriptor(idx++);

            // Should never allocate these, however improbable that is
            if (MagicConstants.MAGIC_DESCRIPTOR_VALS.contains(candidate))
                continue;

            Object inflated = inflate(candidate);
            int hash;

            if (inflated.getClass().isArray())
            {
                Class<?> klass = inflated.getClass();
                if (klass == Object[].class) hash = Arrays.hashCode((Object[]) inflated);
                else if (klass == byte[].class) hash = Arrays.hashCode((byte[]) inflated);
                else if (klass == char[].class) hash = Arrays.hashCode((char[]) inflated);
                else if (klass == short[].class) hash = Arrays.hashCode((short[]) inflated);
                else if (klass == int[].class) hash = Arrays.hashCode((int[]) inflated);
                else if (klass == long[].class) hash = Arrays.hashCode((long[]) inflated);
                else if (klass == double[].class) hash = Arrays.hashCode((double[]) inflated);
                else if (klass == float[].class) hash = Arrays.hashCode((float[]) inflated);
                else if (klass == boolean[].class) hash = Arrays.hashCode((boolean[]) inflated);
                else  throw new IllegalArgumentException("Unknown type: " + klass);
            }
            else
            {
                hash = inflated.hashCode();
                Invariants.checkState(hash  != System.identityHashCode(inflated));
            }

            if (hashes.add(hash))
                allocatedDescriptors.add(candidate);
        }
        hashes.clear();

        allocatedDescriptors.sort(this::compare);

        // Check there are no duplicates, and items are properly sorted.
        if (PARANOIA)
        {
            T prev = inflate(allocatedDescriptors.get(0));
            for (int i = 1; i < allocatedDescriptors.size(); i++)
            {
                T current = inflate(allocatedDescriptors.get(i));
                Invariants.checkState( comparator.compare(current, prev) > 0,
                                       () -> String.format("%s should be strictly after %s", prev, current));
            }
        }
    }

    @Override
    public int idxFor(long descriptor)
    {
        return Collections.binarySearch(allocatedDescriptors, descriptor, this.descriptorsComparator());
    }

    @Override
    public long descriptorAt(int idx)
    {
        return allocatedDescriptors.get(idx);
    }

    @Override
    public T inflate(long descriptor)
    {
        Invariants.checkState(!MagicConstants.MAGIC_DESCRIPTOR_VALS.contains(descriptor),
                              String.format("Should not be able to inflate %d, as it's magic value", descriptor));
        return rngSupplier.computeWithSeed(descriptor, gen::generate);
    }

    @Override
    public long deflate(T value)
    {
        final int idx = binarySearch(value);
        if (PARANOIA)
        {
            if (idx < 0)
            {
                for (long descriptor : allocatedDescriptors)
                {
                    Object expected = inflate(descriptor);
                    if (value.getClass().isArray())
                    {
                        Object[] valueArr = (Object[]) value;
                        Object[] expectedArr = (Object[]) expected;
                        Invariants.checkState(comparator.compare((T) expected, value) != 0,
                                              "%s was found: %s", Arrays.toString(expectedArr), Arrays.toString(valueArr));

                    }
                    else
                    {
                        Invariants.checkState(comparator.compare((T) expected, value) != 0,
                                              "%s was found: %s", expected, value);
                    }

                }
            }
            else
            {
                long res = allocatedDescriptors.get(idx);
                Object expected = inflate(res);
                if (value.getClass().isArray())
                {
                    Object[] valueArr = (Object[]) value;
                    Object[] expectedArr = (Object[]) expected;

                    Invariants.checkState(comparator.compare((T) expected, value) == 0,
                                          "%s != %s", Arrays.toString(expectedArr), Arrays.toString(valueArr));

                }
                else
                {
                    Invariants.checkState(comparator.compare((T) expected, value) == 0,
                                          "%s != %s", expected, value);
                }

                return res;
            }
        }

        if (idx < 0)
        {
            int start = Math.max(0, idx - 2);
            List<Object> nearby = new ArrayList<>();
            for (int i = start; i < start + 2; i++)
                nearby.add(inflate(allocatedDescriptors.get(i)));
            throw new IllegalStateException(String.format("Could not find: %s\nNearby objects: %s",
                                                          toString(value), nearby.stream().map(InvertibleGenerator::toString).collect(Collectors.toList())));
        }

        return allocatedDescriptors.get(idx);
    }

    private static String toString(Object obj)
    {
        if (obj.getClass().isArray())
            return Arrays.toString((Object[]) obj);
        return obj.toString();
    }

    @Override
    public int byteSize()
    {
        return Long.BYTES;
    }

    private int binarySearch(T key)
    {
        int low = 0, mid = allocatedDescriptors.size(), high = mid - 1, result = -1;
        while (low <= high)
        {
            mid = (low + high) >>> 1;
            result = comparator.compare(key, inflate(allocatedDescriptors.get(mid)));
            if (result > 0)
            {
                low = mid + 1;
            }
            else if (result == 0)
            {
                return mid;
            }
            else
            {
                high = mid - 1;
            }
        }
        return -mid - (result < 0 ? 1 : 2);
    }


    @Override
    public int compare(long d1, long d2)
    {
        if (d1 == d2)
            return 0;
        T v1 = inflate(d1);
        T v2 = inflate(d2);
        return comparator.compare(v1, v2);
    }

    /**
     * Returns a number of allocated descriptors
     */
    @Override
    public int population()
    {
        return allocatedDescriptors.size();
    }

    public Comparator<Long> descriptorsComparator()
    {
        // TODO: this can be cached
        Map<Long, Integer> descriptorToIdx = new HashMap<>();
        for (int i = 0; i < allocatedDescriptors.size(); i++)
            descriptorToIdx.put(allocatedDescriptors.get(i), i);
        return Comparator.comparingInt(descriptorToIdx::get);
    }

    public static Comparator<Object[]> keyComparator(List<ColumnSpec<?>> columns)
    {
        return (o1, o2) -> compareKeys(columns, o1, o2);
    }

    public static int compareKeys(List<ColumnSpec<?>> columns, Object[] v1, Object[] v2)
    {
        assert v1.length == v2.length : String.format("Values should be of same length: %d != %d\n%s\n%s",
                                                      v1.length, v2.length, Arrays.toString(v1), Arrays.toString(v2));

        for (int i = 0; i < v1.length; i++)
        {
            int res;
            ColumnSpec column = columns.get(i);
            if (column.type.isReversed())
                res = column.type.comparator().reversed().compare(v1[i], v2[i]);
            else
                res = column.type.comparator().compare(v1[i], v2[i]);
            if (res != 0)
                return res;
        }
        return 0;
    }

    private interface IdxToDescriptor
    {
        long descriptor(long idx);
    }
}