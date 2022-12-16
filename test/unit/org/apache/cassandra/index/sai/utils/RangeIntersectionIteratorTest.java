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
package org.apache.cassandra.index.sai.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.cassandra.io.util.FileUtils;

import static org.apache.cassandra.index.sai.utils.LongIterator.convert;
import static org.apache.cassandra.index.sai.utils.RangeIterator.Builder.IteratorType.INTERSECTION;
import static org.apache.cassandra.index.sai.utils.RangeIterator.Builder.IteratorType.UNION;
import static org.junit.Assert.assertEquals;

public class RangeIntersectionIteratorTest extends AbstractRangeIteratorTester
{
    @Test
    public void testNoOverlappingValues()
    {
        RangeIterator.Builder builder = RangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 1L, 7L }));
        builder.add(new LongIterator(new long[] { 4L, 8L, 9L, 10L }));

        assertEquals(convert(), convert(builder.build()));

        builder = RangeIntersectionIterator.builder();
        // both ranges overlap by min/max but not by value
        builder.add(new LongIterator(new long[] { 1L, 5L, 7L, 9L }));
        builder.add(new LongIterator(new long[] { 6L }));

        RangeIterator range = builder.build();

        Assert.assertNotNull(range);
        Assert.assertFalse(range.hasNext());

        builder = RangeIntersectionIterator.builder();
        // both ranges overlap by min/max but not by value
        builder.add(new LongIterator(new long[] { 1L, 5L, 7L, 9L }));
        builder.add(new LongIterator(new long[] { 0L, 10L, 12L }));

        range = builder.build();

        Assert.assertNotNull(range);
        Assert.assertFalse(range.hasNext());
    }

    @Test
    public void testOverlappingValues()
    {
        RangeIterator.Builder builder = RangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 4L, 6L, 7L }));
        builder.add(new LongIterator(new long[] { 2L, 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 6L, 8L, 9L, 10L }));

        assertEquals(convert(4L, 6L), convert(builder.build()));
    }

    @Test
    public void testSameValues()
    {
        RangeIterator.Builder builder = RangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L, 4L }));
        builder.add(new LongIterator(new long[] { 1L, 2L, 3L, 4L }));

        assertEquals(convert(1L, 2L, 3L, 4L), convert(builder.build()));
    }

    @Test
    public void testSingleIterator()
    {
        RangeIntersectionIterator.Builder builder = RangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 4L, 9L }));

        assertEquals(convert(1L, 2L, 4L, 9L), convert(builder.build()));
    }

    @Test
    public void testSkipTo()
    {
        RangeIterator.Builder builder = RangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 4L, 6L, 7L, 9L, 10L }));
        builder.add(new LongIterator(new long[] { 2L, 4L, 5L, 6L, 7L, 10L, 12L }));
        builder.add(new LongIterator(new long[] { 4L, 6L, 7L, 9L, 10L }));

        RangeIterator range = builder.build();
        Assert.assertNotNull(range);

        // first let's skipTo something before range
        assertEquals(4L, range.skipTo(LongIterator.fromToken(3L)).token().getLongValue());
        assertEquals(4L, range.getCurrent().token().getLongValue());

        // now let's skip right to the send value
        assertEquals(6L, range.skipTo(LongIterator.fromToken(5L)).token().getLongValue());
        assertEquals(6L, range.getCurrent().token().getLongValue());

        // now right to the element
        assertEquals(7L, range.skipTo(LongIterator.fromToken(7L)).token().getLongValue());
        assertEquals(7L, range.getCurrent().token().getLongValue());
        assertEquals(7L, range.next().token().getLongValue());

        Assert.assertTrue(range.hasNext());
        assertEquals(10L, range.getCurrent().token().getLongValue());

        // now right after the last element
        Assert.assertNull(range.skipTo(LongIterator.fromToken(11L)));
        Assert.assertFalse(range.hasNext());
    }

    @Test
    public void testMinMaxAndCount()
    {
        RangeIterator.Builder builder = RangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[]{1L, 2L, 9L}));
        builder.add(new LongIterator(new long[]{4L, 5L, 9L}));
        builder.add(new LongIterator(new long[]{7L, 8L, 9L}));

        assertEquals(9L, builder.getMaximum().token().getLongValue());
        assertEquals(9L, builder.getTokenCount());

        RangeIterator tokens = builder.build();

        Assert.assertNotNull(tokens);
        assertEquals(7L, tokens.getMinimum().token().getLongValue());
        assertEquals(9L, tokens.getMaximum().token().getLongValue());
        assertEquals(9L, tokens.getCount());

        assertEquals(convert(9L), convert(builder.build()));
    }

    @Test
    public void testBuilder()
    {
        RangeIntersectionIterator.Builder builder = RangeIntersectionIterator.builder();

        Assert.assertNull(builder.getMinimum());
        Assert.assertNull(builder.getMaximum());
        assertEquals(0L, builder.getTokenCount());
        assertEquals(0L, builder.rangeCount());

        builder.add(new LongIterator(new long[] { 1L, 2L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 6L, 8L, 9L }));

        assertEquals(6L, builder.getMinimum().token().getLongValue());
        assertEquals(6L, builder.getMaximum().token().getLongValue());
        assertEquals(9L, builder.getTokenCount());
        assertEquals(3L, builder.rangeCount());
        Assert.assertFalse(builder.statistics.isDisjoint());

        assertEquals(1L, builder.rangeIterators.get(0).getMinimum().token().getLongValue());
        assertEquals(4L, builder.rangeIterators.get(1).getMinimum().token().getLongValue());
        assertEquals(6L, builder.rangeIterators.get(2).getMinimum().token().getLongValue());

        builder.add(new LongIterator(new long[] { 1L, 2L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 6L, 8L, 9L }));

        assertEquals(convert(6L), convert(builder.build()));

        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[]{ 1L, 5L, 6L }));
        builder.add(new LongIterator(new long[]{ 3L, 5L, 6L }));

        RangeIterator tokens = builder.build();

        assertEquals(convert(5L, 6L), convert(tokens));

        FileUtils.closeQuietly(tokens);

        RangeIterator emptyTokens = RangeIntersectionIterator.builder().build();
        assertEquals(0, emptyTokens.getCount());

        builder = RangeIntersectionIterator.builder();
        assertEquals(0L, builder.add((RangeIterator) null).rangeCount());
        assertEquals(0L, builder.add((List<RangeIterator>) null).getTokenCount());
        assertEquals(0L, builder.add(new LongIterator(new long[] {})).rangeCount());

        RangeIterator single = new LongIterator(new long[] { 1L, 2L, 3L });
        RangeIterator range = RangeIntersectionIterator.builder().add(single).build();

        // because build should return first element if it's only one instead of building yet another iterator
        assertEquals(range, single);

        // Make a difference between empty and null ranges.
        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        assertEquals(0L, builder.rangeCount());
        builder.add(single);
        assertEquals(1L, builder.rangeCount());
        range = builder.build();
        assertEquals(0, range.getCount());

        // disjoint case
        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));

        Assert.assertTrue(builder.statistics.isDisjoint());

        RangeIterator disjointIntersection = builder.build();
        Assert.assertNotNull(disjointIntersection);
        Assert.assertFalse(disjointIntersection.hasNext());

    }

    @Test
    public void emptyRangeTest()
    {
        RangeIterator.Builder builder;

        // empty, then non-empty
        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {10}));
        assertEmpty(builder.build());

        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        assertEmpty(builder.build());

        // non-empty, then empty
        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {10}));
        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        builder = RangeIntersectionIterator.builder();
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));

        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        // empty, then non-empty then empty again
        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {0, 10}));
        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        // non-empty, empty, then non-empty again
        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {0, 10}));
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {0, 10}));
        assertEmpty(builder.build());

        builder = RangeIntersectionIterator.builder();
        for (int i = 0; i < 5; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        builder.add(new LongIterator(new long[] {}));
        for (int i = 5; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        assertEmpty(builder.build());
    }

    public static void assertEmpty(RangeIterator range)
    {
        Assert.assertNull(range.getMinimum());
        Assert.assertNull(range.getMaximum());
        Assert.assertFalse(range.hasNext());
        assertEquals(0, range.getCount());
    }

    @Test
    public void testClose() throws IOException
    {
        RangeIterator tokens = RangeIntersectionIterator.builder()
                                                        .add(new LongIterator(new long[] { 1L, 2L, 3L }))
                                                        .build();

        Assert.assertNotNull(tokens);
        tokens.close();
    }

    @Test
    public void testIsOverlapping()
    {
        RangeIterator rangeA, rangeB;

        rangeA = new LongIterator(new long[] { 1L, 5L });
        rangeB = new LongIterator(new long[] { 5L, 9L });
        Assert.assertTrue(RangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 5L, 9L });
        rangeB = new LongIterator(new long[] { 1L, 6L });
        Assert.assertTrue(RangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 5L, 9L });
        rangeB = new LongIterator(new long[] { 5L, 9L });
        Assert.assertTrue(RangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 1L, 4L });
        rangeB = new LongIterator(new long[] { 5L, 9L });
        Assert.assertFalse(RangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 6L, 9L });
        rangeB = new LongIterator(new long[] { 1L, 4L });
        Assert.assertFalse(RangeIterator.isOverlapping(rangeA, rangeB));
    }

    @Test
    public void testIntersectionOfRandomRanges()
    {
        for (int attempt = 0; attempt < 16; attempt++)
        {
            final int maxRanges = nextInt(2, 16);

            // generate randomize ranges
            long[][] ranges = new long[maxRanges][];
            for (int i = 0; i < ranges.length; i++)
            {
                int rangeSize = nextInt(16, 512);
                LongSet range = new LongHashSet(rangeSize);

                for (int j = 0; j < rangeSize; j++)
                    range.add(nextLong(0, 100));

                ranges[i] = range.toArray();
                Arrays.sort(ranges[i]);
            }

            List<Long> expected = new ArrayList<>();
            // determine unique tokens which intersect every range
            for (long token : ranges[0])
            {
                boolean intersectsAll = true;
                for (int i = 1; i < ranges.length; i++)
                {
                    if (Arrays.binarySearch(ranges[i], token) < 0)
                    {
                        intersectsAll = false;
                        break;
                    }
                }

                if (intersectsAll)
                    expected.add(token);
            }

            RangeIterator.Builder builder = RangeIntersectionIterator.builder();
            for (long[] range : ranges)
                builder.add(new LongIterator(range));

            assertEquals(expected, convert(builder.build()));
        }
    }

    // SAI specific tests
    @Test
    public void testSelectiveIntersection()
    {
        RangeIterator intersection = buildSelectiveIntersection(2,
                                                                arr(1L, 4L, 6L, 7L),
                                                                arr(1L, 4L, 5L, 6L),
                                                                arr(4L, 6L, 8L, 9L, 10L)); // skipped

        assertEquals(convert(1L, 4L, 6L), convert(intersection));

        intersection = buildSelectiveIntersection(1,
                                                  arr(2L, 4L, 6L),
                                                  arr(1L, 4L, 5L, 6L),       // skipped
                                                  arr(4L, 6L, 8L, 9L, 10L)); // skipped

        assertEquals(convert(2L, 4L, 6L), convert(intersection));
    }

    @Test
    public void testIntersectionOfUnionsOnError()
    {
        // intersection of two unions
        RangeIterator unionA = buildOnErrorB(UNION, arr(1L, 2L, 3L), arr(5L, 6L, 7L));
        RangeIterator unionB = buildUnion(arr(2L, 4L, 6L), arr(5L, 7L, 9L));
        assertOnError(buildIntersection(unionA, unionB));

        // intersection of union and intersection
        RangeIterator unionC = buildOnErrorB(UNION, arr(2L, 4L, 6L), arr(5L, 6L, 9L));
        RangeIterator intersectionA = buildIntersection(arr(3L, 4L, 6L, 9L), arr(2L, 3L, 6L, 9L));
        assertOnError(buildIntersection(unionC, intersectionA));
    }

    @Test
    public void testIntersectionOfIntersectionsOnError()
    {
        RangeIterator intersectionA = buildIntersection(arr(1L, 2L, 3L, 6L), arr(2L, 3L, 6L));
        RangeIterator intersectionB = buildOnErrorA(INTERSECTION, arr(2L, 4L, 6L), arr(5L, 6L, 7L, 9L));
        assertOnError(buildIntersection(intersectionA, intersectionB));

        intersectionA = buildOnErrorB(INTERSECTION, arr(1L, 2L, 3L, 4L, 5L), arr(2L, 3L, 4L));
        intersectionB = buildIntersection(arr(1L, 2L, 3L, 4L, 6L), arr(2L, 3L, 4L, 7L, 9L));
        assertOnError(buildIntersection(intersectionA, intersectionB));

        intersectionA = buildOnError(INTERSECTION, arr(1L, 2L, 3L, 5L), arr( 3L, 4L));
        intersectionB = buildIntersection(arr(1L, 2L, 3L, 4L, 6L), arr(2L, 3L, 4L, 7L, 9L));
        assertOnError(buildIntersection(intersectionA, intersectionB));
    }
}
