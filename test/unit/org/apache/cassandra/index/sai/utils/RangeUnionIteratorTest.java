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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.util.FileUtils;

import static org.apache.cassandra.index.sai.utils.LongIterator.convert;
import static org.apache.cassandra.index.sai.utils.RangeIterator.Builder.IteratorType.CONCAT;
import static org.apache.cassandra.index.sai.utils.RangeIterator.Builder.IteratorType.INTERSECTION;
import static org.apache.cassandra.index.sai.utils.RangeIterator.Builder.IteratorType.UNION;
import static org.junit.Assert.assertEquals;

public class RangeUnionIteratorTest extends AbstractRangeIteratorTester
{
    @Test
    public void testNoOverlappingValues()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 1L, 7L }));
        builder.add(new LongIterator(new long[] { 4L, 8L, 9L, 10L }));

        assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), convert(builder.build()));
    }

    @Test
    public void testSingleIterator()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 4L, 9L }));

        assertEquals(convert(1L, 2L, 4L, 9L), convert(builder.build()));
    }

    @Test
    public void testOverlappingValues()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 4L, 6L, 7L }));
        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 6L, 8L, 9L, 10L }));

        List<Long> values = convert(builder.build());

        assertEquals(values.toString(), convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), values);
    }

    @Test
    public void testNoOverlappingRanges()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 7L, 8L, 9L }));

        assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), convert(builder.build()));
    }

    @Test
    public void testTwoIteratorsWithSingleValues()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L }));
        builder.add(new LongIterator(new long[] { 1L }));

        assertEquals(convert(1L), convert(builder.build()));
    }

    @Test
    public void testDifferentSizeIterators()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L, 12L, 13L }));
        builder.add(new LongIterator(new long[] { 1L, 7L, 14L, 15 }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 8L, 9L, 10L }));

        assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 12L, 13L, 14L, 15L), convert(builder.build()));
    }

    @Test
    public void testRandomSequences()
    {
        long[][] values = new long[getRandom().nextIntBetween(1, 20)][];
        int numTests = getRandom().nextIntBetween(10, 20);

        for (int tests = 0; tests < numTests; tests++)
        {
            RangeUnionIterator.Builder builder = RangeUnionIterator.builder();
            int totalCount = 0;

            for (int i = 0; i < values.length; i++)
            {
                long[] part = new long[getRandom().nextIntBetween(1, 500)];
                for (int j = 0; j < part.length; j++)
                    part[j] = getRandom().nextLong();

                // all of the parts have to be sorted to mimic SSTable
                Arrays.sort(part);

                values[i] = part;
                builder.add(new LongIterator(part));
                totalCount += part.length;
            }

            long[] totalOrdering = new long[totalCount];
            int index = 0;

            for (long[] part : values)
            {
                for (long value : part)
                    totalOrdering[index++] = value;
            }

            Arrays.sort(totalOrdering);

            int count = 0;
            RangeIterator tokens = builder.build();

            Assert.assertNotNull(tokens);
            while (tokens.hasNext())
                assertEquals(totalOrdering[count++], tokens.next().token().getLongValue());

            assertEquals(totalCount, count);
        }
    }

    @Test
    public void testMinMaxAndCount()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 7L, 8L, 9L }));

        assertEquals(9L, builder.getMaximum().token().getLongValue());
        assertEquals(9L, builder.getTokenCount());

        RangeIterator tokens = builder.build();

        Assert.assertNotNull(tokens);
        assertEquals(1L, tokens.getMinimum().token().getLongValue());
        assertEquals(9L, tokens.getMaximum().token().getLongValue());
        assertEquals(9L, tokens.getCount());

        for (long i = 1; i < 10; i++)
        {
            Assert.assertTrue(tokens.hasNext());
            assertEquals(i, tokens.next().token().getLongValue());
        }

        Assert.assertFalse(tokens.hasNext());
        assertEquals(1L, tokens.getMinimum().token().getLongValue());
    }

    @Test
    public void testBuilder()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        Assert.assertNull(builder.getMinimum());
        Assert.assertNull(builder.getMaximum());
        assertEquals(0L, builder.getTokenCount());
        assertEquals(0L, builder.rangeCount());

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 7L, 8L, 9L }));

        assertEquals(1L, builder.getMinimum().token().getLongValue());
        assertEquals(9L, builder.getMaximum().token().getLongValue());
        assertEquals(9L, builder.getTokenCount());
        assertEquals(3L, builder.rangeCount());
        Assert.assertFalse(builder.statistics.isDisjoint());

        assertEquals(1L, builder.rangeIterators.get(0).getMinimum().token().getLongValue());
        assertEquals(4L, builder.rangeIterators.get(1).getMinimum().token().getLongValue());
        assertEquals(7L, builder.rangeIterators.get(2).getMinimum().token().getLongValue());

        RangeIterator tokens = RangeUnionIterator.build(new ArrayList<RangeIterator>()
        {{
            add(new LongIterator(new long[]{1L, 2L, 4L}));
            add(new LongIterator(new long[]{3L, 5L, 6L}));
        }});

        assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L), convert(tokens));

        FileUtils.closeQuietly(tokens);

        RangeIterator emptyTokens = RangeUnionIterator.builder().build();
        assertEquals(0, emptyTokens.getCount());

        builder = RangeUnionIterator.builder();
        assertEquals(0L, builder.add((RangeIterator) null).rangeCount());
        assertEquals(0L, builder.add((List<RangeIterator>) null).getTokenCount());
        assertEquals(0L, builder.add(new LongIterator(new long[] {})).rangeCount());

        RangeIterator single = new LongIterator(new long[] { 1L, 2L, 3L });
        RangeIterator range = RangeIntersectionIterator.builder().add(single).build();

        // because build should return first element if it's only one instead of building yet another iterator
        assertEquals(range, single);
    }

    @Test
    public void testSkipTo()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[]{1L, 2L, 3L}));
        builder.add(new LongIterator(new long[]{4L, 5L, 6L}));
        builder.add(new LongIterator(new long[]{7L, 8L, 9L}));

        RangeIterator tokens = builder.build();
        Assert.assertNotNull(tokens);

        tokens.skipTo(LongIterator.fromToken(5L));
        Assert.assertTrue(tokens.hasNext());
        assertEquals(5L, tokens.next().token().getLongValue());

        tokens.skipTo(LongIterator.fromToken(7L));
        Assert.assertTrue(tokens.hasNext());
        assertEquals(7L, tokens.next().token().getLongValue());

        tokens.skipTo(LongIterator.fromToken(10L));
        Assert.assertFalse(tokens.hasNext());
        assertEquals(1L, tokens.getMinimum().token().getLongValue());
        assertEquals(9L, tokens.getMaximum().token().getLongValue());
    }

    @Test
    public void testMergingMultipleIterators()
    {
        RangeUnionIterator.Builder builderA = RangeUnionIterator.builder();

        builderA.add(new LongIterator(new long[] { 1L, 3L, 5L }));
        builderA.add(new LongIterator(new long[] { 8L, 10L, 12L }));

        RangeUnionIterator.Builder builderB = RangeUnionIterator.builder();

        builderB.add(new LongIterator(new long[] { 7L, 9L, 11L }));
        builderB.add(new LongIterator(new long[] { 2L, 4L, 6L }));

        RangeIterator union = RangeUnionIterator.build(Arrays.asList(builderA.build(), builderB.build()));
        assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L), convert(union));
    }

    @Test
    public void testRangeIterator()
    {
        LongIterator tokens = new LongIterator(new long[] { 0L, 1L, 2L, 3L });

        assertEquals(0L, tokens.getMinimum().token().getLongValue());
        assertEquals(3L, tokens.getMaximum().token().getLongValue());

        for (int i = 0; i <= 3; i++)
        {
            Assert.assertTrue(tokens.hasNext());
            assertEquals(i, tokens.getCurrent().token().getLongValue());
            assertEquals(i, tokens.next().token().getLongValue());
        }

        tokens = new LongIterator(new long[] { 0L, 1L, 3L, 5L });

        assertEquals(3L, tokens.skipTo(LongIterator.fromToken(2L)).token().getLongValue());
        Assert.assertTrue(tokens.hasNext());
        assertEquals(3L, tokens.getCurrent().token().getLongValue());
        assertEquals(3L, tokens.next().token().getLongValue());

        assertEquals(5L, tokens.skipTo(LongIterator.fromToken(5L)).token().getLongValue());
        Assert.assertTrue(tokens.hasNext());
        assertEquals(5L, tokens.getCurrent().token().getLongValue());
        assertEquals(5L, tokens.next().token().getLongValue());

        LongIterator empty = new LongIterator(new long[0]);

        Assert.assertNull(empty.skipTo(LongIterator.fromToken(3L)));
        Assert.assertFalse(empty.hasNext());
    }

    @Test
    public void emptyRangeTest() {
        RangeIterator.Builder builder;
        RangeIterator range;
        // empty, then non-empty
        builder = RangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        range = builder.build();
        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(19L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        assertEquals(10, range.getCount());

        builder = RangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {10}));
        range = builder.build();
        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(10L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        assertEquals(1, range.getCount());

        // non-empty, then empty
        builder = RangeUnionIterator.builder();
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        builder.add(new LongIterator(new long[] {}));
        range = builder.build();
        assertEquals(10, range.getMinimum().token().getLongValue());
        assertEquals(19, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        assertEquals(10, range.getCount());

        builder = RangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {10}));
        builder.add(new LongIterator(new long[] {}));
        range = builder.build();
        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(10L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        assertEquals(1, range.getCount());

        // empty, then non-empty then empty again
        builder = RangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        builder.add(new LongIterator(new long[] {}));
        range = builder.build();
        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(19L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        assertEquals(10, range.getCount());

        // non-empty, empty, then non-empty again
        builder = RangeUnionIterator.builder();
        for (int i = 0; i < 5; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        builder.add(new LongIterator(new long[] {}));
        for (int i = 5; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        range = builder.build();
        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(19L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        assertEquals(10, range.getCount());
    }

    // SAI specific tests
    @Test
    public void testUnionOfIntersection()
    {
        // union of two non-intersected intersections
        RangeIterator intersectionA = buildIntersection(arr(1L, 2L, 3L), arr(4L, 5L, 6L));
        RangeIterator intersectionB = buildIntersection(arr(6L, 7L, 8L), arr(9L, 10L, 11L));

        RangeIterator union = buildUnion(intersectionA, intersectionB);
        assertEquals(convert(), convert(union));

        // union of two intersected intersections
        intersectionA = buildIntersection(arr(1L, 2L, 3L), arr(2L, 3L, 4L));
        intersectionB = buildIntersection(arr(6L, 7L, 8L), arr(7L, 8L, 9L));

        union = buildUnion(intersectionA, intersectionB);
        assertEquals(convert(2L, 3L, 7L, 8L), convert(union));
        assertEquals(RangeUnionIterator.class, union.getClass());

        // union of one intersected intersection and one non-intersected intersection
        intersectionA = buildIntersection(arr(1L, 2L, 3L), arr(2L, 3L, 4L ));
        intersectionB = buildIntersection(arr(6L, 7L, 8L), arr(10L ));

        union = buildUnion(intersectionA, intersectionB);
        assertEquals(convert(2L, 3L), convert(union));
    }

    @Test
    public void testUnionOnError()
    {
        assertOnError(buildOnError(UNION, arr(1L, 3L, 4L ), arr(7L, 8L)));
        assertOnError(buildOnErrorA(UNION, arr(1L, 3L, 4L ), arr(4L, 5L)));
        assertOnError(buildOnErrorB(UNION, arr(1L), arr(2)));
    }

    @Test
    public void testUnionOfIntersectionsOnError()
    {
        RangeIterator intersectionA = buildIntersection(arr(1L, 2L, 3L, 6L), arr(2L, 3L, 6L));
        RangeIterator intersectionB = buildOnErrorA(INTERSECTION, arr(2L, 4L, 6L), arr(5L, 6L, 7L, 9L));
        assertOnError(buildUnion(intersectionA, intersectionB));

        intersectionA = buildOnErrorB(INTERSECTION, arr(1L, 2L, 3L, 4L, 5L), arr(2L, 3L, 5L));
        intersectionB = buildIntersection(arr(2L, 4L, 5L), arr(5L, 6L, 7L));
        assertOnError(buildUnion(intersectionA, intersectionB));
    }

    @Test
    public void testUnionOfUnionsOnError()
    {
        RangeIterator unionA = buildUnion(arr(1L, 2L, 3L, 6L), arr(6L, 7L, 8L));
        RangeIterator unionB = buildOnErrorA(UNION, arr(2L, 4L, 6L), arr (6L, 7L, 9L));
        assertOnError(buildUnion(unionA, unionB));

        unionA = buildOnErrorB(UNION, arr(1L, 2L, 3L), arr(3L, 7L, 8L));
        unionB = buildUnion(arr(2L, 4L, 5L), arr (5L, 7L, 9L));
        assertOnError(buildUnion(unionA, unionB));
    }

    @Test
    public void testUnionOfMergingOnError()
    {
        RangeIterator mergingA = buildConcat(arr(1L, 2L, 3L, 6L), arr(6L, 7L, 8L));
        RangeIterator mergingB = buildOnErrorA(CONCAT, arr(2L, 4L, 6L), arr (6L, 7L, 9L));
        assertOnError(buildUnion(mergingA, mergingB));

        mergingA = buildOnErrorB(CONCAT, arr(1L, 2L, 3L), arr(3L, 7L, 8L));
        mergingB = buildConcat(arr(2L, 4L, 5L), arr (5L, 7L, 9L));
        assertOnError(buildUnion(mergingA, mergingB));
    }
}
