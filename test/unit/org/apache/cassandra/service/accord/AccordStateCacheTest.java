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

package org.apache.cassandra.service.accord;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.service.accord.AccordStateCache.Node;

public class AccordStateCacheTest
{
    private static final long DEFAULT_ITEM_SIZE = 100;
    private static final long KEY_SIZE = 4;
    private static final long DEFAULT_NODE_SIZE = nodeSize(DEFAULT_ITEM_SIZE);

    private static class Item implements AccordStateCache.AccordState<Integer, Item>
    {
        long size = DEFAULT_ITEM_SIZE;

        final Integer key;

        public Item(Integer key)
        {
            this.key = key;
        }

        @Override
        public Node<Integer, Item> createNode()
        {
            return new Node<>(this)
            {
                @Override
                long sizeInBytes(Item value)
                {
                    return size;
                }
            };
        }

        @Override
        public Integer key()
        {
            return key;
        }
    }

    private static long nodeSize(long itemSize)
    {
        return itemSize + KEY_SIZE + Node.EMPTY_SIZE;
    }

    private static void assertCacheState(AccordStateCache<?, ?> cache, int active, int cached, long bytes)
    {
        Assert.assertEquals(active, cache.numActiveEntries());
        Assert.assertEquals(cached, cache.numCachedEntries());
        Assert.assertEquals(bytes, cache.bytesCached());
    }

    @Test
    public void testAcquisitionAndRelease()
    {
        AccordStateCache<Integer, Item> cache = new AccordStateCache<>(Item::new, 500);
        assertCacheState(cache, 0, 0, 0);

        Item item1 = cache.acquire(1);
        assertCacheState(cache, 1, 0, DEFAULT_NODE_SIZE);
        Assert.assertNull(cache.head);
        Assert.assertNull(cache.tail);

        item1.size = 110;
        cache.release(1, item1);
        assertCacheState(cache, 0, 1, nodeSize(110));
        Assert.assertSame(item1, cache.tail.value);
        Assert.assertSame(item1, cache.head.value);

        Item item2 = cache.acquire(2);
        assertCacheState(cache, 1, 1, DEFAULT_NODE_SIZE + nodeSize(110));
        cache.release(2, item2);
        assertCacheState(cache, 0, 2, DEFAULT_NODE_SIZE + nodeSize(110));

        Assert.assertSame(item1, cache.tail.value);
        Assert.assertSame(item2, cache.head.value);
    }

    @Test
    public void testRotation()
    {
        AccordStateCache<Integer, Item> cache = new AccordStateCache<>(Item::new, DEFAULT_NODE_SIZE * 5);
        assertCacheState(cache, 0, 0, 0);

        Item[] items = new Item[3];
        for (int i=0; i<3; i++)
        {
            Item item = cache.acquire(i);
            items[i] = item;
            cache.release(i, item);
        }

        Assert.assertSame(items[0], cache.tail.value);
        Assert.assertSame(items[2], cache.head.value);
        assertCacheState(cache, 0, 3, DEFAULT_NODE_SIZE * 3);

        Item item = cache.acquire(1);
        assertCacheState(cache, 1, 2, DEFAULT_NODE_SIZE * 3);

        // releasing item should return it to the head
        cache.release(1, item);
        assertCacheState(cache, 0, 3, DEFAULT_NODE_SIZE * 3);
        Assert.assertSame(items[0], cache.tail.value);
        Assert.assertSame(items[1], cache.head.value);
    }

    @Test
    public void testEvictionOnAcquire()
    {
        AccordStateCache<Integer, Item> cache = new AccordStateCache<>(Item::new, DEFAULT_NODE_SIZE * 5);
        assertCacheState(cache, 0, 0, 0);

        Item[] items = new Item[5];
        for (int i=0; i<5; i++)
        {
            Item item = cache.acquire(i);
            items[i] = item;
            cache.release(i, item);
        }

        assertCacheState(cache, 0, 5, DEFAULT_NODE_SIZE * 5);
        Assert.assertSame(items[0], cache.tail.value);
        Assert.assertSame(items[4], cache.head.value);

        cache.acquire(5);
        assertCacheState(cache, 1, 4, DEFAULT_NODE_SIZE * 5);
        Assert.assertSame(items[1], cache.tail.value);
        Assert.assertSame(items[4], cache.head.value);
        Assert.assertFalse(cache.keyIsCached(0));
        Assert.assertFalse(cache.keyIsActive(0));
    }

    @Test
    public void testEvictionOnRelease()
    {
        AccordStateCache<Integer, Item> cache = new AccordStateCache<>(Item::new, DEFAULT_NODE_SIZE * 4);
        assertCacheState(cache, 0, 0, 0);

        Item[] items = new Item[5];
        for (int i=0; i<5; i++)
        {
            Item item = cache.acquire(i);
            items[i] = item;
        }

        assertCacheState(cache, 5, 0, DEFAULT_NODE_SIZE * 5);
        Assert.assertNull(cache.head);
        Assert.assertNull(cache.tail);

        cache.release(2, items[2]);
        assertCacheState(cache, 4, 0, DEFAULT_NODE_SIZE * 4);
        Assert.assertNull(cache.head);
        Assert.assertNull(cache.tail);

        cache.release(4, items[4]);
        assertCacheState(cache, 3, 1, DEFAULT_NODE_SIZE * 4);
        Assert.assertSame(items[4], cache.tail.value);
        Assert.assertSame(items[4], cache.head.value);
    }

    @Test
    public void testAcquisitionFailure()
    {
        AccordStateCache<Integer, Item> cache = new AccordStateCache<>(Item::new, DEFAULT_NODE_SIZE * 4);
        assertCacheState(cache, 0, 0, 0);

        Assert.assertNotNull(cache.acquire(0));
        assertCacheState(cache, 1, 0, DEFAULT_NODE_SIZE);

        Assert.assertNull(cache.acquire(0));
        assertCacheState(cache, 1, 0, DEFAULT_NODE_SIZE);
    }
}
