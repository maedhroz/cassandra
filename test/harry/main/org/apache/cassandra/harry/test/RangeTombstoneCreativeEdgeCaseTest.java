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

package org.apache.cassandra.harry.test;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.checker.ModelChecker;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.HistoryBuilderHelper;
import org.apache.cassandra.harry.execution.CQLTesterVisitExecutor;
import org.apache.cassandra.harry.execution.CQLVisitExecutor;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

/**
 * Creative edge case tests that target subtle, rarely-exercised interactions
 * between range tombstones and the SSTable read/write/compaction machinery.
 *
 * These tests go beyond basic "insert-delete-compact-validate" by targeting:
 *
 *   - Reverse iteration through index blocks with range tombstones
 *     (SSTableReversedIterator's skipFirstIteratedItem/skipLastIteratedItem logic)
 *   - Slice queries that partially overlap with range tombstones
 *     (AbstractSSTableIterator.handlePreSliceData boundary marker deduplication)
 *   - Static columns surviving range tombstones (separate merge path)
 *   - Mixed partition sizes in the same SSTable (small partitions adjacent
 *     to large ones with range tombstones)
 *   - Memtable range tombstones shadowing SSTable data (pre-flush reads)
 *   - Range tombstone covering entire clustering range vs partition delete
 *   - Interleaved ASC/DESC reads exercising both forward and reverse iterators
 *   - Extreme partition shapes: single-row partitions with range tombstones,
 *     very sparse partitions, partitions with only static data + range tombstones
 */
public class RangeTombstoneCreativeEdgeCaseTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(RangeTombstoneCreativeEdgeCaseTest.class);

    private static final int LARGE_PARTITION = 500;

    private int savedColumnIndexSizeInKiB;
    private int savedColumnIndexCacheSizeInKiB;

    @Before
    public void saveConfig()
    {
        savedColumnIndexSizeInKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        savedColumnIndexCacheSizeInKiB = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
    }

    @After
    public void restoreConfig()
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(savedColumnIndexSizeInKiB);
        DatabaseDescriptor.setColumnIndexCacheSize(savedColumnIndexCacheSizeInKiB);
    }

    // ========================================================================
    // Schema generators
    // ========================================================================

    private final Generator<SchemaSpec> ascSchema = rng -> new SchemaSpec(
        rng.next(), 1000, KEYSPACE,
        "rt_creative_" + rng.nextLong(0, Long.MAX_VALUE),
        Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.int64Type)),
        Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.int64Type, false)),
        Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.int64Type),
                      ColumnSpec.regularColumn("v2", ColumnSpec.asciiType),
                      ColumnSpec.regularColumn("v3", ColumnSpec.int32Type)),
        Arrays.asList(ColumnSpec.staticColumn("s1", ColumnSpec.int64Type),
                      ColumnSpec.staticColumn("s2", ColumnSpec.asciiType)));

    private final Generator<SchemaSpec> descSchema = rng -> new SchemaSpec(
        rng.next(), 1000, KEYSPACE,
        "rt_creative_" + rng.nextLong(0, Long.MAX_VALUE),
        Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.int64Type)),
        Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.int64Type, true)),
        Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.int64Type),
                      ColumnSpec.regularColumn("v2", ColumnSpec.asciiType),
                      ColumnSpec.regularColumn("v3", ColumnSpec.int32Type)),
        Arrays.asList(ColumnSpec.staticColumn("s1", ColumnSpec.int64Type),
                      ColumnSpec.staticColumn("s2", ColumnSpec.asciiType)));

    private final Generator<SchemaSpec> multiCkMixed = rng -> new SchemaSpec(
        rng.next(), 1000, KEYSPACE,
        "rt_creative_" + rng.nextLong(0, Long.MAX_VALUE),
        Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.int64Type)),
        Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.int64Type, false),
                      ColumnSpec.ck("ck2", ColumnSpec.int64Type, true)),
        Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.int64Type),
                      ColumnSpec.regularColumn("v2", ColumnSpec.asciiType)),
        Arrays.asList(ColumnSpec.staticColumn("s1", ColumnSpec.int64Type)));

    // ========================================================================
    // Table creation helpers
    // ========================================================================

    private void createAggressive(SchemaSpec schema)
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        DatabaseDescriptor.setColumnIndexCacheSize(1);
        String cql = schema.compile().replace(";", "");
        if (cql.contains(" WITH "))
            cql += " AND gc_grace_seconds = 0"
                   + " AND compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}"
                   + " AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': false};";
        else
            cql += " WITH gc_grace_seconds = 0"
                   + " AND compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}"
                   + " AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': false};";
        createTable(cql);
    }

    // ========================================================================
    // Tests: Reverse iteration with range tombstones
    // ========================================================================

    /**
     * Reverse read (ORDER BY ck1 DESC) through a large partition where a
     * range tombstone spans many index blocks.
     *
     * SSTableReversedIterator loads blocks in reverse. When it encounters an
     * endOpenMarker on block N-1, it must emit a synthetic opening marker at
     * the start of block N (in reverse order). With tiny index blocks, this
     * happens on nearly every block transition.
     *
     * The test:
     *   1. Insert 500 rows, flush
     *   2. Delete range [100..400], flush
     *   3. Re-insert rows [200..300], flush
     *   4. Read in DESC order (reverse iteration) -> validate
     *   5. Compact
     *   6. Read in DESC order again -> validate
     *
     * The DESC read forces SSTableReversedIterator to reconstruct range
     * tombstone state backwards through index blocks, and the re-inserts
     * in the middle of the range tombstone create a complex interleaving.
     */
    @Test
    public void testReverseIterationAcrossIndexBlocks()
    {
        withRandom(rng -> {
            SchemaSpec schema = ascSchema.generate(rng);
            createAggressive(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            for (int i = 0; i < LARGE_PARTITION; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush data");

            history.deleteRowRange(0, 100, 400, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush range delete");

            for (int i = 200; i <= 300; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush re-inserts");

            // DESC read before compaction
            history.selectPartition(0, Operations.ClusteringOrderBy.DESC);

            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact");

            // DESC read after compaction
            history.selectPartition(0, Operations.ClusteringOrderBy.DESC);

            replay(schema, history);
        });
    }

    /**
     * DESC-ordered table with reverse read of range-tombstoned data.
     *
     * With a DESC clustering key, the "natural" read order is already reversed.
     * Reading with ClusteringOrderBy.ASC on a DESC table uses ForwardReader,
     * but reading with ClusteringOrderBy.DESC uses the ReverseReader.
     *
     * This test issues BOTH ASC and DESC reads after every mutation and flush
     * to validate that both iteration directions produce the same logical result.
     */
    @Test
    public void testBothOrdersDescTable()
    {
        withRandom(rng -> {
            SchemaSpec schema = descSchema.generate(rng);
            createAggressive(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            for (int i = 0; i < LARGE_PARTITION; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush data");

            // ASC and DESC reads before any deletions
            history.selectPartition(0, Operations.ClusteringOrderBy.ASC);
            history.selectPartition(0, Operations.ClusteringOrderBy.DESC);

            // Range delete
            history.deleteRowRange(0, 50, 350, 0, true, false);
            history.customThrowing(() -> flushCfs(schema), "flush range delete");

            // Both reads after deletion
            history.selectPartition(0, Operations.ClusteringOrderBy.ASC);
            history.selectPartition(0, Operations.ClusteringOrderBy.DESC);

            // Re-insert scattered rows
            for (int i = 100; i <= 300; i += 7)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush scattered re-inserts");

            history.selectPartition(0, Operations.ClusteringOrderBy.ASC);
            history.selectPartition(0, Operations.ClusteringOrderBy.DESC);

            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact");

            history.selectPartition(0, Operations.ClusteringOrderBy.ASC);
            history.selectPartition(0, Operations.ClusteringOrderBy.DESC);

            replay(schema, history);
        });
    }

    // ========================================================================
    // Tests: Slice queries partially overlapping with range tombstones
    // ========================================================================

    /**
     * Slice query whose bounds partially overlap with a range tombstone.
     *
     * Query: selectRowRange [150..350]
     * Range tombstone: [100..250]
     * Overlap region: [150..250]
     *
     * The AbstractSSTableIterator.handlePreSliceData() must correctly handle
     * the case where the range tombstone's OPEN marker is BEFORE the slice
     * start. It must emit a synthetic open marker at the slice start with the
     * active deletion time. If this is wrong, rows [150..250] will incorrectly
     * survive.
     */
    @Test
    public void testSliceQueryPartiallyOverlapsRangeTombstone()
    {
        withRandom(rng -> {
            SchemaSpec schema = ascSchema.generate(rng);
            createAggressive(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            for (int i = 0; i < LARGE_PARTITION; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush data");

            // Range tombstone [100..250]
            history.deleteRowRange(0, 100, 250, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush range delete");

            // Slice query [150..350] - partially overlaps tombstone
            history.selectRowRange(0, 150, 350, 0, true, true);

            // Slice query [50..200] - starts before, ends within tombstone
            history.selectRowRange(0, 50, 200, 0, true, true);

            // Slice query [100..250] - exactly matches tombstone bounds
            history.selectRowRange(0, 100, 250, 0, true, true);

            // Slice query [100..250] with exclusive bounds
            history.selectRowRange(0, 100, 250, 0, false, false);

            // Full partition for reference
            history.selectPartition(0);

            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact");

            // Repeat all queries after compaction
            history.selectRowRange(0, 150, 350, 0, true, true);
            history.selectRowRange(0, 50, 200, 0, true, true);
            history.selectRowRange(0, 100, 250, 0, true, true);
            history.selectRowRange(0, 100, 250, 0, false, false);
            history.selectPartition(0);

            replay(schema, history);
        });
    }

    /**
     * Multiple overlapping range tombstones with slice queries that cut
     * through them at different positions. Combined with re-inserts to
     * create a complex mosaic of live and dead data.
     */
    @Test
    public void testSliceQueriesThroughMultipleOverlappingRangeTombstones()
    {
        withRandom(rng -> {
            SchemaSpec schema = ascSchema.generate(rng);
            createAggressive(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            for (int i = 0; i < LARGE_PARTITION; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush data");

            // Three overlapping range tombstones
            history.deleteRowRange(0, 50, 200, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush RT1 [50..200]");

            history.deleteRowRange(0, 150, 350, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush RT2 [150..350]");

            history.deleteRowRange(0, 300, 450, 0, false, false);
            history.customThrowing(() -> flushCfs(schema), "flush RT3 (300..450)");

            // Re-insert scattered rows across all tombstone regions
            for (int i = 0; i < LARGE_PARTITION; i += 3)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush scattered re-inserts");

            // Slice queries that cut at tombstone boundaries
            history.selectRowRange(0, 0, 100, 0, true, true);     // spans RT1 start
            history.selectRowRange(0, 100, 200, 0, true, true);   // inside RT1, spans RT2 start
            history.selectRowRange(0, 175, 325, 0, true, true);   // spans RT1-RT2 overlap and RT3 start
            history.selectRowRange(0, 350, 499, 0, true, true);   // spans RT3 end
            history.selectRowRange(0, 0, 499, 0, true, true);     // entire partition
            history.selectPartition(0);

            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact");

            // Same slice queries after compaction
            history.selectRowRange(0, 0, 100, 0, true, true);
            history.selectRowRange(0, 100, 200, 0, true, true);
            history.selectRowRange(0, 175, 325, 0, true, true);
            history.selectRowRange(0, 350, 499, 0, true, true);
            history.selectRowRange(0, 0, 499, 0, true, true);
            history.selectPartition(0);

            replay(schema, history);
        });
    }

    // ========================================================================
    // Tests: Static columns surviving range tombstones
    // ========================================================================

    /**
     * Range tombstone must NOT delete static columns. After deleting all
     * clustering rows via range tombstone, the static columns must still
     * be readable. This tests the separate merge path for static rows
     * (UnfilteredRowIterators.mergeStaticRows uses only partitionDeletion,
     * never range tombstone markers).
     *
     * Sequence:
     *   1. Insert rows 0..49 (which also sets static column values)
     *   2. Flush
     *   3. Delete range [0..49] inclusive (all rows)
     *   4. Flush
     *   5. Read -> static columns must survive, regular rows must be gone
     *   6. Compact
     *   7. Read again -> same result
     */
    @Test
    public void testStaticColumnsSurviveRangeTombstone()
    {
        withRandom(rng -> {
            SchemaSpec schema = ascSchema.generate(rng);
            createAggressive(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // Insert with static values
            for (int i = 0; i < 50; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush data");

            // Range delete all rows
            history.deleteRowRange(0, 0, 49, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush range delete");

            // Static columns should survive
            history.selectPartition(0);

            // Re-insert a single row (also updates static columns)
            history.insert(0, 25);
            history.customThrowing(() -> flushCfs(schema), "flush single re-insert");

            history.selectPartition(0);

            // Now partition delete (this DOES kill static columns)
            history.deletePartition(0);
            history.customThrowing(() -> flushCfs(schema), "flush partition delete");

            history.selectPartition(0);

            // Final insert
            history.insert(0, 10);
            history.customThrowing(() -> flushCfs(schema), "flush final insert");

            history.selectPartition(0);

            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact");

            history.selectPartition(0);

            replay(schema, history);
        });
    }

    /**
     * Static-only writes followed by range tombstones.
     *
     * A partition can have static data with NO clustering rows. A range
     * tombstone should be a no-op in this case (nothing to delete), but
     * the static data must still be visible.
     */
    @Test
    public void testStaticOnlyPartitionWithRangeTombstone()
    {
        withRandom(rng -> {
            SchemaSpec schema = ascSchema.generate(rng);
            createAggressive(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // Insert rows (which set static columns), then delete all rows
            // leaving only static data
            for (int i = 0; i < 5; i++)
                history.insert(0, i);
            history.deleteRowRange(0, 0, 4, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush data + range delete");

            // Static-only partition should be visible
            history.selectPartition(0);

            // In a different partition, insert just one row + range delete it
            history.insert(1, 0);
            history.deleteRowRange(1, 0, 0, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush partition 1");

            history.selectPartition(1);

            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact");

            history.selectPartition(0);
            history.selectPartition(1);

            replay(schema, history);
        });
    }

    // ========================================================================
    // Tests: Mixed partition sizes
    // ========================================================================

    /**
     * Same SSTable contains a tiny partition (1 row) and a huge partition
     * (500 rows) with range tombstones. The tiny partition does NOT produce
     * index blocks (non-indexed RowIndexEntry), while the huge one produces
     * many (indexed RowIndexEntry). The compaction iterator must correctly
     * switch between non-indexed and indexed readers as it moves between
     * partitions.
     */
    @Test
    public void testMixedPartitionSizesWithRangeTombstones()
    {
        withRandom(rng -> {
            SchemaSpec schema = ascSchema.generate(rng);
            createAggressive(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // Tiny partition 0: single row
            history.insert(0, 0);

            // Huge partition 1: 500 rows
            for (int i = 0; i < LARGE_PARTITION; i++)
                history.insert(1, i);

            // Tiny partition 2: two rows
            history.insert(2, 0);
            history.insert(2, 1);

            // Huge partition 3: 500 rows
            for (int i = 0; i < LARGE_PARTITION; i++)
                history.insert(3, i);

            history.customThrowing(() -> flushCfs(schema), "flush all partitions");

            // Range tombstones in both tiny and huge partitions
            history.deleteRowRange(0, 0, 0, 0, true, true);  // deletes the only row
            history.deleteRowRange(1, 100, 400, 0, true, true);
            history.deleteRowRange(2, 0, 1, 0, true, true);  // deletes both rows
            history.deleteRowRange(3, 0, 499, 0, false, false);  // exclusive: rows 0 and 499 survive
            history.customThrowing(() -> flushCfs(schema), "flush range deletes");

            for (int p = 0; p < 4; p++)
                history.selectPartition(p);

            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact");

            for (int p = 0; p < 4; p++)
                history.selectPartition(p);

            replay(schema, history);
        });
    }

    // ========================================================================
    // Tests: Memtable range tombstone shadowing SSTable data
    // ========================================================================

    /**
     * Range tombstone in memtable shadows data in SSTable, validated BEFORE
     * flush. This exercises the UnfilteredRowMergeIterator path that merges
     * memtable and SSTable iterators, where the RangeTombstoneMarker.Merger
     * must track the memtable's range tombstone as the "biggest open marker."
     *
     * After validation from memtable, flush and validate again (now both are
     * SSTables), then compact and validate a third time.
     */
    @Test
    public void testMemtableRangeTombstoneShadowsSSTableData()
    {
        withRandom(rng -> {
            SchemaSpec schema = ascSchema.generate(rng);
            createAggressive(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // SSTable: data
            for (int i = 0; i < LARGE_PARTITION; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush data to SSTable");

            // Memtable: range tombstone (NOT flushed yet)
            history.deleteRowRange(0, 100, 400, 0, true, true);

            // Validate while tombstone is still in memtable
            // This exercises memtable + SSTable merge path
            history.selectPartition(0);
            history.selectRowRange(0, 50, 200, 0, true, true);
            history.selectRowRange(0, 150, 450, 0, true, true);
            history.selectPartition(0, Operations.ClusteringOrderBy.DESC);

            // Now flush the tombstone
            history.customThrowing(() -> flushCfs(schema), "flush range tombstone");

            // Validate with both SSTables
            history.selectPartition(0);
            history.selectPartition(0, Operations.ClusteringOrderBy.DESC);

            // Compact
            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact");

            // Validate after compaction
            history.selectPartition(0);
            history.selectPartition(0, Operations.ClusteringOrderBy.DESC);

            replay(schema, history);
        });
    }

    /**
     * Multiple memtable flushes interleaved with reads, where each memtable
     * contributes a different range tombstone. Reads between flushes exercise
     * the merge of N memtables (if multiple are pending) and M SSTables.
     */
    @Test
    public void testInterleavedMemtableFlushesWithReads()
    {
        withRandom(rng -> {
            SchemaSpec schema = ascSchema.generate(rng);
            createAggressive(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // SSTable: initial data
            for (int i = 0; i < LARGE_PARTITION; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush data");

            // Cycle: add range tombstone -> read (memtable + SSTable) -> flush -> read
            for (int cycle = 0; cycle < 8; cycle++)
            {
                int lo = cycle * 50;
                int hi = lo + 60;  // overlapping ranges

                // Range tombstone goes into memtable
                history.deleteRowRange(0, lo, Math.min(hi, 499), 0,
                                       cycle % 2 == 0, cycle % 3 != 0);

                // Read while tombstone is in memtable
                history.selectPartition(0);

                // Some re-inserts into memtable
                for (int i = lo; i <= Math.min(lo + 10, 499); i++)
                    history.insert(0, i);

                // Read again (memtable has both inserts and range tombstone)
                history.selectPartition(0);

                // Flush
                history.customThrowing(() -> flushCfs(schema), "flush cycle " + cycle);

                // Read from SSTables
                history.selectPartition(0);
            }

            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact");
            history.selectPartition(0);
            history.selectPartition(0, Operations.ClusteringOrderBy.DESC);

            replay(schema, history);
        });
    }

    // ========================================================================
    // Tests: Range tombstone covering entire partition vs partition delete
    // ========================================================================

    /**
     * A range tombstone from row 0 to row MAX_IDX (covering all possible
     * clustering values that were inserted) is semantically equivalent to
     * "delete all clustering rows but keep static columns."
     *
     * A partition delete, by contrast, also kills static columns.
     *
     * This test verifies both behaviors are correct by applying one to
     * partition 0 and the other to partition 1, then validating.
     */
    @Test
    public void testFullRangeTombstoneVsPartitionDelete()
    {
        withRandom(rng -> {
            SchemaSpec schema = ascSchema.generate(rng);
            createAggressive(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // Insert same data into partitions 0 and 1
            for (int i = 0; i < 100; i++)
            {
                history.insert(0, i);
                history.insert(1, i);
            }
            history.customThrowing(() -> flushCfs(schema), "flush data");

            // Partition 0: full-range tombstone (static columns survive)
            history.deleteRowRange(0, 0, 99, 0, true, true);

            // Partition 1: partition delete (static columns die)
            history.deletePartition(1);

            history.customThrowing(() -> flushCfs(schema), "flush deletes");

            // Validate: partition 0 should have static columns, partition 1 should be empty
            history.selectPartition(0);
            history.selectPartition(1);

            // Re-insert into both
            history.insert(0, 50);
            history.insert(1, 50);
            history.customThrowing(() -> flushCfs(schema), "flush re-inserts");

            history.selectPartition(0);
            history.selectPartition(1);

            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact");

            history.selectPartition(0);
            history.selectPartition(1);

            replay(schema, history);
        });
    }

    // ========================================================================
    // Tests: Composite clustering keys with mixed ASC/DESC
    // ========================================================================

    /**
     * Table with (ck1 ASC, ck2 DESC). Range tombstones with nonEqFrom=0
     * (range on ck1) and nonEqFrom=1 (ck1 pinned, range on ck2 in reverse
     * order) exercise different comparator paths.
     *
     * The mixed ordering means the ClusteringComparator uses reversed
     * comparison for ck2 but not ck1, which creates subtle ordering issues
     * in the range tombstone bound comparisons.
     */
    @Test
    public void testMixedAscDescClusteringRangeTombstones()
    {
        withRandom(rng -> {
            SchemaSpec schema = multiCkMixed.generate(rng);
            createAggressive(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            int maxCk = Math.min(200, schema.valueGenerators.ckPopulation());
            for (int i = 0; i < maxCk; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush data");

            // Range delete on ck1 (ASC) - nonEqFrom=0
            history.deleteRowRange(0, 20, 80, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush RT on ck1");

            // Range delete on ck2 (DESC) - nonEqFrom=1
            history.deleteRowRange(0, 100, 150, 1, false, true);
            history.customThrowing(() -> flushCfs(schema), "flush RT on ck2");

            history.selectPartition(0);
            history.selectPartition(0, Operations.ClusteringOrderBy.DESC);

            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact");

            history.selectPartition(0);
            history.selectPartition(0, Operations.ClusteringOrderBy.DESC);

            replay(schema, history);
        });
    }

    // ========================================================================
    // Tests: Sparse partition edge cases
    // ========================================================================

    /**
     * Very sparse partition: only rows at indices 0, 100, 200, 300, 400
     * exist. A range tombstone [50..350] covers the middle three rows.
     * With tiny index blocks, the gap between rows means some index blocks
     * contain only the range tombstone markers and no actual row data.
     *
     * This exercises the iterator's ability to handle "empty" index blocks
     * that contain only range tombstone state.
     */
    @Test
    public void testSparsePartitionWithRangeTombstone()
    {
        withRandom(rng -> {
            SchemaSpec schema = ascSchema.generate(rng);
            createAggressive(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // Very sparse data
            for (int i = 0; i < 500; i += 100)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush sparse data");

            // Range tombstone covering middle rows
            history.deleteRowRange(0, 50, 350, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush range delete");

            history.selectPartition(0);
            history.selectPartition(0, Operations.ClusteringOrderBy.DESC);

            // Slice query that falls entirely within the range tombstone
            // but between two sparse rows
            history.selectRowRange(0, 150, 250, 0, true, true);

            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact");

            history.selectPartition(0);
            history.selectPartition(0, Operations.ClusteringOrderBy.DESC);
            history.selectRowRange(0, 150, 250, 0, true, true);

            replay(schema, history);
        });
    }

    // ========================================================================
    // Tests: Burn test with creative operations
    // ========================================================================

    /**
     * Burn test combining all creative edge cases: DESC reads, slice queries,
     * static columns, mixed partition sizes, open-ended slices, and all four
     * inclusivity combinations.
     *
     * Uses tiny index blocks and gc_grace=0 to maximize stress.
     */
    @Test
    public void testCreativeBurnTest()
    {
        Generator<SchemaSpec> randomSchemaGen = SchemaGenerators.schemaSpecGen(KEYSPACE, "rt_creative_burn", 100);
        withRandom(598635288979916L, rng -> {
            SchemaSpec schema = randomSchemaGen.generate(rng);
            createAggressive(schema);

            int maxPk = Math.min(5, schema.valueGenerators.pkPopulation());
            int maxCk = Math.min(100, schema.valueGenerators.ckPopulation());

            Generator<Integer> pkGen = Generators.int32(0, maxPk);
            Generator<Integer> ckGen = Generators.int32(0, maxCk);

            ModelChecker<HistoryBuilder, Void> mc = new ModelChecker<>();
            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            mc.init(history)
              // Insert
              .step((h, r) -> HistoryBuilderHelper.insertRandomData(schema, pkGen, ckGen, rng, h))
              // Range delete with random bounds
              .step((h, r) -> rng.nextDouble() >= 0.85,
                    (h, r) -> h.deleteRowRange(pkGen.generate(rng),
                                               ckGen.generate(rng), ckGen.generate(rng),
                                               rng.nextInt(schema.clusteringKeys.size()),
                                               rng.nextBoolean(), rng.nextBoolean()))
              // Open-ended lower bound delete
              .step((h, r) -> rng.nextDouble() >= 0.95,
                    (h, r) -> h.deleteRowSliceByLowerBound(pkGen.generate(rng),
                                                           ckGen.generate(rng),
                                                           rng.nextInt(schema.clusteringKeys.size()),
                                                           rng.nextBoolean()))
              // Open-ended upper bound delete
              .step((h, r) -> rng.nextDouble() >= 0.95,
                    (h, r) -> h.deleteRowSliceByUpperBound(pkGen.generate(rng),
                                                           ckGen.generate(rng),
                                                           rng.nextInt(schema.clusteringKeys.size()),
                                                           rng.nextBoolean()))
              // Row delete
              .step((h, r) -> rng.nextDouble() >= 0.92,
                    (h, r) -> h.deleteRow(pkGen.generate(rng), ckGen.generate(rng)))
              // Partition delete (rare)
              .step((h, r) -> rng.nextDouble() >= 0.99,
                    (h, r) -> h.deletePartition(pkGen.generate(rng)))
              // Column delete
              .step((h, r) -> rng.nextDouble() >= 0.93,
                    (h, r) -> HistoryBuilderHelper.deleteRandomColumns(
                        schema, pkGen.generate(rng), ckGen.generate(rng), rng, h))
              // Flush
              .step((h, r) -> rng.nextDouble() >= 0.88,
                    (h, r) -> h.custom(() -> flush(schema.keyspace, schema.table), "FLUSH"))
              // Compact
              .step((h, r) -> rng.nextDouble() >= 0.97,
                    (h, r) -> h.custom(() -> compact(schema.keyspace, schema.table), "COMPACT"))
              // ASC select
              .step((h, r) -> h.selectPartition(pkGen.generate(rng)))
              // DESC select
              .step((h, r) -> rng.nextDouble() >= 0.7,
                    (h, r) -> h.selectPartition(pkGen.generate(rng), Operations.ClusteringOrderBy.DESC))
              // Slice query
              .step((h, r) -> h.selectRowRange(pkGen.generate(rng),
                                               ckGen.generate(rng), ckGen.generate(rng),
                                               rng.nextInt(schema.clusteringKeys.size()),
                                               rng.nextBoolean(), rng.nextBoolean()))
              // Single row select
              .step((h, r) -> h.selectRow(pkGen.generate(rng), ckGen.generate(rng)))
              .exitCondition((h) -> {
                  if (history.size() < 5000)
                      return false;

                  dropTable("DROP TABLE " + schema.keyspace + "." + schema.table);
                  createTable(schema.compile().replace(";", " AND gc_grace_seconds = 0;"));
                  replay(schema, history);
                  return true;
              })
              .run(0, Long.MAX_VALUE, rng);
        });
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    private ColumnFamilyStore getCfs(SchemaSpec schema)
    {
        return Keyspace.open(schema.keyspace).getColumnFamilyStore(schema.table);
    }

    private void flushCfs(SchemaSpec schema)
    {
        getCfs(schema).forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    private void replay(SchemaSpec schema, HistoryBuilder historyBuilder)
    {
        CQLVisitExecutor executor = create(schema, historyBuilder);
        for (Visit visit : historyBuilder)
            executor.execute(visit);
    }

    private CQLVisitExecutor create(SchemaSpec schema, HistoryBuilder historyBuilder)
    {
        DataTracker tracker = new DataTracker.SequentialDataTracker();
        return new CQLTesterVisitExecutor(schema, tracker,
                                          new QuiescentChecker(schema.valueGenerators, tracker, historyBuilder),
                                          statement -> {
                                              if (logger.isTraceEnabled())
                                                  logger.trace(statement.toString());
                                              return execute(statement.cql(), statement.bindings());
                                          });
    }
}
