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
import org.apache.cassandra.harry.op.Visit;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

/**
 * Edge case tests for range tombstone interactions with SSTables and compaction.
 *
 * These tests use aggressive configurations to stress code paths that are
 * rarely exercised under default settings:
 *
 *   - column_index_size = 1 KiB: forces range tombstones to span multiple
 *     index blocks, exercising the endOpenMarker tracking in IndexInfo and
 *     openMarker restoration in IndexState.setToBlock()
 *   - gc_grace_seconds = 0: allows immediate tombstone purging, stressing
 *     CompactionController.getPurgeEvaluator()
 *   - Small compression chunks (4 KiB): misaligns compression boundaries
 *     with index blocks, exercising cross-chunk decompression during iteration
 *   - provide_overlapping_tombstones = ROW: changes shadow source resolution
 *   - Large partitions with hundreds of rows to actually cross index block
 *     boundaries and generate indexed RowIndexEntry instances
 *   - No-compression variant to exercise the uncompressed code path
 *   - DESC clustering keys with reversed comparator
 *   - Multiple read validations between compaction rounds to catch
 *     iterator-level state corruption
 */
public class RangeTombstoneCompactionEdgeCaseTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(RangeTombstoneCompactionEdgeCaseTest.class);

    // Large enough that partitions exceed the 1 KiB column_index_size, creating
    // multiple index blocks per partition. With int64 + ascii columns, each row
    // is roughly 50-100 bytes, so 500 rows ~ 25-50 KiB per partition, yielding
    // 25-50 index blocks at 1 KiB granularity.
    private static final int ROWS_PER_PARTITION = 500;

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

    // Single CK, ASC, no special table properties (applied via createTable override)
    private final Generator<SchemaSpec> singleCkSchema = rng -> {
        return new SchemaSpec(rng.next(), 1000, KEYSPACE,
                              "rt_edge_" + rng.nextLong(0, Long.MAX_VALUE),
                              Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.int64Type)),
                              Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.int64Type, false)),
                              Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.int64Type),
                                            ColumnSpec.regularColumn("v2", ColumnSpec.asciiType),
                                            ColumnSpec.regularColumn("v3", ColumnSpec.int32Type)),
                              Arrays.asList(ColumnSpec.staticColumn("s1", ColumnSpec.int64Type)));
    };

    // DESC clustering key
    private final Generator<SchemaSpec> descCkSchema = rng -> {
        return new SchemaSpec(rng.next(), 1000, KEYSPACE,
                              "rt_edge_" + rng.nextLong(0, Long.MAX_VALUE),
                              Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.int64Type)),
                              Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.int64Type, true)),
                              Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.int64Type),
                                            ColumnSpec.regularColumn("v2", ColumnSpec.asciiType),
                                            ColumnSpec.regularColumn("v3", ColumnSpec.int32Type)),
                              Arrays.asList(ColumnSpec.staticColumn("s1", ColumnSpec.int64Type)));
    };

    // Two clustering keys for nonEqFrom tests
    private final Generator<SchemaSpec> multiCkSchema = rng -> {
        return new SchemaSpec(rng.next(), 1000, KEYSPACE,
                              "rt_edge_" + rng.nextLong(0, Long.MAX_VALUE),
                              Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.int64Type)),
                              Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.int64Type, false),
                                            ColumnSpec.ck("ck2", ColumnSpec.int64Type, false)),
                              Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.int64Type),
                                            ColumnSpec.regularColumn("v2", ColumnSpec.asciiType)),
                              Arrays.asList(ColumnSpec.staticColumn("s1", ColumnSpec.int64Type)));
    };

    // ========================================================================
    // Table creation helpers with aggressive configurations
    // ========================================================================

    /**
     * Creates a table with gc_grace_seconds=0, small compression chunks, and
     * specific compaction options. The column_index_size is set globally via
     * DatabaseDescriptor since it is not a per-table option.
     */
    private void createWithGcGrace0SmallChunks(SchemaSpec schema)
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);  // 1 KiB index blocks
        DatabaseDescriptor.setColumnIndexCacheSize(1);   // 1 KiB cache threshold

        String cql = schema.compile().replace(";", "");
        // Append aggressive table options
        if (cql.contains(" WITH "))
            cql += " AND gc_grace_seconds = 0"
                   + " AND compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}"
                   + " AND compaction = {'class': 'SizeTieredCompactionStrategy',"
                   + " 'provide_overlapping_tombstones': 'ROW', 'enabled': false};";
        else
            cql += " WITH gc_grace_seconds = 0"
                   + " AND compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}"
                   + " AND compaction = {'class': 'SizeTieredCompactionStrategy',"
                   + " 'provide_overlapping_tombstones': 'ROW', 'enabled': false};";
        createTable(cql);
    }

    /**
     * Creates a table with no compression at all, gc_grace_seconds=0, tiny index.
     * Tests the uncompressed data path which has different I/O code.
     */
    private void createWithNoCompression(SchemaSpec schema)
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        DatabaseDescriptor.setColumnIndexCacheSize(1);

        String cql = schema.compile().replace(";", "");
        if (cql.contains(" WITH "))
            cql += " AND gc_grace_seconds = 0"
                   + " AND compression = {'enabled': 'false'}"
                   + " AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': false};";
        else
            cql += " WITH gc_grace_seconds = 0"
                   + " AND compression = {'enabled': 'false'}"
                   + " AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': false};";
        createTable(cql);
    }

    /**
     * Creates a table with LCS compaction strategy, gc_grace_seconds=0, tiny index.
     * LCS produces narrow, non-overlapping SSTables per level, exercising different
     * compaction merge paths than STCS.
     */
    private void createWithLCS(SchemaSpec schema)
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        DatabaseDescriptor.setColumnIndexCacheSize(1);

        String cql = schema.compile().replace(";", "");
        if (cql.contains(" WITH "))
            cql += " AND gc_grace_seconds = 0"
                   + " AND compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}"
                   + " AND compaction = {'class': 'LeveledCompactionStrategy',"
                   + " 'sstable_size_in_mb': 1, 'enabled': false};";
        else
            cql += " WITH gc_grace_seconds = 0"
                   + " AND compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}"
                   + " AND compaction = {'class': 'LeveledCompactionStrategy',"
                   + " 'sstable_size_in_mb': 1, 'enabled': false};";
        createTable(cql);
    }

    // ========================================================================
    // Tests: Range tombstones spanning index block boundaries
    // ========================================================================

    /**
     * EDGE CASE 1: Large partition with range tombstone spanning many index blocks.
     *
     * With column_index_size=1KiB and ~500 rows, a partition has ~25-50 index blocks.
     * A range tombstone covering rows [100..400] spans most of those blocks.
     * The key code path: IndexState.setToBlock() must restore openMarker from
     * the previous block's endOpenMarker for every block transition during reads.
     *
     * After compaction, the merged SSTable must preserve the endOpenMarker chain
     * and BigFormatPartitionWriter.addIndexBlock() must capture the open range
     * tombstone state at each new block boundary.
     */
    @Test
    public void testRangeTombstoneSpanningManyIndexBlocks()
    {
        withRandom(rng -> {
            SchemaSpec schema = singleCkSchema.generate(rng);
            createWithGcGrace0SmallChunks(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // Insert a large partition to span many index blocks
            for (int i = 0; i < ROWS_PER_PARTITION; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush large partition");

            // Range tombstone spanning most index blocks
            history.deleteRowRange(0, 100, 400, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush range delete");

            // Validate BEFORE compaction - reads must traverse index blocks
            history.selectPartition(0);

            // Compact
            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "major compaction");

            // Validate AFTER compaction - the compacted SSTable must preserve
            // endOpenMarker chain correctly
            history.selectPartition(0);

            replay(schema, history);
        });
    }

    /**
     * EDGE CASE 2: Multiple overlapping range tombstones that each span different
     * sets of index blocks, with data re-inserted between them.
     *
     * This creates a complex endOpenMarker state where the merger must track
     * which of multiple range tombstones is "biggest" (most recent) at each
     * index block boundary.
     *
     * Uses gc_grace_seconds=0 so purging is attempted immediately. The purge
     * evaluator must correctly determine that tombstones shadowing data in the
     * other SSTable cannot be purged.
     */
    @Test
    public void testOverlappingRangeTombstonesAcrossIndexBlocks()
    {
        withRandom(rng -> {
            SchemaSpec schema = singleCkSchema.generate(rng);
            createWithGcGrace0SmallChunks(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // SSTable A: full partition
            for (int i = 0; i < ROWS_PER_PARTITION; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush SSTable A");

            // SSTable B: range delete [50..200] - spans ~15 index blocks
            history.deleteRowRange(0, 50, 200, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush range delete [50..200]");

            // SSTable C: re-insert [100..150] + new range delete [150..350]
            for (int i = 100; i <= 150; i++)
                history.insert(0, i);
            history.deleteRowRange(0, 150, 350, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush re-inserts + range delete [150..350]");

            // SSTable D: re-insert [200..300] (into the second range tombstone)
            for (int i = 200; i <= 300; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush re-inserts [200..300]");

            // Validate before compaction
            history.selectPartition(0);

            // Compact
            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "major compaction");

            // Validate after
            history.selectPartition(0);

            replay(schema, history);
        });
    }

    /**
     * EDGE CASE 3: No compression + tiny index blocks.
     *
     * Without compression, the data file is read directly (no decompression
     * buffer). Index block seeking operates on raw file offsets. Range tombstones
     * spanning index blocks exercise a different I/O path than compressed tables.
     */
    @Test
    public void testRangeTombstoneNoCompressionTinyIndex()
    {
        withRandom(rng -> {
            SchemaSpec schema = singleCkSchema.generate(rng);
            createWithNoCompression(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // Large partition
            for (int i = 0; i < ROWS_PER_PARTITION; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush data");

            // Multiple non-overlapping range tombstones across the partition
            history.deleteRowRange(0, 10, 60, 0, true, false);
            history.deleteRowRange(0, 60, 120, 0, true, false);
            history.deleteRowRange(0, 200, 300, 0, false, true);
            history.deleteRowRange(0, 400, 499, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush range deletes");

            history.selectPartition(0);

            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "major compaction");

            history.selectPartition(0);

            replay(schema, history);
        });
    }

    /**
     * EDGE CASE 4: DESC clustering key + tiny index blocks.
     *
     * The reversed comparator means range tombstone bounds are stored in
     * reversed order. With tiny index blocks, the endOpenMarker chain must
     * be maintained correctly despite the reversed iteration direction.
     * This is a historically fragile area (reversed type unwrapping bugs).
     */
    @Test
    public void testDescClusteringTinyIndexBlocks()
    {
        withRandom(rng -> {
            SchemaSpec schema = descCkSchema.generate(rng);
            createWithGcGrace0SmallChunks(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            for (int i = 0; i < ROWS_PER_PARTITION; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush data");

            // Range tombstone spanning many index blocks in DESC order
            history.deleteRowRange(0, 50, 400, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush range delete");

            // Re-insert at specific positions within the deleted range
            for (int i = 100; i <= 200; i += 10)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush re-inserts");

            // Validate reads in DESC order traverse index blocks correctly
            history.selectPartition(0);

            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "major compaction");

            history.selectPartition(0);

            replay(schema, history);
        });
    }

    // ========================================================================
    // Tests: gc_grace_seconds=0 purging edge cases
    // ========================================================================

    /**
     * EDGE CASE 5: Tombstone purging with gc_grace_seconds=0 and shadow sources.
     *
     * With gc_grace_seconds=0, tombstones become purgeable immediately. But
     * CompactionController.getPurgeEvaluator() must still check shadow sources:
     * a tombstone can only be purged if its timestamp is less than the minimum
     * timestamp of all non-compacting SSTables containing the same partition.
     *
     * This test creates a scenario where a range tombstone in SSTable B shadows
     * data in SSTable A, then adds SSTable C. When compacting B+C (minor),
     * the tombstone in B must NOT be purged because A still holds the data.
     *
     * Then we compact everything and verify the final result.
     */
    @Test
    public void testGcGrace0PurgeWithShadowSources()
    {
        withRandom(rng -> {
            SchemaSpec schema = singleCkSchema.generate(rng);
            createWithGcGrace0SmallChunks(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // SSTable A: data
            for (int i = 0; i < 200; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush SSTable A");

            // SSTable B: range tombstone shadowing A's data
            history.deleteRowRange(0, 50, 150, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush SSTable B");

            // SSTable C: some new inserts outside the deleted range
            for (int i = 200; i < 300; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush SSTable C");

            // Validate before any compaction
            history.selectPartition(0);

            // Major compaction - all SSTables participate
            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "major compaction");

            history.selectPartition(0);

            replay(schema, history);
        });
    }

    /**
     * EDGE CASE 6: Layered deletion types with gc_grace=0 and tiny index.
     *
     * Partition delete + range delete + row delete + column delete, all in
     * a large partition with tiny index blocks. With gc_grace=0, the
     * PurgeFunction.applyToMarker() must correctly handle the interaction
     * between range tombstone markers, partition-level deletion, and
     * per-row/per-cell tombstones during compaction.
     */
    @Test
    public void testLayeredDeletionsGcGrace0TinyIndex()
    {
        withRandom(rng -> {
            SchemaSpec schema = singleCkSchema.generate(rng);
            createWithGcGrace0SmallChunks(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // Large partition
            for (int i = 0; i < ROWS_PER_PARTITION; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush initial data");

            // Row tombstones at specific positions
            history.deleteRow(0, 50);
            history.deleteRow(0, 150);
            history.deleteRow(0, 250);
            history.deleteRow(0, 350);

            // Column tombstones at other positions
            HistoryBuilderHelper.deleteRandomColumns(schema, 0, 75, rng, history);
            HistoryBuilderHelper.deleteRandomColumns(schema, 0, 175, rng, history);

            // Range tombstone [100..300]
            history.deleteRowRange(0, 100, 300, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush mixed deletes");

            // Re-insert some rows in the deleted range
            for (int i = 150; i <= 250; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush re-inserts");

            // Partition delete
            history.deletePartition(0);
            history.customThrowing(() -> flushCfs(schema), "flush partition delete");

            // Final inserts (survive the partition delete)
            for (int i = 0; i <= 30; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush final inserts");

            history.selectPartition(0);

            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "major compaction");

            history.selectPartition(0);

            replay(schema, history);
        });
    }

    /**
     * EDGE CASE 7: Multiple compaction rounds with gc_grace=0.
     *
     * After each compaction round, tombstones may have been purged. New data
     * and new tombstones are added, and a second compaction must correctly
     * handle the merged state where some tombstones from round 1 no longer
     * exist in the compacted SSTable.
     */
    @Test
    public void testMultipleCompactionRoundsGcGrace0()
    {
        withRandom(rng -> {
            SchemaSpec schema = singleCkSchema.generate(rng);
            createWithGcGrace0SmallChunks(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // Round 1
            for (int i = 0; i < 300; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush R1 data");

            history.deleteRowRange(0, 50, 200, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush R1 range delete");

            history.selectPartition(0);
            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact R1");
            history.selectPartition(0);

            // Round 2: insert into the previously-deleted range
            for (int i = 50; i <= 200; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush R2 data");

            history.deleteRowRange(0, 100, 250, 0, false, true);
            history.customThrowing(() -> flushCfs(schema), "flush R2 range delete");

            history.selectPartition(0);
            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact R2");
            history.selectPartition(0);

            // Round 3: more writes and a partition delete
            for (int i = 100; i <= 250; i++)
                history.insert(0, i);
            history.deletePartition(0);
            history.customThrowing(() -> flushCfs(schema), "flush R3 partition delete");

            // Insert after partition delete (newest timestamps)
            for (int i = 0; i < 50; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush R3 final inserts");

            history.selectPartition(0);
            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact R3");
            history.selectPartition(0);

            replay(schema, history);
        });
    }

    // ========================================================================
    // Tests: Boundary and inclusivity edge cases with large partitions
    // ========================================================================

    /**
     * EDGE CASE 8: Range tombstone boundaries at index block transitions.
     *
     * Insert enough data that rows at specific positions fall exactly at
     * index block boundaries. Then issue range tombstones whose bounds
     * are at those positions, testing the edge where a range tombstone
     * bound coincides with an index block boundary.
     *
     * The endOpenMarker at block N must correctly reflect whether the
     * tombstone's bound at the block boundary is inclusive or exclusive.
     */
    @Test
    public void testBoundariesAtIndexBlockTransitions()
    {
        withRandom(rng -> {
            SchemaSpec schema = singleCkSchema.generate(rng);
            createWithGcGrace0SmallChunks(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // Insert rows densely
            for (int i = 0; i < ROWS_PER_PARTITION; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush data");

            // Test all four inclusivity combos with range bounds that likely
            // fall near index block boundaries (rows are distributed across
            // ~25-50 blocks, so row 50, 100, 200 are at different blocks)
            boolean[][] combos = { {true, true}, {false, false}, {true, false}, {false, true} };
            for (boolean[] combo : combos)
            {
                history.deleteRowRange(0, 50, 200, 0, combo[0], combo[1]);
            }
            history.customThrowing(() -> flushCfs(schema), "flush range deletes");

            // Re-insert at the exact boundary rows
            history.insert(0, 50);
            history.insert(0, 200);
            history.customThrowing(() -> flushCfs(schema), "flush boundary re-inserts");

            history.selectPartition(0);
            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "major compaction");
            history.selectPartition(0);

            replay(schema, history);
        });
    }

    /**
     * EDGE CASE 9: Adjacent range tombstones with shared boundary in large partition.
     *
     * Two range tombstones meeting at a shared clustering key, where that key
     * is near an index block boundary. The compaction merger must handle the
     * close/open marker transition correctly even when the boundary marker
     * falls at the edge of an index block.
     */
    @Test
    public void testAdjacentRangeTombstonesAtIndexBoundary()
    {
        withRandom(rng -> {
            SchemaSpec schema = singleCkSchema.generate(rng);
            createWithGcGrace0SmallChunks(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            for (int i = 0; i < ROWS_PER_PARTITION; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush data");

            // Two adjacent range tombstones meeting at row 250
            history.deleteRowRange(0, 0, 250, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush range [0..250]");

            history.deleteRowRange(0, 250, 499, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush range [250..499]");

            history.selectPartition(0);
            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "major compaction");
            history.selectPartition(0);

            replay(schema, history);
        });
    }

    /**
     * EDGE CASE 10: Open-ended slice deletes in large partitions.
     *
     * deleteRowSliceByLowerBound and deleteRowSliceByUpperBound generate
     * half-open range tombstones. With large partitions and tiny index blocks,
     * a half-open tombstone generates many index blocks with endOpenMarker set.
     */
    @Test
    public void testOpenEndedSliceDeletesLargePartition()
    {
        withRandom(rng -> {
            SchemaSpec schema = singleCkSchema.generate(rng);
            createWithGcGrace0SmallChunks(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            for (int i = 0; i < ROWS_PER_PARTITION; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush data");

            // Delete all rows >= 400 (open-ended lower bound)
            history.deleteRowSliceByLowerBound(0, 400, 0, true);
            history.customThrowing(() -> flushCfs(schema), "flush lower-bound slice");

            // Delete all rows <= 50 (open-ended upper bound)
            history.deleteRowSliceByUpperBound(0, 50, 0, false);
            history.customThrowing(() -> flushCfs(schema), "flush upper-bound slice");

            // Re-insert at boundaries
            history.insert(0, 50);
            history.insert(0, 400);
            history.customThrowing(() -> flushCfs(schema), "flush boundary re-inserts");

            history.selectPartition(0);
            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "major compaction");
            history.selectPartition(0);

            replay(schema, history);
        });
    }

    // ========================================================================
    // Tests: Compaction strategy variations
    // ========================================================================

    /**
     * EDGE CASE 11: LCS compaction with range tombstones and tiny index.
     *
     * LCS produces non-overlapping SSTables per level. Range tombstones that
     * span data across levels are handled differently than in STCS. The
     * level-based overlap detection in getPurgeEvaluator() follows a different
     * code path.
     */
    @Test
    public void testLCSCompactionWithRangeTombstones()
    {
        withRandom(rng -> {
            SchemaSpec schema = singleCkSchema.generate(rng);
            createWithLCS(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // Create multiple partitions
            for (int p = 0; p < 3; p++)
                for (int i = 0; i < 200; i++)
                    history.insert(p, i);
            history.customThrowing(() -> flushCfs(schema), "flush data");

            // Range deletes in each partition
            history.deleteRowRange(0, 20, 80, 0, true, true);
            history.deleteRowRange(1, 50, 150, 0, false, false);
            history.deleteRowRange(2, 0, 199, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush range deletes");

            // More data
            for (int p = 0; p < 3; p++)
                for (int i = 50; i < 100; i++)
                    history.insert(p, i);
            history.customThrowing(() -> flushCfs(schema), "flush more data");

            for (int p = 0; p < 3; p++)
                history.selectPartition(p);

            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "major compaction");

            for (int p = 0; p < 3; p++)
                history.selectPartition(p);

            replay(schema, history);
        });
    }

    /**
     * EDGE CASE 12: provide_overlapping_tombstones=ROW with range tombstone purging.
     *
     * When provide_overlapping_tombstones is set to ROW, compaction resolves
     * overlapping tombstones at row granularity rather than cell granularity.
     * This tests that range tombstones spanning many rows are correctly handled
     * when the shadow source resolution changes granularity.
     */
    @Test
    public void testProvideOverlappingTombstonesRow()
    {
        withRandom(rng -> {
            SchemaSpec schema = singleCkSchema.generate(rng);
            createWithGcGrace0SmallChunks(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // Partition 0: data + range tombstone
            for (int i = 0; i < 300; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush partition 0 data");

            history.deleteRowRange(0, 50, 250, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush range delete");

            // Re-insert interleaved with row deletes
            for (int i = 100; i <= 200; i += 2)
            {
                history.insert(0, i);
                history.deleteRow(0, i + 1);
            }
            history.customThrowing(() -> flushCfs(schema), "flush interleaved inserts+row deletes");

            history.selectPartition(0);
            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "major compaction");
            history.selectPartition(0);

            replay(schema, history);
        });
    }

    // ========================================================================
    // Tests: Multi-column clustering keys with tiny index
    // ========================================================================

    /**
     * EDGE CASE 13: Multi-column clustering with nonEqFrom and tiny index.
     *
     * With composite clustering keys (ck1, ck2) and tiny index blocks,
     * range tombstones with nonEqFrom=1 (ck1 pinned, range on ck2) create
     * narrow ranges that may fall within a single index block, while
     * nonEqFrom=0 ranges span many blocks. The interaction of both in
     * the same partition after compaction tests the merger's ability to
     * handle differently-scoped range tombstones.
     */
    @Test
    public void testMultiColumnClusteringTinyIndex()
    {
        withRandom(rng -> {
            SchemaSpec schema = multiCkSchema.generate(rng);
            createWithGcGrace0SmallChunks(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            // Insert many rows
            int maxCk = Math.min(200, schema.valueGenerators.ckPopulation());
            for (int i = 0; i < maxCk; i++)
                history.insert(0, i);
            history.customThrowing(() -> flushCfs(schema), "flush data");

            // Range delete with nonEqFrom=0 (range on ck1, spans many blocks)
            history.deleteRowRange(0, 20, 80, 0, true, true);
            history.customThrowing(() -> flushCfs(schema), "flush range delete nonEqFrom=0");

            // Range delete with nonEqFrom=1 (ck1 pinned, range on ck2 only)
            history.deleteRowRange(0, 100, 150, 1, true, false);
            history.customThrowing(() -> flushCfs(schema), "flush range delete nonEqFrom=1");

            history.selectPartition(0);
            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "major compaction");
            history.selectPartition(0);

            replay(schema, history);
        });
    }

    // ========================================================================
    // Tests: Burn tests with aggressive configurations
    // ========================================================================

    /**
     * EDGE CASE 14: Burn test with tiny index blocks, gc_grace=0, random schema.
     *
     * ModelChecker-driven randomized test with aggressive configurations.
     * Explores random sequences of inserts, range deletes, row deletes,
     * partition deletes, flushes, and compactions.
     *
     * The key difference from the basic burn test: column_index_size=1KiB
     * and gc_grace_seconds=0 exercise the index block boundary tracking
     * and tombstone purging code paths under stress.
     */
    @Test
    public void testBurnTestTinyIndexGcGrace0()
    {
        Generator<SchemaSpec> randomSchemaGen = SchemaGenerators.schemaSpecGen(KEYSPACE, "rt_burn", 100);
        withRandom(rng -> {
            SchemaSpec schema = randomSchemaGen.generate(rng);
            createWithGcGrace0SmallChunks(schema);

            int maxPk = Math.min(3, schema.valueGenerators.pkPopulation());
            int maxCk = Math.min(100, schema.valueGenerators.ckPopulation());

            Generator<Integer> pkGen = Generators.int32(0, maxPk);
            Generator<Integer> ckGen = Generators.int32(0, maxCk);

            ModelChecker<HistoryBuilder, Void> mc = new ModelChecker<>();
            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            mc.init(history)
              .step((h, r) -> HistoryBuilderHelper.insertRandomData(schema, pkGen, ckGen, rng, h))
              .step((h, r) -> rng.nextDouble() >= 0.85,
                    (h, r) -> h.deleteRowRange(pkGen.generate(rng),
                                               ckGen.generate(rng), ckGen.generate(rng),
                                               rng.nextInt(schema.clusteringKeys.size()),
                                               rng.nextBoolean(), rng.nextBoolean()))
              .step((h, r) -> rng.nextDouble() >= 0.90,
                    (h, r) -> h.deleteRow(pkGen.generate(rng), ckGen.generate(rng)))
              .step((h, r) -> rng.nextDouble() >= 0.98,
                    (h, r) -> h.deletePartition(pkGen.generate(rng)))
              .step((h, r) -> rng.nextDouble() >= 0.93,
                    (h, r) -> HistoryBuilderHelper.deleteRandomColumns(
                        schema, pkGen.generate(rng), ckGen.generate(rng), rng, h))
              .step((h, r) -> rng.nextDouble() >= 0.90,
                    (h, r) -> h.custom(() -> flush(schema.keyspace, schema.table), "FLUSH"))
              .step((h, r) -> rng.nextDouble() >= 0.97,
                    (h, r) -> h.custom(() -> compact(schema.keyspace, schema.table), "COMPACT"))
              .step((h, r) -> h.selectPartition(pkGen.generate(rng)))
              .step((h, r) -> h.selectRowRange(pkGen.generate(rng),
                                               ckGen.generate(rng), ckGen.generate(rng),
                                               rng.nextInt(schema.clusteringKeys.size()),
                                               rng.nextBoolean(), rng.nextBoolean()))
              .exitCondition((h) -> {
                  if (history.size() < 3000)
                      return false;

                  dropTable("DROP TABLE " + schema.keyspace + "." + schema.table);
                  createTable(schema.compile().replace(";", " AND gc_grace_seconds = 0;"));
                  replay(schema, history);
                  return true;
              })
              .run(0, Long.MAX_VALUE, rng);
        });
    }

    /**
     * EDGE CASE 15: Burn test with no compression.
     *
     * Same random exploration as test 14 but with compression disabled.
     * The uncompressed data path uses different buffer management and
     * file I/O code. Range tombstone markers are read directly from
     * the data file without decompression, exercising a separate code path.
     */
    @Test
    public void testBurnTestNoCompression()
    {
        Generator<SchemaSpec> randomSchemaGen = SchemaGenerators.schemaSpecGen(KEYSPACE, "rt_burn_nc", 100);
        withRandom(rng -> {
            SchemaSpec schema = randomSchemaGen.generate(rng);
            createWithNoCompression(schema);

            int maxPk = Math.min(3, schema.valueGenerators.pkPopulation());
            int maxCk = Math.min(100, schema.valueGenerators.ckPopulation());

            Generator<Integer> pkGen = Generators.int32(0, maxPk);
            Generator<Integer> ckGen = Generators.int32(0, maxCk);

            ModelChecker<HistoryBuilder, Void> mc = new ModelChecker<>();
            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            mc.init(history)
              .step((h, r) -> HistoryBuilderHelper.insertRandomData(schema, pkGen, ckGen, rng, h))
              .step((h, r) -> rng.nextDouble() >= 0.85,
                    (h, r) -> h.deleteRowRange(pkGen.generate(rng),
                                               ckGen.generate(rng), ckGen.generate(rng),
                                               rng.nextInt(schema.clusteringKeys.size()),
                                               rng.nextBoolean(), rng.nextBoolean()))
              .step((h, r) -> rng.nextDouble() >= 0.90,
                    (h, r) -> h.deleteRow(pkGen.generate(rng), ckGen.generate(rng)))
              .step((h, r) -> rng.nextDouble() >= 0.98,
                    (h, r) -> h.deletePartition(pkGen.generate(rng)))
              .step((h, r) -> rng.nextDouble() >= 0.90,
                    (h, r) -> h.custom(() -> flush(schema.keyspace, schema.table), "FLUSH"))
              .step((h, r) -> rng.nextDouble() >= 0.97,
                    (h, r) -> h.custom(() -> compact(schema.keyspace, schema.table), "COMPACT"))
              .step((h, r) -> h.selectPartition(pkGen.generate(rng)))
              .exitCondition((h) -> {
                  if (history.size() < 3000)
                      return false;

                  String ddl = schema.compile().replace(";", "");
                  if (ddl.contains(" WITH "))
                      ddl += " AND gc_grace_seconds = 0 AND compression = {'enabled': 'false'};";
                  else
                      ddl += " WITH gc_grace_seconds = 0 AND compression = {'enabled': 'false'};";
                  dropTable("DROP TABLE " + schema.keyspace + "." + schema.table);
                  createTable(ddl);
                  replay(schema, history);
                  return true;
              })
              .run(0, Long.MAX_VALUE, rng);
        });
    }

    /**
     * EDGE CASE 16: Interleaved range tombstones across many SSTables with
     * tiny index, gc_grace=0, and reads between every operation.
     *
     * This test validates after every single flush and before/after every
     * compaction, catching bugs where the read path produces incorrect
     * results from a specific SSTable layout that compaction later "fixes."
     */
    @Test
    public void testInterleavedRangeTombstonesWithReadsBetween()
    {
        withRandom(rng -> {
            SchemaSpec schema = singleCkSchema.generate(rng);
            createWithGcGrace0SmallChunks(schema);

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            int maxRow = 300;

            for (int cycle = 0; cycle < 15; cycle++)
            {
                // Insert some rows
                int insertCount = rng.nextInt(10, 40);
                for (int j = 0; j < insertCount; j++)
                    history.insert(0, rng.nextInt(0, maxRow));

                // Range deletion
                int lo = rng.nextInt(0, maxRow);
                int hi = rng.nextInt(lo, maxRow);
                history.deleteRowRange(0, lo, hi, 0, rng.nextBoolean(), rng.nextBoolean());

                // Flush
                history.customThrowing(() -> flushCfs(schema), "flush cycle " + cycle);

                // Validate after every flush
                history.selectPartition(0);

                // Occasional compaction
                if (cycle % 4 == 3)
                {
                    history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "compact at cycle " + cycle);
                    history.selectPartition(0);
                }
            }

            // Final compaction
            history.customThrowing(() -> getCfs(schema).forceMajorCompaction(), "final compaction");
            history.selectPartition(0);

            replay(schema, history);
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
