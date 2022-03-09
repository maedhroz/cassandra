/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sai;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.management.AttributeNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.base.Predicates;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.ResourceLeakDetector;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.snapshot.TableSnapshot;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.awaitility.Awaitility;

import static org.apache.cassandra.inject.ActionBuilder.newActionBuilder;
import static org.apache.cassandra.inject.Expression.quote;
import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SAITester extends CQLTester
{
    protected static final String CREATE_KEYSPACE_TEMPLATE = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}";

    protected static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
            "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    protected static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX IF NOT EXISTS ON %%s(%s) USING 'StorageAttachedIndex'";

    protected static int ASSERTION_TIMEOUT_SECONDS = 15;

    protected static ColumnIdentifier V1_COLUMN_IDENTIFIER = ColumnIdentifier.getInterned("v1", true);
    protected static ColumnIdentifier V2_COLUMN_IDENTIFIER = ColumnIdentifier.getInterned("v2", true);

    private static Randomization random;

    public static final ClusteringComparator EMPTY_COMPARATOR = new ClusteringComparator();

    public static final PrimaryKey.Factory TEST_FACTORY = PrimaryKey.factory(EMPTY_COMPARATOR);

    @Rule
    public TestRule testRules = new ResourceLeakDetector();

    @Rule
    public FailureWatcher failureRule = new FailureWatcher();

    @After
    public void removeAllInjections()
    {
        Injections.deleteAll();
    }

    public static Randomization getRandom()
    {
        if (random == null)
            random = new Randomization();
        return random;
    }

    public static IndexContext createIndexContext(String name, AbstractType<?> validator)
    {
        return new IndexContext("test_ks",
                                "test_cf",
                                UTF8Type.instance,
                                new ClusteringComparator(),
                                ColumnMetadata.regularColumn("sai", "internal", name, validator),
                                IndexMetadata.fromSchemaMetadata(name, IndexMetadata.Kind.CUSTOM, null));
    }

    protected void simulateNodeRestart(boolean wait)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        cfs.indexManager.listIndexes().forEach(index -> {
            ((StorageAttachedIndexGroup)cfs.indexManager.getIndexGroup(index)).reset();
        });
        cfs.indexManager.listIndexes().forEach(index -> cfs.indexManager.buildIndex(index));
        cfs.indexManager.executePreJoinTasksBlocking(true);
        if (wait)
        {
            waitForIndexQueryable();
        }
    }

    protected void waitForAssert(Runnable runnableAssert, long timeout, TimeUnit unit)
    {
        Awaitility.await().dontCatchUncaughtExceptions().atMost(timeout, unit).untilAsserted(runnableAssert::run);
    }

    protected void waitForAssert(Runnable assertion)
    {
        waitForAssert(() -> assertion.run(), ASSERTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    protected boolean indexNeedsFullRebuild(String index)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        return cfs.indexManager.needsFullRebuild(index);
    }

    protected boolean isIndexQueryable()
    {
        return isIndexQueryable(KEYSPACE, currentTable());
    }

    protected boolean isIndexQueryable(String keyspace, String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        for (Index index : cfs.indexManager.listIndexes())
        {
            if (!cfs.indexManager.isIndexQueryable(index))
                return false;
        }
        return true;
    }

    protected void verifyInitialIndexFailed(String indexName)
    {
        // Verify that the initial index build fails...
        waitForAssert(() -> assertTrue(indexNeedsFullRebuild(indexName)));
    }

    protected Object getMBeanAttribute(ObjectName name, String attribute) throws Exception
    {
        return jmxConnection.getAttribute(name, attribute);
    }

    protected Object getMetricValue(ObjectName metricObjectName)
    {
        // lets workaround the fact that gauges have Value, but counters have Count
        Object metricValue;
        try
        {
            try
            {
                metricValue = getMBeanAttribute(metricObjectName, "Value");
            }
            catch (AttributeNotFoundException ignored)
            {
                metricValue = getMBeanAttribute(metricObjectName, "Count");
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return metricValue;
    }

    public void waitForIndexQueryable()
    {
        waitForIndexQueryable(KEYSPACE, currentTable());
    }

    public void waitForIndexQueryable(String keyspace, String table)
    {
        waitForAssert(() -> assertTrue(isIndexQueryable(keyspace, table)), 60, TimeUnit.SECONDS);
    }

    protected void startCompaction() throws Throwable
    {
        Iterable<ColumnFamilyStore> tables = StorageService.instance.getValidColumnFamilies(true, false, KEYSPACE, currentTable());
        tables.forEach(table ->
        {
            int gcBefore = CompactionManager.getDefaultGcBefore(table, FBUtilities.nowInSeconds());
            CompactionManager.instance.submitMaximal(table, gcBefore, false);
        });
    }

    public void waitForCompactions()
    {
        waitForAssert(() -> assertFalse(CompactionManager.instance.isCompacting(ColumnFamilyStore.all(), Predicates.alwaysTrue())), 10, TimeUnit.SECONDS);
    }

    protected void waitForCompactionsFinished()
    {
        waitForAssert(() -> assertEquals(0, getCompactionTasks()), 10, TimeUnit.SECONDS);
    }

    protected void waitForEquals(ObjectName name, ObjectName name2)
    {
        waitForAssert(() -> {
            long jmxValue = ((Number) getMetricValue(name)).longValue();
            long jmxValue2 = ((Number) getMetricValue(name2)).longValue();

            jmxValue2 += 2; // add 2 for the first 2 queries in setupCluster

            assertEquals(jmxValue, jmxValue2);
        }, 10, TimeUnit.SECONDS);
    }

    protected void waitForEquals(ObjectName name, long value)
    {
        waitForAssert(() -> assertEquals(value, ((Number) getMetricValue(name)).longValue()), 10, TimeUnit.SECONDS);
    }

    protected ObjectName objectName(String name, String keyspace, String table, String index, String type)
    {
        try
        {
            return new ObjectName(String.format("org.apache.cassandra.metrics:type=StorageAttachedIndex,keyspace=%s,table=%s,index=%s,scope=%s,name=%s",
                    keyspace, table, index, type, name));
        }
        catch (Throwable ex)
        {
            throw Throwables.unchecked(ex);
        }
    }

    protected ObjectName objectNameNoIndex(String name, String keyspace, String table, String type)
    {
        try
        {
            return new ObjectName(String.format("org.apache.cassandra.metrics:type=StorageAttachedIndex,keyspace=%s,table=%s,scope=%s,name=%s",
                    keyspace, table, type, name));
        }
        catch (Throwable ex)
        {
            throw Throwables.unchecked(ex);
        }
    }

     protected ObjectName bufferSpaceObjectName(String name) throws MalformedObjectNameException
    {
        return new ObjectName(String.format("org.apache.cassandra.metrics:type=StorageAttachedIndex,name=%s", name));
    }

    protected long getSegmentBufferSpaceLimit() throws Exception
    {
        ObjectName limitBytesName = bufferSpaceObjectName("SegmentBufferSpaceLimitBytes");
        return (long) (Long) getMetricValue(limitBytesName);
    }

    protected Object getSegmentBufferUsedBytes() throws Exception
    {
        ObjectName usedBytesName = bufferSpaceObjectName("SegmentBufferSpaceUsedBytes");
        return getMetricValue(usedBytesName);
    }

    protected Object getColumnIndexBuildsInProgress() throws Exception
    {
        ObjectName buildersInProgressName = bufferSpaceObjectName("ColumnIndexBuildsInProgress");
        return getMetricValue(buildersInProgressName);
    }

    public String createTable(String query)
    {
        return createTable(KEYSPACE, query);
    }

    public UntypedResultSet execute(String query, Object... values) throws Throwable
    {
        return executeFormattedQuery(formatQuery(query), values);
    }

    public Session sessionNet()
    {
        return sessionNet(getDefaultVersion());
    }

    public void flush(String keyspace, String table)
    {
        ColumnFamilyStore store = Keyspace.open(keyspace).getColumnFamilyStore(table);
        if (store != null)
            store.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    public void compact(String keyspace, String table)
    {
        ColumnFamilyStore store = Keyspace.open(keyspace).getColumnFamilyStore(table);
        if (store != null)
            store.forceMajorCompaction();
    }

    protected void truncate(boolean snapshot)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        if (snapshot)
            cfs.truncateBlocking();
        else
            cfs.truncateBlockingWithoutSnapshot();
    }

    protected void runInitializationTask() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        for (Index i : cfs.indexManager.listIndexes())
        {
            assert i instanceof StorageAttachedIndex;
            cfs.indexManager.makeIndexNonQueryable(i, Index.Status.BUILD_FAILED);
            cfs.indexManager.buildIndex(i).get();
        }
    }

    protected int getCompactionTasks()
    {
        return CompactionManager.instance.getActiveCompactions() + CompactionManager.instance.getPendingTasks();
    }

    protected String getSingleTraceStatement(Session session, String query, String contains) throws Throwable
    {
        query = String.format(query, KEYSPACE + "." + currentTable());
        QueryTrace trace = session.execute(session.prepare(query).bind().enableTracing()).getExecutionInfo().getQueryTrace();
        waitForTracingEvents();

        for (QueryTrace.Event event : trace.getEvents())
        {
            if (event.getDescription().contains(contains))
                return event.getDescription();
        }
        return null;
    }

    public static void copyTo(DataInput in, OutputStream out, int length) throws IOException
    {
        byte[] buffer = new byte[64 * 1024];
        int copiedBytes = 0;

        while (copiedBytes + buffer.length < length)
        {
            in.readFully(buffer);
            out.write(buffer);
            copiedBytes += buffer.length;
        }

        if (copiedBytes < length)
        {
            int left = length - copiedBytes;
            in.readFully(buffer, 0, left);
            out.write(buffer, 0, left);
        }
    }

    /**
     *  Because the tracing executor is single threaded, submitting an empty event should ensure
     *  that all tracing events mutations have been applied.
     */
    protected void waitForTracingEvents()
    {
        try
        {
            Stage.TRACING.executor().submit(() -> {}).get();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Failed to wait for tracing events: {}", t);
        }
    }

    protected void assertNumRows(int expected, String query, Object... args) throws Throwable
    {
        ResultSet rs = executeNet(String.format(query, args));
        assertEquals(expected, rs.all().size());
    }

    protected static Injection newFailureOnEntry(String name, Class<?> invokeClass, String method, Class<? extends Throwable> exception)
    {
        return Injections.newCustom(name)
                         .add(newInvokePoint().onClass(invokeClass).onMethod(method))
                         .add(newActionBuilder().actions().doThrow(exception, quote("Injected failure!")))
                         .build();
    }

    protected int snapshot(String snapshotName)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        TableSnapshot snapshot = cfs.snapshot(snapshotName);
        return snapshot.getDirectories().size();
    }

    protected List<String> restoreSnapshot(String snapshot)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).snapshots(snapshot);
        return restore(cfs, lister);
    }

    protected List<String> restore(ColumnFamilyStore cfs, Directories.SSTableLister lister)
    {
        File dataDirectory = cfs.getDirectories().getDirectoryForNewSSTables();

        List<String> fileNames = new ArrayList<>();
        for (File file : lister.listFiles())
        {
            if (file.tryMove(new File(dataDirectory.absolutePath() + File.pathSeparator() + file.name())))
            {
                fileNames.add(file.name());
            }
        }
        cfs.loadNewSSTables();
        return fileNames;
    }

    public static class Randomization
    {
        private long seed;
        private Random random;

        Randomization()
        {
            if (random == null)
            {
                seed = Long.getLong("cassandra.test.random.seed", System.nanoTime());
                random = new Random(seed);
            }
        }

        public void printSeedOnFailure()
        {
            System.err.println("Randomized test failed. To rerun test use -Dcassandra.test.random.seed=" + seed);
        }

        public int nextInt()
        {
            return random.nextInt();
        }

        public int nextIntBetween(int minValue, int maxValue)
        {
            return RandomInts.randomIntBetween(random, minValue, maxValue);
        }

        public long nextLong()
        {
            return random.nextLong();
        }

        public short nextShort()
        {
            return (short)random.nextInt(Short.MAX_VALUE + 1);
        }

        public byte nextByte()
        {
            return (byte)random.nextInt(Byte.MAX_VALUE + 1);
        }

        public BigInteger nextBigInteger(int minNumBits, int maxNumBits)
        {
            return new BigInteger(RandomInts.randomIntBetween(random, minNumBits, maxNumBits), random);
        }

        public BigDecimal nextBigDecimal(int minUnscaledValue, int maxUnscaledValue, int minScale, int maxScale)
        {
            return BigDecimal.valueOf(RandomInts.randomIntBetween(random, minUnscaledValue, maxUnscaledValue),
                                      RandomInts.randomIntBetween(random, minScale, maxScale));
        }

        public float nextFloat()
        {
            return random.nextFloat();
        }

        public double nextDouble()
        {
            return random.nextDouble();
        }

        public String nextAsciiString(int minLength, int maxLength)
        {
            return RandomStrings.randomAsciiOfLengthBetween(random, minLength, maxLength);
        }

        public String nextTextString(int minLength, int maxLength)
        {
            return RandomStrings.randomRealisticUnicodeOfLengthBetween(random, minLength, maxLength);
        }

        public boolean nextBoolean()
        {
            return random.nextBoolean();
        }

        public void nextBytes(byte[] bytes)
        {
            random.nextBytes(bytes);
        }
    }

    public static class FailureWatcher extends TestWatcher
    {
        @Override
        protected void failed(Throwable e, Description description)
        {
            if (random != null)
                random.printSeedOnFailure();
        }
    }
}
