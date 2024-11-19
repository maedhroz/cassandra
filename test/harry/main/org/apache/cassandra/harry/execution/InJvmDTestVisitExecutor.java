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

package org.apache.cassandra.harry.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;

import static org.apache.cassandra.harry.MagicConstants.LTS_UNKNOWN;
import static org.apache.cassandra.harry.MagicConstants.NIL_DESCR;
import static org.apache.cassandra.harry.MagicConstants.NIL_KEY;
import static org.apache.cassandra.harry.MagicConstants.UNKNOWN_DESCR;
import static org.apache.cassandra.harry.MagicConstants.UNSET_DESCR;

public class InJvmDTestVisitExecutor extends CQLVisitExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(InJvmDTestVisitExecutor.class);

    protected final ICluster<?> cluster;
    protected final ConsistencyLevelSelector consistencyLevel;

    protected final NodeSelector nodeSelector;
    protected final PageSizeSelector pageSizeSelector;
    protected final RetryPolicy retryPolicy;

    protected InJvmDTestVisitExecutor(SchemaSpec schema,
                                      DataTracker dataTracker,
                                      Model model,
                                      ICluster<?> cluster,
                                      NodeSelector nodeSelector,
                                      PageSizeSelector pageSizeSelector,
                                      RetryPolicy retryPolicy,
                                      ConsistencyLevelSelector consistencyLevel)
    {
        super(schema, dataTracker, model);
        this.cluster = cluster;
        this.consistencyLevel = consistencyLevel;

        this.nodeSelector = nodeSelector;
        this.pageSizeSelector = pageSizeSelector;
        this.retryPolicy = retryPolicy;
    }

    @Override
    protected List<ResultSetRow> executeWithResult(Visit visit, CompiledStatement statement)
    {
        while (true)
        {
            try
            {
                return executeWithResult(visit, nodeSelector.select(visit.lts), statement);
            }
            catch (Throwable t)
            {
                if (retryPolicy.retry(t))
                    continue;
                throw t;
            }
        }
    }

    protected List<ResultSetRow> executeWithResult(Visit visit, int node, CompiledStatement statement)
    {
        Invariants.checkState(visit.operations.length == 1);
        Object[][] rows;
        ConsistencyLevel consistencyLevel = this.consistencyLevel.consistencyLevel(visit);
        if (consistencyLevel == ConsistencyLevel.NODE_LOCAL)
            rows = cluster.get(node).executeInternal(statement.cql(), statement.bindings());
        else
        {
            int pages = pageSizeSelector.pages(visit);
            if (pages == PageSizeSelector.NO_PAGING)
                rows = cluster.coordinator(node).execute(statement.cql(), consistencyLevel, statement.bindings());
            else
                rows = iterToArr(cluster.coordinator(node)
                                        .executeWithPaging(statement.cql(), consistencyLevel, pages, statement.bindings()));
        }

        if (logger.isTraceEnabled())
            logger.trace("{} returned {} results", statement, rows.length);

        return rowsToResultSet(schema, (Operations.SelectStatement) visit.operations[0], rows);
    }

    protected static Object[][] iterToArr(Iterator<Object[]> iter)
    {
        List<Object[]> tmp = new ArrayList<>();
        while (iter.hasNext())
            tmp.add(iter.next());
        return tmp.toArray(new Object[tmp.size()][]);
    }

    @Override
    protected void executeWithoutResult(Visit visit, CompiledStatement statement)
    {
        while (true)
        {
            try
            {
                executeWithoutResult(visit, nodeSelector.select(visit.lts), statement);
                return;
            }
            catch (Throwable t)
            {
                if (retryPolicy.retry(t))
                    continue;
                throw t;
            }
        }

    }

    protected void executeWithoutResult(Visit visit, int node, CompiledStatement statement)
    {
        ConsistencyLevel consistencyLevel = this.consistencyLevel.consistencyLevel(visit);
        if (consistencyLevel == ConsistencyLevel.NODE_LOCAL)
            cluster.get(node).executeInternal(statement.cql(), statement.bindings());
        else
            cluster.coordinator(node).execute(statement.cql(), consistencyLevel, statement.bindings());
    }

    public static List<ResultSetRow> rowsToResultSet(SchemaSpec schema, Operations.SelectStatement select, Object[][] result)
    {
        List<ResultSetRow> rs = new ArrayList<>();
        for (Object[] res : result)
            rs.add(rowToResultSet(schema, select, res));
        return rs;
    }

    public static ResultSetRow rowToResultSet(SchemaSpec schema, Operations.SelectStatement select, Object[] result)
    {
        long[] staticColumns = new long[schema.staticColumns.size()];
        long[] regularColumns = new long[schema.regularColumns.size()];
        Arrays.fill(staticColumns, UNKNOWN_DESCR);
        Arrays.fill(regularColumns, UNKNOWN_DESCR);
        // TODO: with timestamps
        long[] staticLts = LTS_UNKNOWN;
        long[] regularLts = LTS_UNKNOWN;

        long pd = UNKNOWN_DESCR;
        Operations.Selection selection = Operations.Selection.fromBitSet(select.selection(), schema);
        if (selection.selectsAllOf(schema.partitionKeys))
        {
            Object[] partitionKey = new Object[schema.partitionKeys.size()];
            for (int i = 0; i < schema.partitionKeys.size(); i++)
                partitionKey[i] = result[selection.indexOf(schema.partitionKeys.get(i))];

            pd = schema.valueGenerators.pkGen.deflate(partitionKey);
        }

        // Deflate logic for clustering key is a bit more involved, since CK can be nil in case of a single static row.
        long cd = UNKNOWN_DESCR;
        if (selection.selectsAllOf(schema.clusteringKeys))
        {
            Object[] clusteringKey = new Object[schema.clusteringKeys.size()];
            for (int i = 0; i < schema.clusteringKeys.size(); i++)
            {
                Object v = result[selection.indexOf(schema.clusteringKeys.get(i))];
                if (v == null)
                {
                    for (int j = 0; j < schema.clusteringKeys.size(); j++)
                    {
                        Invariants.checkState(result[selection.indexOf(schema.clusteringKeys.get(j))] == null,
                                              "All elements of clustering key should have been null");
                    }
                    clusteringKey = NIL_KEY;
                    break;
                }
                clusteringKey[i] = v;
            }

            // Clusterings can not be set to nil, so if we do not see, we assume it is unset
            if (clusteringKey == NIL_KEY)
                cd = UNSET_DESCR;
            else
                cd = schema.valueGenerators.ckGen.deflate(clusteringKey);
        }

        for (int i = 0; i < schema.regularColumns.size(); i++)
        {
            ColumnSpec<?> column = schema.regularColumns.get(i);
            if (selection.selects(column))
            {
                Object v = result[selection.indexOf(schema.regularColumns.get(i))];
                if (v == null)
                    regularColumns[i] = NIL_DESCR;
                else
                    regularColumns[i] = schema.valueGenerators.regularColumnGens.get(i).deflate(v);
            }
            else
            {
                regularLts[i] = UNKNOWN_DESCR;
            }
        }

        for (int i = 0; i < schema.staticColumns.size(); i++)
        {
            ColumnSpec<?> column = schema.staticColumns.get(i);
            if (selection.selects(column))
            {
                Object v = result[selection.indexOf(schema.staticColumns.get(i))];
                if (v == null)
                    staticColumns[i] = NIL_DESCR;
                else
                    staticColumns[i] = schema.valueGenerators.staticColumnGens.get(i).deflate(v);
            }
            else
            {
                staticColumns[i] = UNKNOWN_DESCR;
            }
        }

        if (selection.includeTimestamp())
            throw new IllegalStateException("not implemented for CQL Tester");
        if (selection.includeTimestamp())
            throw new IllegalStateException("not implemented for CQL Tester");

        return new ResultSetRow(pd, cd, staticColumns, staticLts, regularColumns, regularLts);
    }

    public interface NodeSelector
    {
        int select(long lts);
    }

    public interface PageSizeSelector
    {
        int NO_PAGING = -1;
        int pages(Visit visit);
    }

    public interface ConsistencyLevelSelector
    {
        ConsistencyLevel consistencyLevel(Visit visit);
    }

    public interface RetryPolicy
    {
        boolean retry(Throwable t);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        protected ConsistencyLevelSelector consistencyLevel = v -> ConsistencyLevel.QUORUM;

        protected NodeSelector nodeSelector = null;
        protected PageSizeSelector pageSizeSelector = (lts) -> 10;
        protected RetryPolicy retryPolicy = (retry) -> false;

        public Builder consistencyLevel(ConsistencyLevel consistencyLevel)
        {
            this.consistencyLevel = v -> consistencyLevel;
            return this;
        }

        public Builder consistencyLevel(ConsistencyLevelSelector consistencyLevel)
        {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        public Builder nodeSelector(NodeSelector nodeSelector)
        {
            this.nodeSelector = nodeSelector;
            return this;
        }

        public Builder pageSizeSelector(PageSizeSelector pageSizeSelector)
        {
            this.pageSizeSelector = pageSizeSelector;
            return this;
        }

        public Builder retryPolicy(RetryPolicy retryPolicy)
        {
            this.retryPolicy = retryPolicy;
            return this;
        }

        protected void setDefaults(SchemaSpec schema, ICluster<?> cluster)
        {
            if (nodeSelector == null)
            {
                this.nodeSelector = new NodeSelector()
                {
                    final int nodes = cluster.size();
                    long counter = 0;

                    @Override
                    public int select(long lts)
                    {
                        return (int) (counter++ % nodes + 1);
                    }
                };
            }
        }

        public InJvmDTestVisitExecutor build(SchemaSpec schema, Model.Replay replay, ICluster<?> cluster)
        {
            setDefaults(schema, cluster);
            DataTracker tracker = new DataTracker.SequentialDataTracker();
            Model model = new QuiescentChecker(schema.valueGenerators, tracker, replay);
            return new InJvmDTestVisitExecutor(schema, tracker, model, cluster,
                                               nodeSelector, pageSizeSelector, retryPolicy, consistencyLevel);
        }
    }

}