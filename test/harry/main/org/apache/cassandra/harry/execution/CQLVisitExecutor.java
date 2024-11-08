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

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.model.Model;

/**
 *
 * TODO: Transactional results ; LET
 */
public abstract class CQLVisitExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(QueryBuildingVisitExecutor.class);
    protected final SchemaSpec schema;

//    protected final String wrapQueryFormat = "BEGIN UNLOGGED BATCH\n%s\nAPPLY BATCH;";
    protected final String wrapQueryFormat = "BEGIN TRANSACTION\n" +
                                             "  %s;\n" +
                                             "COMMIT TRANSACTION;";

    protected final DataTracker dataTracker;
    protected final Model model;
    private final QueryBuildingVisitExecutor queryBuilder;

    public CQLVisitExecutor(SchemaSpec schema, DataTracker dataTracker, Model model)
    {
        this.schema = schema;
        this.dataTracker = dataTracker;
        this.model = model;
        this.queryBuilder = new QueryBuildingVisitExecutor(schema);
    }

    public static void replay(CQLVisitExecutor executor, Model.Replay replay)
    {
        for (Visit visit : replay)
        {
            while (true)
            {
                try
                {
                    executor.execute(visit);
                    break;
                }
                catch (Throwable t)
                {
                    logger.info("Caught an exception at {} while replaying {}. Operations up to this visit:", visit.lts, visit);
                    for (Visit rereplay : replay)
                    {
                        logger.info("{}", visit);
                        if (rereplay.lts > visit.lts)
                            throw t;
                    }
                }
            }
        }
    }

    public final void execute(Visit visit)
    {
        dataTracker.begin(visit);
        CompiledStatement compiledStatement = queryBuilder.compile(visit);
        // All operations are not touching any data
        if (compiledStatement == null)
        {
            Invariants.checkArgument(Arrays.stream(visit.operations).allMatch(op -> op.kind() == Operations.Kind.CUSTOM));
            return;
        }

        List<Operations.SelectStatement> selects = queryBuilder.selects;
        if (selects.isEmpty())
        {
            executeMutatingVisit(visit, compiledStatement);
        }
        else
        {
            Invariants.checkState(selects.size() == 1);
            executeValidatingVisit(visit, selects, compiledStatement);
        }
        dataTracker.end(visit);
    }

    // Lives in a separate method so that it is easier to override it
    protected void executeMutatingVisit(Visit visit, CompiledStatement statement)
    {
        executeWithoutResult(visit, statement);
    }

    // Lives in a separate method so that it is easier to override it
    protected void executeValidatingVisit(Visit visit, List<Operations.SelectStatement> selects, CompiledStatement compiledStatement)
    {
        // TODO: Have never tested with multiple
        List<ResultSetRow> resultSetRow = executeWithResult(visit, compiledStatement);
        try
        {
            model.validate(selects.get(0), resultSetRow);
        }
        catch (Throwable t)
        {
            throw new AssertionError(String.format("Caught an exception while validating %s:\n%s", selects.get(0), compiledStatement),
                                     t);
        }
    }

    protected abstract List<ResultSetRow> executeWithResult(Visit visit, CompiledStatement statement);
    protected abstract void executeWithoutResult(Visit visit, CompiledStatement statement);

}
