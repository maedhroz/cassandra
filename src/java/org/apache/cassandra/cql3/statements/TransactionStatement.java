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

package org.apache.cassandra.cql3.statements;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import accord.api.Key;
import accord.primitives.Keys;
import accord.txn.Txn;
import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnReference;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.selection.ResultSetBuilder;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.transactions.ConditionStatement;
import org.apache.cassandra.cql3.transactions.ReferenceOperation;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.txn.TxnCondition;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnNamedRead;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnUpdate;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LazyToString;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class TransactionStatement implements CQLStatement
{
    public static final String DUPLICATE_TUPLE_NAME_MESSAGE = "The name '%s' has already been used by a LET assignment.";
    public static final String INCOMPLETE_PRIMARY_KEY_LET_MESSAGE = "SELECT in LET assignment without LIMIT 1 must specify all primary key elements; CQL %s";
    public static final String INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE = "Normal SELECT without LIMIT 1 must specify all primary key elements; CQL %s";
    public static final String NO_CONDITIONS_IN_UPDATES_MESSAGE = "Updates within transactions may not specify their own conditions.";
    public static final String NO_TIMESTAMPS_IN_UPDATES_MESSAGE = "Updates within transactions may not specify custom timestamps.";
    public static final String EMPTY_TRANSACTION_MESSAGE = "Transaction contains no reads or writes";
    public static final String SELECT_REFS_NEED_COLUMN_MESSAGE = "SELECT references must specify a column.";

    static class NamedSelect
    {
        final TxnDataName name;
        final SelectStatement select;

        public NamedSelect(TxnDataName name, SelectStatement select)
        {
            this.name = name;
            this.select = select;
        }
    }

    private final List<NamedSelect> assignments;
    private final NamedSelect returningSelect;
    private final List<ColumnReference> returningReferences;
    private final List<ModificationStatement> updates;
    private final List<ConditionStatement> conditions;

    private final VariableSpecifications bindVariables;

    public TransactionStatement(List<NamedSelect> assignments,
                                NamedSelect returningSelect,
                                List<ColumnReference> returningReferences,
                                List<ModificationStatement> updates,
                                List<ConditionStatement> conditions,
                                VariableSpecifications bindVariables)
    {
        this.assignments = assignments;
        this.returningSelect = returningSelect;
        this.returningReferences = returningReferences;
        this.updates = updates;
        this.conditions = conditions;
        this.bindVariables = bindVariables;
    }

    @Override
    public List<ColumnSpecification> getBindVariables()
    {
        return bindVariables.getBindVariables();
    }

    @Override
    public void authorize(ClientState state)
    {
        // Assess read permissions for all data from both explicit LET statements and generated reads.
        for (NamedSelect let : assignments)
            let.select.authorize(state);

        if (returningSelect != null)
            returningSelect.select.authorize(state);

        for (ModificationStatement update : updates)
            update.authorize(state);
    }

    @Override
    public void validate(ClientState state)
    {
        for (ModificationStatement statement : updates)
            statement.validate(state);
    }

    @VisibleForTesting
    public List<ColumnReference> getReturningReferences()
    {
        return returningReferences;
    }

    TxnNamedRead createNamedRead(NamedSelect namedSelect, QueryOptions options)
    {
        SelectStatement select = namedSelect.select;
        ReadQuery readQuery = select.getQuery(options, 0);

        // We reject reads from both LET and SELECT that do not specify a single row.
        @SuppressWarnings("unchecked") 
        SinglePartitionReadQuery.Group<SinglePartitionReadCommand> selectQuery = (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) readQuery;

        return new TxnNamedRead(namedSelect.name, Iterables.getOnlyElement(selectQuery.queries));
    }

    TxnRead createRead(QueryOptions options, Consumer<Key> keyConsumer)
    {
        List<TxnNamedRead> reads = new ArrayList<>(assignments.size() + 1);

        for (NamedSelect select : assignments)
        {
            TxnNamedRead read = createNamedRead(select, options);
            keyConsumer.accept(read.key());
            reads.add(read);
        }

        if (returningSelect != null)
        {
            TxnNamedRead read = createNamedRead(returningSelect, options);
            keyConsumer.accept(read.key());
            reads.add(read);
        }

        for (NamedSelect select : autoReads.values())
            // don't need keyConsumer as the keys are known to exist due to Modification
            reads.add(createNamedRead(select, options));
        
        return new TxnRead(reads);
    }

    TxnCondition createCondition(QueryOptions options)
    {
        if (conditions.isEmpty())
            return TxnCondition.none();
        if (conditions.size() == 1)
            return conditions.get(0).createCondition(options);

        List<TxnCondition> result = new ArrayList<>(conditions.size());
        for (ConditionStatement condition : conditions)
            result.add(condition.createCondition(options));

        return new TxnCondition.BooleanGroup(TxnCondition.Kind.AND, result);
    }

    private final Map<TxnDataName, NamedSelect> autoReads = new HashMap<>();

    List<TxnWrite.Fragment> createWriteFragments(ClientState state, QueryOptions options, Consumer<Key> keyConsumer)
    {
        List<TxnWrite.Fragment> fragments = new ArrayList<>(updates.size());
        int idx = 0;
        for (ModificationStatement modification : updates)
        {
            TxnWrite.Fragment fragment = modification.getTxnWriteFragment(idx++, state, options);
            keyConsumer.accept(fragment.key);
            fragments.add(fragment);

            if (modification.allReferenceOperations().stream().anyMatch(ReferenceOperation::requiresRead))
            {
                TxnDataName partitionName = TxnDataName.partitionRead(modification.metadata(), fragment.key.partitionKey());
                if (!autoReads.containsKey(partitionName))
                    autoReads.put(partitionName, new NamedSelect(partitionName, ((UpdateStatement) modification).createSelectForTxn()));
            }
        }
        return fragments;
    }

    TxnUpdate createUpdate(ClientState state, QueryOptions options, Consumer<Key> keyConsumer)
    {
        return new TxnUpdate(createWriteFragments(state, options, keyConsumer), createCondition(options));
    }

    Keys toKeys(SortedSet<Key> keySet)
    {
        return new Keys(keySet);
    }

    @VisibleForTesting
    public Txn createTxn(ClientState state, QueryOptions options)
    {
        SortedSet<Key> keySet = new TreeSet<>();
        if (updates.isEmpty())
        {
            Preconditions.checkState(conditions.isEmpty());
            TxnRead read = createRead(options, keySet::add);
            return new Txn.InMemory(toKeys(keySet), read, TxnQuery.ALL);
        }
        else
        {
            TxnUpdate update = createUpdate(state, options, keySet::add);
            TxnRead read = createRead(options, keySet::add);
            return new Txn.InMemory(toKeys(keySet), read, TxnQuery.ALL, update);
        }
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime)
    {
        TxnData data = AccordService.instance.coordinate(createTxn(state.getClientState(), options));
        
        if (returningSelect != null)
        {
            FilteredPartition partition = data.get(TxnDataName.returning());
            Selection.Selectors selectors = returningSelect.select.getSelection().newSelectors(options);
            ResultSetBuilder result = new ResultSetBuilder(returningSelect.select.getResultMetadata(), selectors, null);
            returningSelect.select.processPartition(partition.rowIterator(), options, result, FBUtilities.nowInSeconds());
            return new ResultMessage.Rows(result.build());
        }
        
        if (returningReferences != null)
        {
            // TODO: If the ResultMetadata ctor didn't require ColumnSpecification, we wouldn't need both of these.
            List<ColumnSpecification> names = new ArrayList<>(returningReferences.size());
            List<ColumnMetadata> columns = new ArrayList<>(returningReferences.size());

            // TODO: If we support selecting entire LET rows, this will have to pull column names from Row.
            for (ColumnReference reference : returningReferences)
            {
                ColumnIdentifier fullName = reference.getFullyQualifiedName();
                ColumnMetadata forMetadata = reference.column.withNewName(fullName);
                
                // TODO: This likely will need some expansion for tuple/UDT?
                if (reference.isSetElementSelection())
                    forMetadata = forMetadata.withNewType(((SetType<?>) forMetadata.type).nameComparator());
                
                names.add(forMetadata);
                columns.add(reference.column);
            }

            ResultSetBuilder result = new ResultSetBuilder(new ResultSet.ResultMetadata(names), Selection.noopSelector(), null);
            result.newRow(options.getProtocolVersion(), null, null, columns);
            
            for (ColumnReference reference : returningReferences)
            {
                ColumnData columnData = reference.toValueReference(options).getColumnData(data);
                
                if (reference.isSetElementSelection())
                    result.add(((Cell<?>) columnData).path().get(0));
                else
                    result.add(columnData, FBUtilities.nowInSeconds());
            }

            return new ResultMessage.Rows(result.build());
        }

        // This is a write-only transaction
        return new ResultMessage.Void();
    }

    @Override
    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        return execute(state, options, nanoTime());
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        // TODO: this
        return null;
    }

    private static class SelectReferenceSource implements ColumnReference.ReferenceSource
    {
        private final SelectStatement statement;
        private final Set<ColumnMetadata> selectedColumns;
        private final TableMetadata metadata;

        public SelectReferenceSource(SelectStatement statement)
        {
            this.statement = statement;
            this.metadata = statement.table;
            Selection selection = statement.getSelection();
            selectedColumns = new HashSet<>(selection.getColumns());
        }

        @Override
        public boolean isPointSelect()
        {
            return statement.getRestrictions().hasAllPKColumnsRestrictedByEqualities()
                   || statement.getLimit(QueryOptions.DEFAULT) == 1;
        }

        @Override
        public ColumnMetadata getColumn(String name)
        {
            ColumnMetadata column = metadata.getColumn(new ColumnIdentifier(name, true));
            if (column != null)
                checkTrue(selectedColumns.contains(column), "%s refererences a column not included in the select", this);
            return column;
        }
    }

    public static class Parsed extends QualifiedStatement
    {
        private final List<SelectStatement.RawStatement> assignments;
        private final SelectStatement.RawStatement select;
        private final List<ColumnReference.Raw> returning;
        private final List<ModificationStatement.Parsed> updates;
        private final List<ConditionStatement.Raw> conditions;
        private final List<ColumnReference.Raw> columnReferences;

        public Parsed(List<SelectStatement.RawStatement> assignments,
                      SelectStatement.RawStatement select,
                      List<ColumnReference.Raw> returning,
                      List<ModificationStatement.Parsed> updates,
                      List<ConditionStatement.Raw> conditions,
                      List<ColumnReference.Raw> columnReferences)
        {
            super(null);
            this.assignments = assignments;
            this.select = select;
            this.returning = returning;
            this.updates = updates;
            this.conditions = conditions != null ? conditions : Collections.emptyList();
            this.columnReferences = columnReferences;
        }

        @Override
        public void setKeyspace(ClientState state)
        {
            assignments.forEach(select -> select.setKeyspace(state));
            updates.forEach(update -> update.setKeyspace(state));
        }

        @Override
        public CQLStatement prepare(ClientState state)
        {
            checkFalse(updates.isEmpty() && returning == null && select == null, EMPTY_TRANSACTION_MESSAGE);

            if (select != null || returning != null)
                checkTrue(select != null ^ returning != null, "Cannot specify both a full SELECT and a SELECT w/ LET references.");

            List<NamedSelect> preparedAssignments = new ArrayList<>(assignments.size());
            Map<TxnDataName, ColumnReference.ReferenceSource> refSources = new HashMap<>();
            Set<TxnDataName> selectNames = new HashSet<>();

            for (SelectStatement.RawStatement select : assignments)
            {
                TxnDataName name = TxnDataName.user(select.parameters.refName);
                checkNotNull(name, "Assignments must be named");
                checkTrue(selectNames.add(name), DUPLICATE_TUPLE_NAME_MESSAGE, name.name());
                checkFalse(name.equals("returning"), "Assignments may not use the name \"returning\"");

                SelectStatement prepared = select.prepare(bindVariables);
                checkAtMostOneRowSpecified(prepared, INCOMPLETE_PRIMARY_KEY_LET_MESSAGE, LazyToString.lazy(() -> prepared.asCQL(QueryOptions.DEFAULT, state)));

                NamedSelect namedSelect = new NamedSelect(name, prepared);
                preparedAssignments.add(namedSelect);
                refSources.put(name, new SelectReferenceSource(prepared));
            }

            if (columnReferences != null)
            {
                for (ColumnReference.Raw reference : columnReferences)
                    reference.resolveReference(refSources);
            }

            NamedSelect returningSelect = null;
            if (select != null)
            {
                SelectStatement prepared = select.prepare(bindVariables);
                // TODO: Accord saves the result of this read, so limit to a single row until that is no longer true.
                checkAtMostOneRowSpecified(prepared, INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, LazyToString.lazy(() -> prepared.asCQL(QueryOptions.DEFAULT, state)));
                returningSelect = new NamedSelect(TxnDataName.returning(), prepared);
            }

            List<ColumnReference> returningReferences = null;

            if (returning != null)
            {
                // TODO: Eliminate/modify this check if we allow full tuple selections.
                returningReferences = returning.stream().peek(raw -> checkTrue(raw.column() != null, SELECT_REFS_NEED_COLUMN_MESSAGE))
                                                        .map(ColumnReference.Raw::prepareAsReceiver)
                                                        .collect(Collectors.toList());
            }

            List<ModificationStatement> preparedUpdates = new ArrayList<>(updates.size());
            
            // check for any read-before-write updates
            for (int i = 0; i < updates.size(); i++)
            {
                ModificationStatement.Parsed parsed = updates.get(i);

                ModificationStatement prepared = parsed.prepare(bindVariables);
                checkFalse(prepared.hasConditions(), NO_CONDITIONS_IN_UPDATES_MESSAGE);
                checkFalse(prepared.isTimestampSet(), NO_TIMESTAMPS_IN_UPDATES_MESSAGE);

                preparedUpdates.add(prepared);
            }

            List<ConditionStatement> preparedConditions = new ArrayList<>(conditions.size());
            for (ConditionStatement.Raw condition : conditions)
                // TODO: Is this synthetic ks name dangerous?
                preparedConditions.add(condition.prepare("[txn]", bindVariables));

            return new TransactionStatement(preparedAssignments, returningSelect, returningReferences, preparedUpdates, preparedConditions, bindVariables);
        }

        private void checkAtMostOneRowSpecified(SelectStatement prepared, String failureMessage, Object messageArg)
        {
            int limit = prepared.getLimit(QueryOptions.DEFAULT);

            if (limit == DataLimits.NO_LIMIT)
                checkTrue(prepared.getRestrictions().hasAllPKColumnsRestrictedByEqualities(), failureMessage, messageArg);
            else
                checkTrue(limit == 1, failureMessage, messageArg);
        }
    }
}
