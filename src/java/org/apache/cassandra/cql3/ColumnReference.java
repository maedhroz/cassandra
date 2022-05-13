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

package org.apache.cassandra.cql3;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.statements.TxnDataName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.accord.txn.TxnReference;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;

// TODO: This isn't a great name. Something like ReferenceTerm or RowReference might make more sense
// TODO: Why isn't this in the "transactions" package?
public class ColumnReference extends Term.NonTerminal
{
    public static final String CANNOT_FIND_TUPLE_MESSAGE = "Cannot resolve reference to tuple '%s'.";
    public static final String COLUMN_NOT_IN_TUPLE_MESSAGE = "Column '%s' does not exist in tuple '%s'.";

    private final TxnDataName selectName;
    public final ColumnMetadata column;
    private final Term cellPath;
    
    public ColumnReference(TxnDataName selectName, ColumnMetadata column, Term cellPath)
    {
        this.selectName = selectName;
        this.column = column;
        this.cellPath = cellPath;
    }

    @Override
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        if (cellPath != null)
            cellPath.collectMarkerSpecification(boundNames);
    }

    @Override
    public Terminal bind(QueryOptions options) throws InvalidRequestException
    {
        throw new UnsupportedOperationException("TODO: " + getFullyQualifiedName());
    }

    @Override
    public boolean containsBindMarker()
    {
        return cellPath != null && cellPath.containsBindMarker();
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        // TODO: this
    }
    
    public boolean isElementSelection()
    {
        return cellPath != null && column.type.isCollection();
    }

    private CellPath bindCellPath(QueryOptions options)
    {
        return cellPath != null ? CellPath.create(cellPath.bindAndGet(options)) : null;
    }

    public TxnReference toTxnReference(QueryOptions options)
    {
        Preconditions.checkArgument(cellPath == null || column.isComplex() || column.type.isFrozenCollection());
        return new TxnReference(selectName, column, bindCellPath(options));
    }

    public ColumnIdentifier getFullyQualifiedName()
    {
        String fullName = selectName.name() + '.' + column.name.toString() + (cellPath == null ? "" : '[' + cellPath.toString() + ']');
        return new ColumnIdentifier(fullName, true);
    }

    public static class Raw extends Term.Raw
    {
        private final List<Term.Raw> terms;
        private boolean isResolved = false;

        private TxnDataName tupleName;
        private ColumnMetadata column;
        private Term cellPath = null;

        private ColumnReference prepared;

        public Raw(List<Term.Raw> terms)
        {
            Preconditions.checkArgument(terms != null && !terms.isEmpty());
            this.terms = terms;
        }

        public static Raw fromSelectable(Selectable.RawIdentifier tuple, Selectable.Raw selectable)
        {
            Constants.Literal tupleLiteral = Constants.Literal.string(tuple.toString());

            if (selectable == null)
                return new ColumnReference.Raw(Collections.singletonList(tupleLiteral));

            // TODO: Ideally it would be nice not to have to make items in the Selectables public
            if (selectable instanceof Selectable.WithFieldSelection.Raw)
            {
                Selectable.WithFieldSelection.Raw selection = (Selectable.WithFieldSelection.Raw) selectable;
                return new ColumnReference.Raw(ImmutableList.of(tupleLiteral,
                                                                Constants.Literal.string(selection.selected.toString()),
                                                                Constants.Literal.string(selection.field.toString())));
            }
            else if (selectable instanceof Selectable.WithElementSelection.Raw)
            {
                Selectable.WithElementSelection.Raw elementSelection = (Selectable.WithElementSelection.Raw) selectable;
                ImmutableList<Term.Raw> terms = ImmutableList.of(tupleLiteral,
                                                                 Constants.Literal.string(elementSelection.selected.toString()),
                                                                 elementSelection.element);
                return new ColumnReference.Raw(terms);
            }
            else if (selectable instanceof Selectable.RawIdentifier)
            {
                Selectable.RawIdentifier selection = (Selectable.RawIdentifier) selectable;
                return new ColumnReference.Raw(ImmutableList.of(tupleLiteral, Constants.Literal.string(selection.toString())));
            }

            throw new UnsupportedOperationException("Cannot create column reference from selectable: " + selectable);
        }

        private void resolveFinished()
        {
            isResolved = true;
        }

        public void resolveReference(Map<TxnDataName, ReferenceSource> sources)
        {
            if (isResolved)
                return;

            Iterator<Term.Raw> termIterator = terms.iterator();

            // root level name
            Constants.Literal literal = (Constants.Literal) termIterator.next();
            tupleName = TxnDataName.user(literal.getRawText());
            ReferenceSource source = sources.get(tupleName);
            checkNotNull(source, CANNOT_FIND_TUPLE_MESSAGE, tupleName.name());

            if (!termIterator.hasNext())
            {
                resolveFinished();
                return;
            }

            if (!source.isPointSelect())
                throw new UnsupportedOperationException("Multi-row reference sources are not allowed!");

            literal = (Constants.Literal) termIterator.next();
            column = source.getColumn(literal.getRawText());
            checkNotNull(column, COLUMN_NOT_IN_TUPLE_MESSAGE, literal.getRawText(), tupleName.name());

            // TODO: confirm update partition key terms don't contain column references. This can't be done in prepare
            //   because there can be intermediate functions (ie: pk=row.v+1 or pk=_add(row.v, 5)). Need a recursive Term visitor

            if (!termIterator.hasNext())
            {
                resolveFinished();
                return;
            }

            // TODO: Having a path for a frozen collection is awkward here
            if (column.type.isCollection())
            {
                Term.Raw subSelection = termIterator.next();
                cellPath = subSelection.prepare(column.ksName, specForElementOrSlice(column));
            }
            else
            {
                throw new UnsupportedOperationException("TODO: udts, etc");
            }

            if (!termIterator.hasNext())
                resolveFinished();
        }

        private ColumnSpecification specForElementOrSlice(ColumnSpecification receiver)
        {
            switch (((CollectionType<?>) receiver.type).kind)
            {
                case LIST: return Lists.indexSpecOf(receiver);
                case SET: return Sets.valueSpecOf(receiver);
                case MAP: return Maps.keySpecOf(receiver);
                default: throw new AssertionError();
            }
        }

        public void checkResolved()
        {
            if (!isResolved)
                throw new IllegalStateException();
        }

        @Override
        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            checkResolved();
            return column.testAssignment(keyspace, receiver);
        }


        public static ColumnReference prepare(String keyspace, ColumnSpecification receiver,
                                              TxnDataName selectName, ColumnMetadata column, Term cellPath)
        {
            if (!column.testAssignment(keyspace, receiver).isAssignable())
                if (!column.type.isCollection() && ((CollectionType<?>) column.type).valueComparator().equals(receiver.type))
                    throw new InvalidRequestException(String.format("Invalid reference type %s (%s) for \"%s\" of type %s", 
                                                                    column.type, column.name, receiver.name, receiver.type.asCQL3Type()));

                return new ColumnReference(selectName, column, cellPath);
        }

        @Override
        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            checkResolved();
            prepared = prepare(keyspace, receiver, tupleName, column, cellPath);
            return prepared;
        }

        public ColumnReference prepareAsReceiver()
        {
            checkResolved();
            prepared = new ColumnReference(tupleName, column, cellPath);
            return prepared;
        }

        public ColumnReference prepared()
        {
            Preconditions.checkState(prepared != null);
            return prepared;
        }

        @Override
        public String getText()
        {
            // TODO: What uses this, and do we need to check for resolution?
            return terms.stream().map(Term.Raw::getText).reduce("", (l, r) -> l + '.' + r);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            checkResolved();
            return column.type;
        }

        public ColumnMetadata column()
        {
            return column;
        }
    }

    public interface ReferenceSource
    {
        boolean isPointSelect();
        ColumnMetadata getColumn(String name);
    }
}
