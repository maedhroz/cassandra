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

package org.apache.cassandra.cql3.transactions;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.txn.TxnReferenceOperation;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.db.marshal.CollectionType.Kind.MAP;
import static org.apache.cassandra.schema.TableMetadata.UNDEFINED_COLUMN_NAME_MESSAGE;

public abstract class ReferenceOperation
{
    private final ColumnMetadata receiver;

    public ReferenceOperation(ColumnMetadata receiver)
    {
        this.receiver = receiver;
    }

    /**
     * Creates a {@link ReferenceOperation} from the given {@link  Operation} for the purpose of defering execution
     * within a transaction. When the language sees an Operation using a reference one is created already, but for cases
     * that needs to defer execution (such as when {@link Operation#requiresRead()} is true), this method can be used.
     */
    public static ReferenceOperation create(Operation operation)
    {
        TxnReferenceOperation.Kind kind = TxnReferenceOperation.Kind.toKind(operation);
        ColumnMetadata receiver = operation.column;
        ReferenceValue value = new ReferenceValue.Constant(operation.term());
        
        Term key = null;
        if (operation instanceof Maps.SetterByKey)
            key = ((Maps.SetterByKey) operation).k;
        
        return new Assignment(kind, receiver, key, value);
    }

    public ColumnMetadata receiver()
    {
        return receiver;
    }

    public abstract boolean requiresRead();


    public abstract TxnReferenceOperation bindAndGet(QueryOptions options);

    public abstract static class Raw
    {
        public final ColumnIdentifier column;

        public Raw(ColumnIdentifier column)
        {
            this.column = column;
        }

        public abstract ReferenceOperation prepare(TableMetadata metadata, VariableSpecifications bindVariables);
    }

    //TODO there is now only one type... simplfy
    public static class Assignment extends ReferenceOperation
    {
        private final TxnReferenceOperation.Kind kind;
        private final Term key;
        private final ReferenceValue value;

        public Assignment(TxnReferenceOperation.Kind kind, ColumnMetadata receiver, Term key, ReferenceValue value)
        {
            super(receiver);
            this.kind = kind;
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean requiresRead()
        {
            //TODO this is super hacky but required to delegate to Operation... can we find a better way?
            return new TxnReferenceOperation(kind, receiver(), null, null).getOperation(null, null).requiresRead();
        }

        @Override
        public TxnReferenceOperation bindAndGet(QueryOptions options)
        {
            return new TxnReferenceOperation(kind, receiver(), key != null ? key.bindAndGet(options) : null, value.bindAndGet(options));
        }

        //TODO
        // new Operation.SetField(field, t)

        public static class Raw extends ReferenceOperation.Raw
        {
            private final Operation.RawUpdate operation;
            private final ReferenceValue.Raw value;

            public Raw(Operation.RawUpdate operation, ColumnIdentifier column, ReferenceValue.Raw value)
            {
                super(column);
                this.operation = operation;
                this.value = value;
            }

            @Override
            public ReferenceOperation prepare(TableMetadata metadata, VariableSpecifications bindVariables)
            {
                ColumnMetadata receiver = metadata.getColumn(column);
                Operation op = operation.prepare(metadata, receiver, true);
                checkTrue(receiver != null, UNDEFINED_COLUMN_NAME_MESSAGE, column.toCQLString(), metadata);
                TxnReferenceOperation.Kind kind = TxnReferenceOperation.Kind.toKind(op);
                
                // The value for a map subtraction is actually a set (see Operation.Substraction)
                if (kind == TxnReferenceOperation.Kind.Discarder && receiver.type.isCollection())
                    if ((((CollectionType<?>) receiver.type).kind == MAP))
                        receiver = receiver.withNewType(SetType.getInstance(((MapType<?, ?>) receiver.type).getKeysType(), true));
                    
                // TODO: Is there a way to do this without exposing k and idx?
                Term key = null; 
                if (op instanceof Maps.SetterByKey)
                    key = ((Maps.SetterByKey) op).k;
                else if (op instanceof Lists.SetterByIndex)
                    key = ((Lists.SetterByIndex) op).idx;
                    
                return new Assignment(kind, receiver, key, value.prepare(receiver, bindVariables));
            }
        }
    }
}
