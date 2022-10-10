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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Tuples;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.apache.cassandra.service.accord.SerializationUtils.columnMetadataSerializer;

public class TxnReferenceOperation
{
    public enum Kind
    {
        Adder((byte) 1),
        Appender((byte) 2),
        Deleter((byte) 3),
        Discarder((byte) 4),
        Prepender((byte) 5),
        Putter((byte) 6),
        Setter((byte) 7),
        Substracter((byte) 8);

        private final byte value;

        Kind(byte value)
        {
            this.value = value;
        }

        public static Kind from(byte b)
        {
            switch (b)
            {
                case 1: return Adder;
                case 2: return Appender;
                case 3: return Deleter;
                case 4: return Discarder;
                case 5: return Prepender;
                case 6: return Putter;
                case 7: return Setter;
                case 8: return Substracter;
                default: throw new IllegalArgumentException("Unknown kind: " + b);
            }
        }

        public static Kind toKind(Operation operation)
        {
            return Kind.valueOf(operation.getClass().getSimpleName());
        }
    }
    private final ColumnMetadata receiver;
    private final AbstractType<?> type;
    private final Kind kind;
    private final TxnReferenceValue value;

    public TxnReferenceOperation(Kind kind, ColumnMetadata receiver, TxnReferenceValue value)
    {
        this.kind = kind;
        this.receiver = receiver;
        // shouldn't have operators on ClusteringKeys but just in case call unwrap to make sure
        // we don't ever see the ReversedType
        this.type = receiver.type.unwrap();
        this.value = value;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnReferenceOperation that = (TxnReferenceOperation) o;
        return Objects.equals(receiver, that.receiver) && kind == that.kind && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(receiver, kind, value);
    }

    @Override
    public String toString()
    {
        return receiver + " = " + value;
    }

    public ColumnMetadata receiver()
    {
        return receiver;
    }

    public void apply(TxnData data, DecoratedKey key, UpdateParameters up)
    {
        Operation operation = toOperation(data);
        operation.execute(key, up);
    }

    private Operation toOperation(TxnData data)
    {
        Term term = toTerm(data);
        return getOperation(term);
    }

    //TODO this is only visable to provide a hack... should go back to hiding...
    //STOP BEING BAD DAVID!
    public Operation getOperation(Term term)
    {
        //TODO how do we validate this earlier?  Did modification already validate?
        //TODO SetterByField, SetterByKey, SetterByIndex, DeleterByField, DiscarderByIndex, ElementDiscarder, DiscarderByKey
        switch (kind)
        {
            case Adder:
                return adder(term);
            case Putter:
                return putter(term);
            case Setter:
                return setter(term);
            case Deleter:
                return deleter(term);
            case Appender:
                return appender(term);
            case Discarder:
                return discarder(term);
            case Prepender:
                return prepender(term);
            case Substracter:
                return substracter(term);
            default: throw new IllegalArgumentException("Unknown kind: " + kind);
        }
    }

    private Term toTerm(TxnData data)
    {
        ByteBuffer bytes = value.compute(data, type);
        return constantTerm(bytes);
    }

    private Term constantTerm(ByteBuffer bytes)
    {
        if (type instanceof UserType)
        {
            return UserTypes.Value.fromSerialized(bytes, (UserType) type);
        }
        else if (type instanceof TupleType)
        {
            return Tuples.Value.fromSerialized(bytes, (TupleType) type);
        }
        else if (type instanceof SetType)
        {
            return Sets.Value.fromSerialized(bytes, (SetType) type, ProtocolVersion.CURRENT);
        }
        else if (type instanceof ListType)
        {
            return Lists.Value.fromSerialized(bytes, (ListType) type, ProtocolVersion.CURRENT);
        }
        else if (type instanceof MapType)
        {
            return Maps.Value.fromSerialized(bytes, (MapType) type, ProtocolVersion.CURRENT);
        }
        return new Constants.Value(bytes);
    }

    private Operation substracter(Term term)
    {
        return new Constants.Substracter(receiver, term);
    }

    private Operation prepender(Term term)
    {
        return new Lists.Prepender(receiver, term);
    }

    private Operation discarder(Term term)
    {
        if (type instanceof SetType)
            return new Sets.Discarder(receiver, term);
        return new Lists.Discarder(receiver, term);
    }

    private Operation appender(Term term)
    {
        return new Lists.Appender(receiver, term);
    }

    private Operation deleter(Term term)
    {
        return new Constants.Deleter(receiver);
    }

    private Operation setter(Term term)
    {
        if (type instanceof ListType)
            return new Lists.Setter(receiver, term);
        else if (type instanceof SetType)
            return new Sets.Setter(receiver, term);
        else if (type instanceof MapType)
            return new Maps.Setter(receiver, term);
        else if (type instanceof UserType)
            return new UserTypes.Setter(receiver, term);
        return new Constants.Setter(receiver, term);
    }

    private Operation putter(Term term)
    {
        return new Maps.Putter(receiver, term);
    }

    private Operation adder(Term term)
    {
        if (type instanceof SetType<?>)
            return new Sets.Adder(receiver, term);
        return new Constants.Adder(receiver, term);
    }



    public static final IVersionedSerializer<TxnReferenceOperation> serializer = new IVersionedSerializer<TxnReferenceOperation>()
    {
        @Override
        public void serialize(TxnReferenceOperation operation, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(operation.kind.value);
            columnMetadataSerializer.serialize(operation.receiver, out, version);
            TxnReferenceValue.serializer.serialize(operation.value, out, version);
        }

        @Override
        public TxnReferenceOperation deserialize(DataInputPlus in, int version) throws IOException
        {
            Kind kind = Kind.from(in.readByte());
            ColumnMetadata receiver = columnMetadataSerializer.deserialize(in, version);
            TxnReferenceValue value = TxnReferenceValue.serializer.deserialize(in, version);
            return new TxnReferenceOperation(kind, receiver, value);
        }

        @Override
        public long serializedSize(TxnReferenceOperation operation, int version)
        {
            long size = Byte.BYTES;
            size += columnMetadataSerializer.serializedSize(operation.receiver, version);
            size += TxnReferenceValue.serializer.serializedSize(operation.value, version);
            return size;
        }
    };
}
