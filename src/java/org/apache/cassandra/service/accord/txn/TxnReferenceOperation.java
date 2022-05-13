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
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.accord.SerializationUtils;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.marshal.CollectionType.Kind.MAP;
import static org.apache.cassandra.service.accord.SerializationUtils.columnMetadataSerializer;

public class TxnReferenceOperation
{
    public enum Kind
    {
        Adder((byte) 1),
        Appender((byte) 2),
        Discarder((byte) 3),
        Prepender((byte) 4),
        Putter((byte) 5),
        Setter((byte) 6),
        Substracter((byte) 7),
        SetterByKey((byte) 8),
        SetterByIndex((byte) 19);

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
                case 3: return Discarder;
                case 4: return Prepender;
                case 5: return Putter;
                case 6: return Setter;
                case 7: return Substracter;
                case 8: return SetterByKey;
                case 9: return SetterByIndex;
                default: throw new IllegalArgumentException("Unknown kind: " + b);
            }
        }

        public static Kind toKind(Operation operation)
        {
            // TODO: What happens if one of these classes is renamed?
            return Kind.valueOf(operation.getClass().getSimpleName());
        }
    }

    private final ColumnMetadata receiver;
    private final AbstractType<?> type;
    private final Kind kind;
    private final ByteBuffer key;
    private final TxnReferenceValue value;

    public TxnReferenceOperation(Kind kind, ColumnMetadata receiver, ByteBuffer key, TxnReferenceValue value)
    {
        this.kind = kind;
        this.receiver = receiver;
        // shouldn't have operators on ClusteringKeys but just in case call unwrap to make sure
        // we don't ever see the ReversedType

        AbstractType<?> type = receiver.type.unwrap();

        // The value for a map subtraction is actually a set (see Operation.Substraction)
        if (kind == TxnReferenceOperation.Kind.Discarder && type.isCollection())
            if ((((CollectionType<?>) type).kind == MAP))
                type = SetType.getInstance(((MapType<?, ?>) type).getKeysType(), true);

        this.type = type;
        this.key = key;
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
        AbstractType<?> receivingType = type;
        if (kind == Kind.SetterByKey || kind == Kind.SetterByIndex)
            receivingType = ((CollectionType<?>) type).valueComparator();

        Term term = toTerm(data, receivingType);
        return getOperation(key == null ? null : constantTerm(key, receivingType), term);
    }

    //TODO this is only visable to provide a hack... should go back to hiding...
    //STOP BEING BAD DAVID!
    public Operation getOperation(Term key, Term value)
    {
        //TODO how do we validate this earlier?  Did modification already validate?
        //TODO SetterByField
        switch (kind)
        {
            case Adder:
                return adder(value);
            case Putter:
                return putter(value);
            case Setter:
                return setter(value);
            case Appender:
                return appender(value);
            case Discarder:
                return discarder(value);
            case Prepender:
                return prepender(value);
            case Substracter:
                return subtracter(value);
            case SetterByKey:
                return setterByKey(key, value);
            case SetterByIndex:
                return setterByIndex(key, value);
            default: throw new IllegalArgumentException("Unknown kind: " + kind);
        }
    }

    private Term toTerm(TxnData data, AbstractType<?> receivingType)
    {
        ByteBuffer bytes = value.compute(data, receivingType);
        return constantTerm(bytes, receivingType);
    }

    private Term constantTerm(ByteBuffer bytes, AbstractType<?> receivingType)
    {
        if (receivingType.isCollection())
            return SerializationUtils.deserializeCollection(bytes, receivingType);
            
        if (receivingType instanceof UserType)
            return UserTypes.Value.fromSerialized(bytes, (UserType) receivingType);
        else if (type instanceof TupleType)
            return Tuples.Value.fromSerialized(bytes, (TupleType) receivingType);

        return new Constants.Value(bytes);
    }

    private Operation setterByIndex(Term idx, Term value)
    {
        return new Lists.SetterByIndex(receiver, idx, value);
    }

    private Operation setterByKey(Term key, Term value)
    {
        return new Maps.SetterByKey(receiver, key, value);
    }

    private Operation subtracter(Term term)
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
            out.writeBoolean(operation.key != null);
            if (operation.key != null)
                ByteBufferUtil.writeWithVIntLength(operation.key, out);
        }

        @Override
        public TxnReferenceOperation deserialize(DataInputPlus in, int version) throws IOException
        {
            Kind kind = Kind.from(in.readByte());
            ColumnMetadata receiver = columnMetadataSerializer.deserialize(in, version);
            TxnReferenceValue value = TxnReferenceValue.serializer.deserialize(in, version);
            ByteBuffer key = in.readBoolean() ? ByteBufferUtil.readWithVIntLength(in) : null;
            return new TxnReferenceOperation(kind, receiver, key, value);
        }

        @Override
        public long serializedSize(TxnReferenceOperation operation, int version)
        {
            long size = Byte.BYTES;
            size += columnMetadataSerializer.serializedSize(operation.receiver, version);
            size += TxnReferenceValue.serializer.serializedSize(operation.value, version);
            if (operation.key != null)
                size += ByteBufferUtil.serializedSizeWithVIntLength(operation.key);
            return size;
        }
    };
}
