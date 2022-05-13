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

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.marshal.CollectionType.Kind.SET;

public abstract class TxnReferenceValue
{
    public enum Kind
    {
        CONSTANT(Constant.serializer),
        SUBSTITUTION(Substitution.serializer);

        final Serializer serializer;

        Kind(Serializer<? extends TxnReferenceValue> serializer)
        {
            this.serializer = serializer;
        }
    }

    public abstract Kind kind();
    public abstract ByteBuffer compute(TxnData data, AbstractType<?> receiver);

    /**
     * Serializer for everything except Kind
     * @param <T>
     */
    private interface Serializer<T extends TxnReferenceValue>
    {
        void serialize(T t, DataOutputPlus out, int version) throws IOException;
        T deserialize(DataInputPlus in, int version, Kind kind) throws IOException;
        long serializedSize(T t, int version);
    }

    public static class Constant extends TxnReferenceValue
    {
        private final ByteBuffer value;

        public Constant(ByteBuffer value)
        {
            this.value = value;
        }

        public ByteBuffer getValue()
        {
            return value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Constant constant = (Constant) o;
            return value.equals(constant.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(value);
        }

        @Override
        public String toString()
        {
            return ByteBufferUtil.bytesToHex(value);
        }

        @Override
        public Kind kind()
        {
            return Kind.CONSTANT;
        }

        @Override
        public ByteBuffer compute(TxnData data, AbstractType<?> receiver)
        {
            return value;
        }

        public static final Serializer<Constant> serializer = new Serializer<Constant>()
        {
            @Override
            public void serialize(Constant constant, DataOutputPlus out, int version) throws IOException
            {
                ByteBufferUtil.writeWithVIntLength(constant.value, out);
            }

            @Override
            public Constant deserialize(DataInputPlus in, int version, Kind kind) throws IOException
            {
                return new Constant(ByteBufferUtil.readWithVIntLength(in));
            }

            @Override
            public long serializedSize(Constant constant, int version)
            {
                return ByteBufferUtil.serializedSizeWithVIntLength(constant.value);
            }
        };
    }

    public static class Substitution extends TxnReferenceValue
    {
        private final TxnReference reference;

        public Substitution(TxnReference reference)
        {
            this.reference = reference;
        }

        @Override
        public String toString()
        {
            return reference.toString();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Substitution that = (Substitution) o;
            return reference.equals(that.reference);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(reference);
        }

        @Override
        public Kind kind()
        {
            return Kind.SUBSTITUTION;
        }

        @Override
        public ByteBuffer compute(TxnData data, AbstractType<?> receiver)
        {
            // TODO: confirm all references can be satisfied as part of the txn condition
            AbstractType<?> type = reference.column().type;

            // Modify the type we'll check if the reference is to a collection element.
            if (type.isCollection() && reference.selectsPath())
            {
                CollectionType<?> collectionType = (CollectionType<?>) type;
                type = collectionType.kind == SET ? collectionType.nameComparator() : collectionType.valueComparator();
            }

            // Account for frozen collection and reversed clustering key references:
            Preconditions.checkArgument((type.isFrozenCollection() ? receiver.freeze() : receiver) == type.unwrap());

            if (reference.column().isPartitionKey())
                return reference.getPartitionKey(data);

            if (reference.column().isClusteringColumn())
                return reference.getClusteringKey(data);

            ColumnData columnData = reference.getColumnData(data);

            if (reference.selectsComplex())
            {
                ComplexColumnData complex = (ComplexColumnData) columnData;
                //TODO can we do better?
                if (type instanceof CollectionType)
                {
                    CollectionType<?> col = (CollectionType<?>) type;
                    return col.serializeForNativeProtocol(complex.iterator(), ProtocolVersion.CURRENT);
                }
                else if (type instanceof UserType)
                {
                    UserType udt = (UserType) type;
                    return udt.serializeForNativeProtocol(complex.iterator(), ProtocolVersion.CURRENT);
                }
                else
                {
                    throw new AssertionError("Unknown complex type: " + type);
                }
            }
            else if (reference.selectsFrozenCollectionElement())
            {
                // If a path is selected for a non-frozen collection, the element will already be materialized.
                return reference.getFrozenCollectionElement((Cell<?>) columnData);
            }
            else
            {
                Cell<?> cell = (Cell<?>) columnData;
                return reference.selectsSetElement() ? cell.path().get(0) : cell.buffer();
            }
        }

        static final Serializer<Substitution> serializer = new Serializer<Substitution>()
        {
            @Override
            public void serialize(Substitution substitution, DataOutputPlus out, int version) throws IOException
            {
                TxnReference.serializer.serialize(substitution.reference, out, version);
            }

            @Override
            public Substitution deserialize(DataInputPlus in, int version, Kind kind) throws IOException
            {
                return new Substitution(TxnReference.serializer.deserialize(in, version));
            }

            @Override
            public long serializedSize(Substitution substitution, int version)
            {
                return TxnReference.serializer.serializedSize(substitution.reference, version);
            }
        };
    }

    public static final IVersionedSerializer<TxnReferenceValue> serializer = new IVersionedSerializer<TxnReferenceValue>()
    {
        @Override
        public void serialize(TxnReferenceValue value, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(value.kind().ordinal());
            value.kind().serializer.serialize(value, out, version);
        }

        @Override
        public TxnReferenceValue deserialize(DataInputPlus in, int version) throws IOException
        {
            Kind kind = Kind.values()[in.readInt()];
            return kind.serializer.deserialize(in, version, kind);
        }

        @Override
        public long serializedSize(TxnReferenceValue value, int version)
        {
            return TypeSizes.INT_SIZE + value.kind().serializer.serializedSize(value, version);
        }
    };
}
