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
import java.util.Objects;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.statements.TxnDataName;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.apache.cassandra.service.accord.SerializationUtils.columnMetadataSerializer;

public class ValueReference
{
    private final TxnDataName name;
    private final ColumnMetadata column;
    private final CellPath path;

    public ValueReference(TxnDataName name, ColumnMetadata column, CellPath path)
    {
        this.name = name;
        this.column = column;
        this.path = path;
    }

    public ValueReference(TxnDataName name, ColumnMetadata column)
    {
        this(name, column, null);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueReference reference = (ValueReference) o;
        return name.equals(reference.name) && Objects.equals(column, reference.column) && Objects.equals(path, reference.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, column, path);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("REF:").append(name);
        if (column != null)
            sb.append(':').append(column.ksName).append('.').append(column.cfName).append('.').append(column.name.toString());

        if (path != null)
            sb.append(path);

        return sb.toString();
    }

    public ColumnMetadata column()
    {
        return column;
    }

    public boolean selectsColumn()
    {
        return column != null;
    }

    // TODO: Use when collection elements come into play?
    public boolean selectsPath()
    {
        return selectsColumn() && path != null;
    }

    public FilteredPartition getPartition(TxnData data)
    {
        return data.get(name);
    }

    public Row getRow(TxnData data)
    {
        FilteredPartition partition = getPartition(data);
        return partition != null ? getRow(partition) : null;
    }

    public Row getRow(FilteredPartition partition)
    {
        if (column != null && column.isStatic())
            return partition.staticRow();
        assert partition.rowCount() <= 1 : "Multi-row references are not allowed";
        if (partition.rowCount() == 0)
            return null;
        return partition.getAtIdx(0);
    }

    public ColumnData getColumnData(Row row)
    {
        if (column.isComplex() && path == null)
            return row.getComplexColumnData(column);

        return path != null ? row.getCell(column, path) : row.getCell(column);
    }

    public ColumnData getColumnData(TxnData data)
    {
        Row row = getRow(data);
        return row != null ? getColumnData(row) : null;
    }

    public static final IVersionedSerializer<ValueReference> serializer = new IVersionedSerializer<ValueReference>()
    {
        @Override
        public void serialize(ValueReference reference, DataOutputPlus out, int version) throws IOException
        {
            TxnDataName.serializer.serialize(reference.name, out, version);
            out.writeBoolean(reference.column != null);
            if (reference.column != null)
                columnMetadataSerializer.serialize(reference.column, out, version);
            // TODO: serialize path
            Preconditions.checkArgument(reference.path == null);
        }

        @Override
        public ValueReference deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnDataName name = TxnDataName.serializer.deserialize(in, version);
            ColumnMetadata column = in.readBoolean() ? columnMetadataSerializer.deserialize(in, version) : null;
            // TODO: serialize path
            return new ValueReference(name, column, null);
        }

        @Override
        public long serializedSize(ValueReference reference, int version)
        {
            long size = 0;
            size += TxnDataName.serializer.serializedSize(reference.name, version);
            size += TypeSizes.BOOL_SIZE;
            if (reference.column != null)
                size += columnMetadataSerializer.serializedSize(reference.column, version);
            // TODO: serialize path
            return size;
        }
    };
}
