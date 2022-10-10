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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import accord.api.Data;
import accord.api.Result;
import org.apache.cassandra.cql3.statements.TxnDataName;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.service.accord.SerializationUtils.filteredPartitionSerializer;

public class TxnData implements Data, Result, Iterable<FilteredPartition>
{
    private final Map<TxnDataName, FilteredPartition> data;

    public TxnData(Map<TxnDataName, FilteredPartition> data)
    {
        this.data = data;
    }

    public TxnData()
    {
        this(new HashMap<>());
    }

    public void put(TxnDataName name, FilteredPartition partition)
    {
        data.put(name, partition);
    }

    public FilteredPartition get(TxnDataName name)
    {
        return data.get(name);
    }

    public Set<Map.Entry<TxnDataName, FilteredPartition>> entrySet()
    {
        return data.entrySet();
    }

    @Override
    public Data merge(Data data)
    {
        TxnData that = (TxnData) data;
        TxnData merged = new TxnData();
        this.data.forEach(merged::put);
        that.data.forEach(merged::put);
        return merged;
    }

    // TODO: implement this for AccordCommand
    public long estimatedSizeOnHeap()
    {
        return 0;
    }

    @Override
    public Iterator<FilteredPartition> iterator()
    {
        return data.values().iterator();
    }

    public static final IVersionedSerializer<TxnData> serializer = new IVersionedSerializer<TxnData>()
    {
        @Override
        public void serialize(TxnData data, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(data.data.size());
            for (Map.Entry<TxnDataName, FilteredPartition> entry : data.data.entrySet())
            {
                TxnDataName.serializer.serialize(entry.getKey(), out, version);
                filteredPartitionSerializer.serialize(entry.getValue(), out, version);
            }
        }

        @Override
        public TxnData deserialize(DataInputPlus in, int version) throws IOException
        {
            Map<TxnDataName, FilteredPartition> data = new HashMap<>();
            int size = in.readInt();
            for (int i=0; i<size; i++)
            {
                TxnDataName name = TxnDataName.serializer.deserialize(in, version);
                FilteredPartition partition = filteredPartitionSerializer.deserialize(in, version);
                data.put(name, partition);
            }
            return new TxnData(data);
        }

        @Override
        public long serializedSize(TxnData data, int version)
        {
            long size = TypeSizes.INT_SIZE;
            for (Map.Entry<TxnDataName, FilteredPartition> entry : data.data.entrySet())
            {
                size += TxnDataName.serializer.serializedSize(entry.getKey(), version);
                size += filteredPartitionSerializer.serializedSize(entry.getValue(), version);
            }
            return size;
        }
    };
}
