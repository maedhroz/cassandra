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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.cql3.statements.TxnDataName;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;

public class AccordUpdateParameters
{
    private final Map<TableId, UpdateParameters> updateParametersMap = new HashMap<>();
    private final TxnData data;
    private final QueryOptions options;

    public AccordUpdateParameters(TxnData data, QueryOptions options)
    {
        this.data = data;
        this.options = options;
    }

    public TxnData getData()
    {
        return data;
    }

    public UpdateParameters updateParameters(TableMetadata metadata)
    {
        if (!updateParametersMap.containsKey(metadata.id))
        {
            // This is currently only used by GuardRails, but this logically have issues with Accord as drifts in config
            // values could cause unexpected issues in Accord (some nodes reject writes while others accept);
            // for the time being, GuardRails are disabled for Accord queries
            ClientState disabledGuardrails = null;

            // What we use here doesn't matter as they get replaced before actually performing the write.
            // see org.apache.cassandra.service.accord.txn.TxnWrite.Update.write
            int nowInSeconds = 42;
            long timestamp = nowInSeconds;

            //TODO how "should" Accord work with TTL?
            int ttl = metadata.params.defaultTimeToLive;
            UpdateParameters parameters = new UpdateParameters(metadata,
                                                               disabledGuardrails,
                                                               options,
                                                               timestamp,
                                                               nowInSeconds,
                                                               ttl,
                                                               prefetchedRows(metadata));
            updateParametersMap.put(metadata.id, parameters);
        }
        return updateParametersMap.get(metadata.id);
    }

    private Map<DecoratedKey, Partition> prefetchedRows(TableMetadata metadata)
    {
        Map<DecoratedKey, Partition> map = new HashMap<>();
        for (Map.Entry<TxnDataName, FilteredPartition> e : data.entrySet())
        {
            if (!e.getKey().isFor(metadata))
                continue;
            map.put(e.getKey().getDecoratedKey(metadata), e.getValue());
        }
        if (map.isEmpty())
            return Collections.emptyMap();
        return map;
    }
}
