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

package org.apache.cassandra.service.accord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.LoggerFactory;

import accord.api.ConfigurationService;
import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topology;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.AccordKey;

/**
 * Currently a stubbed out config service meant to be triggered from a dtest
 */
public class AccordConfigurationService implements ConfigurationService
{
    private final Node.Id localId;
    private final List<Listener> listeners = new ArrayList<>();
    private final List<Topology> epochs = new CopyOnWriteArrayList<>();

    public AccordConfigurationService(Node.Id localId)
    {
        this.localId = localId;
        epochs.add(Topology.EMPTY);
    }

    @Override
    public synchronized void registerListener(Listener listener)
    {
        listeners.add(listener);
    }

    @Override
    public synchronized Topology currentTopology()
    {
        return epochs.get(epochs.size() - 1);
    }

    @Override
    public Topology getTopologyForEpoch(long epoch)
    {
        return epochs.get((int) epoch);
    }

    @Override
    public void fetchTopologyForEpoch(long epoch)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void acknowledgeEpoch(long epoch)
    {
        Topology acknowledged = getTopologyForEpoch(epoch);
        for (Node.Id node : acknowledged.nodes())
        {
            if (node.equals(localId))
                continue;
            for (Listener listener : listeners)
                listener.onEpochSyncComplete(node, epoch);
        }
    }

    public void createEpochFromConfig()
    {
        Topology topology = AccordTopologyUtils.createTopology(epochs.size());
        epochs.add(topology);
        for (Listener listener : listeners)
            listener.onTopologyUpdate(topology);
    }

    public void unsafeReloadEpochFromConfig()
    {
        epochs.clear();
        epochs.add(Topology.EMPTY);
        createEpochFromConfig();
        Topology current = currentTopology();
        StringBuilder sb = new StringBuilder();
        sb.append("Current topology:\n");
        for (Shard shard : current)
        {
            TableMetadata metadata = Schema.instance.getTableMetadata(((AccordKey) shard.range.start()).tableId());
            sb.append('\t').append(metadata).append('\t').append(metadata.id).append('\t').append(shard.range).append('\n');
        }
        LoggerFactory.getLogger(AccordConfigurationService.class).info(sb.toString());
    }
}
