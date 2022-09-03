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

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ConfigurationService;
import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

/**
 * Currently a stubbed out config service meant to be triggered from a dtest
 */
public class AccordConfigurationService implements ConfigurationService
{
    private static final Logger logger = LoggerFactory.getLogger(AccordConfigurationService.class);

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
        // since we don't have a dist sys that sets this up, we have to just lie...
        EndpointMapping.knownIds().forEach(id -> {
            for (Listener listener : listeners)
                listener.onEpochSyncComplete(id, topology.epoch());
        });

        Topology current = currentTopology();
        StringBuilder sb = new StringBuilder();
        sb.append("Current topology:\n");
        TableBuilder builder = new TableBuilder(" | ");
        builder.add("Table", "Table ID", "Range");
        for (Shard shard : current)
        {
            TableMetadata metadata = Schema.instance.getTableMetadata(((AccordKey) shard.range.start()).tableId());
            builder.add(metadata.toString(), metadata.id.toString(), shard.range.toString());
        }
        sb.append(builder);
        logger.info(sb.toString());
    }
}
