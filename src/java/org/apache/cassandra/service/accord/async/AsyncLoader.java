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

package org.apache.cassandra.service.accord.async;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.collect.Iterables;

import accord.txn.TxnId;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.service.accord.AccordCommand;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.AccordStateCache.AccordState;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;


public class AsyncLoader
{
    enum State
    {
        INITIALIZED,
        DISPATCHING,
        LOADING,
        FINISHED
    }

    private State state = State.INITIALIZED;
    private final AccordCommandStore commandStore;
    private final Iterable<TxnId> commandsToLoad;
    private final Iterable<PartitionKey> keyCommandsToLoad;

    protected Future<?> readFuture;

    public AsyncLoader(AccordCommandStore commandStore, TxnId txnId, Iterable<TxnId> depsIds, Iterable<PartitionKey> keys)
    {
        this.commandStore = commandStore;
        this.commandsToLoad = Iterables.concat(Collections.singleton(txnId), depsIds);
        this.keyCommandsToLoad = keys;
    }

    private static <K, V extends AccordState<K, V>> List<Future<?>> referenceAndDispatchReads(Iterable<K> keys,
                                                                                              AccordStateCache.Instance<K, V> cache,
                                                                                              Map<K, V> context,
                                                                                              Predicate<V> isLoaded,
                                                                                              Function<V, Future<?>> readFunction,
                                                                                              List<Future<?>> futures)
    {
        for (K key : keys)
        {
            V item = cache.getOrCreate(key);
            context.put(key, item);

            Future<?> future = cache.getReadFuture(key);

            if (future == null && isLoaded.test(item))
                continue;

            if (futures == null)
                futures = new ArrayList<>();

            if (future == null)
            {
                future = readFunction.apply(item);
                cache.setReadFuture(item.key(), future);
            }
            futures.add(future);
        }
        return futures;
    }

    private Future<?> referenceAndDispatchReads(AsyncContext context)
    {
        List<Future<?>> futures = null;

        futures = referenceAndDispatchReads(commandsToLoad,
                                            commandStore.commandCache(),
                                            context.commands,
                                            AccordCommand::isLoaded,
                                            command -> Stage.READ.submit(() -> AccordKeyspace.loadCommand(command)),
                                            futures);

        futures = referenceAndDispatchReads(keyCommandsToLoad,
                                            commandStore.commandsForKeyCache(),
                                            context.keyCommands,
                                            AccordCommandsForKey::isLoaded,
                                            cfk -> Stage.READ.submit(() -> AccordKeyspace.loadCommandsForKey(cfk)),
                                            futures);

        return futures != null ? FutureCombiner.allOf(futures) : null;
    }

    public boolean load(AsyncContext context, BiConsumer<Object, Throwable> callback)
    {
        switch (state)
        {
            case INITIALIZED:
                state = State.DISPATCHING;
            case DISPATCHING:
                readFuture = referenceAndDispatchReads(context);
                state = State.LOADING;
            case LOADING:
                if (readFuture != null && !readFuture.isDone())
                {
                    readFuture.addCallback(callback, commandStore.executor());
                    break;
                }
                state = State.FINISHED;
            case FINISHED:
                break;
            default:
                throw new IllegalStateException();
        }

        return state == State.FINISHED;
    }
}
