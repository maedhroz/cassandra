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

import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

import accord.api.Result;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Listener;
import accord.local.Listeners;
import accord.local.Status;
import accord.txn.Ballot;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.accord.store.StoredNavigableMap;
import org.apache.cassandra.service.accord.store.StoredSet;
import org.apache.cassandra.service.accord.store.StoredValue;
import org.apache.cassandra.utils.ObjectSizes;

public class AccordCommand extends Command implements AccordStateCache.AccordState<TxnId, AccordCommand>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new AccordCommand(null, null));

    enum SummaryVersion
    {
        VERSION_0(0, MessagingService.current_version);
        final byte version;
        final int msg_version;

        SummaryVersion(int version, int msg_version)
        {
            this.version = (byte) version;
            this.msg_version = msg_version;
        }

        static final SummaryVersion current = VERSION_0;

        static SummaryVersion fromByte(byte b)
        {
            switch (b)
            {
                case 0:
                    return VERSION_0;
                default:
                    throw new IllegalArgumentException();
            }
        }
    }

    private final CommandStore commandStore;
    private final TxnId txnId;
    public final StoredValue<Txn> txn = new StoredValue<>();
    public final StoredValue<Ballot> promised = new StoredValue<>();
    public final StoredValue<Ballot> accepted = new StoredValue<>();
    public final StoredValue<Timestamp> executeAt = new StoredValue<>();
    public final StoredValue<Dependencies> deps = new StoredValue<>();
    public final StoredValue<Writes> writes = new StoredValue<>();
    public final StoredValue<Result> result = new StoredValue<>();

    public final StoredValue<Status> status = new StoredValue<>();

    // TODO: compact binary format for below collections, with a step to materialize when needed
    public final StoredNavigableMap<TxnId, TxnId> waitingOnCommit = new StoredNavigableMap<>();
    public final StoredNavigableMap<Timestamp, TxnId> waitingOnApply = new StoredNavigableMap<>();

    public final StoredSet.DeterministicIdentity<ListenerProxy> storedListeners = new StoredSet.DeterministicIdentity<>();
    private final Listeners transientListeners = new Listeners();

    public AccordCommand(CommandStore commandStore, TxnId txnId)
    {
        this.commandStore = commandStore;
        this.txnId = txnId;
    }

    public AccordCommand initialize()
    {
        status.set(Status.NotWitnessed);
        txn.set(null);
        executeAt.load(null);
        promised.set(Ballot.ZERO);
        accepted.set(Ballot.ZERO);
        deps.set(new Dependencies());
        writes.load(null);
        result.load(null);
        waitingOnCommit.load(new TreeMap<>());
        waitingOnApply.load(new TreeMap<>());
        storedListeners.load(new TreeSet<>());
        return this;
    }

    public boolean hasModifications()
    {
        return txn.hasModifications()
               || promised.hasModifications()
               || accepted.hasModifications()
               || executeAt.hasModifications()
               || deps.hasModifications()
               || writes.hasModifications()
               || result.hasModifications()
               || status.hasModifications()
               || waitingOnCommit.hasModifications()
               || waitingOnApply.hasModifications()
               || storedListeners.hasModifications();
    }

    public boolean isLoaded()
    {
        return txn.isLoaded()
               && promised.isLoaded()
               && accepted.isLoaded()
               && executeAt.isLoaded()
               && deps.isLoaded()
               && writes.isLoaded()
               && result.isLoaded()
               && status.isLoaded()
               && waitingOnCommit.isLoaded()
               && waitingOnApply.isLoaded()
               && storedListeners.isLoaded();
    }

    private void resetModificationFlag()
    {
        txn.clearModifiedFlag();
        promised.clearModifiedFlag();
        accepted.clearModifiedFlag();
        executeAt.clearModifiedFlag();
        deps.clearModifiedFlag();
        writes.clearModifiedFlag();
        result.clearModifiedFlag();
        status.clearModifiedFlag();
        waitingOnCommit.clearModifiedFlag();
        waitingOnApply.clearModifiedFlag();
        storedListeners.clearModifiedFlag();;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordCommand command = (AccordCommand) o;
        return commandStore == command.commandStore
               && txnId.equals(command.txnId)
               && txn.equals(command.txn)
               && promised.equals(command.promised)
               && accepted.equals(command.accepted)
               && executeAt.equals(command.executeAt)
               && deps.equals(command.deps)
               && writes.equals(command.writes)
               && result.equals(command.result)
               && status.equals(command.status)
               && waitingOnCommit.equals(command.waitingOnCommit)
               && waitingOnApply.equals(command.waitingOnApply)
               && storedListeners.equals(command.storedListeners)
               && transientListeners.equals(command.transientListeners);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(commandStore,
                            txnId,
                            txn,
                            promised,
                            accepted,
                            executeAt,
                            deps,
                            writes,
                            result,
                            status,
                            waitingOnCommit,
                            waitingOnApply,
                            storedListeners,
                            transientListeners);
    }

    @Override
    public AccordStateCache.Node<TxnId, AccordCommand> createNode()
    {
        return new AccordStateCache.Node<>(this)
        {
            @Override
            long sizeInBytes(AccordCommand value)
            {
                return value.unsharedSizeOnHeap();
            }
        };
    }

    @Override
    public TxnId key()
    {
        return txnId;
    }

    private long unsharedSizeOnHeap()
    {
        // FIXME (metadata): calculate
        return EMPTY_SIZE + 400;
    }

    @Override
    public TxnId txnId()
    {
        return txnId;
    }

    @Override
    public CommandStore commandStore()
    {
        return commandStore;
    }

    @Override
    public Txn txn()
    {
        return txn.get();
    }

    @Override
    public void txn(Txn txn)
    {
        this.txn.set(txn);
    }

    @Override
    public Ballot promised()
    {
        return promised.get();
    }

    @Override
    public void promised(Ballot ballot)
    {
        this.promised.set(ballot);
    }

    @Override
    public Ballot accepted()
    {
        return accepted.get();
    }

    @Override
    public void accepted(Ballot ballot)
    {
        this.accepted.set(ballot);
    }

    @Override
    public Timestamp executeAt()
    {
        return executeAt.get();
    }

    @Override
    public void executeAt(Timestamp timestamp)
    {
        this.executeAt.set(timestamp);
    }

    @Override
    public Dependencies savedDeps()
    {
        return deps.get();
    }

    @Override
    public void savedDeps(Dependencies deps)
    {
        this.deps.set(deps);
    }

    @Override
    public Writes writes()
    {
        return writes.get();
    }

    @Override
    public void writes(Writes writes)
    {
        this.writes.set(writes);
    }

    @Override
    public Result result()
    {
        return result.get();
    }

    @Override
    public void result(Result result)
    {
        this.result.set(result);
    }

    @Override
    public Status status()
    {
        return status.get();
    }

    @Override
    public void status(Status status)
    {
        this.status.set(status);
    }

    private Listener maybeWrapListener(Listener listener)
    {
        if (listener.isTransient())
            return listener;

        if (listener instanceof AccordCommand)
            return new ListenerProxy.CommandListenerProxy(commandStore, ((AccordCommand) listener).txnId());

        if (listener instanceof AccordCommandsForKey)
            return new ListenerProxy.CommandsForKeyListenerProxy(commandStore, ((AccordCommandsForKey) listener).key());

        throw new RuntimeException("Unhandled non-transient listener: " + listener);
    }

    @Override
    public Command addListener(Listener listener)
    {
        listener = maybeWrapListener(listener);
        if (listener instanceof ListenerProxy)
            storedListeners.blindAdd((ListenerProxy) listener);
        else
            transientListeners.add(listener);
        return this;
    }

    @Override
    public void removeListener(Listener listener)
    {
        listener = maybeWrapListener(listener);
        if (listener instanceof ListenerProxy)
            storedListeners.blindRemove((ListenerProxy) listener);
        else
            transientListeners.remove(listener);
    }

    @Override
    public void notifyListeners()
    {
        // TODO: defer command listeners to executor, call cfk immediately (it's updating uncommitted mapping).
        //  maybe update C* implementation to duplicate status etc into cfk table
        storedListeners.getView().forEach(this);
        transientListeners.forEach(this);
    }

    @Override
    public void clearWaitingOnCommit()
    {
        waitingOnCommit.clear();
    }

    @Override
    public void addWaitingOnCommit(TxnId txnId, Command command)
    {
        waitingOnCommit.blindPut(txnId, command.txnId());
    }

    @Override
    public boolean isWaitingOnCommit()
    {
        return !waitingOnCommit.getView().isEmpty();
    }

    @Override
    public void removeWaitingOnCommit(TxnId txnId)
    {
        waitingOnCommit.blindRemove(txnId);
    }

    @Override
    public Command firstWaitingOnCommit()
    {
        return isWaitingOnCommit() ? commandStore.command(waitingOnCommit.getView().firstEntry().getValue()) : null;
    }

    @Override
    public void clearWaitingOnApply()
    {
        waitingOnApply.clear();
    }

    @Override
    public void addWaitingOnApplyIfAbsent(Timestamp timestamp, Command command)
    {
        waitingOnApply.blindPut(timestamp, command.txnId());
    }

    @Override
    public boolean isWaitingOnApply()
    {
        return !waitingOnApply.getView().isEmpty();
    }

    @Override
    public void removeWaitingOnApply(Timestamp timestamp)
    {
        waitingOnApply.blindRemove(timestamp);
    }

    @Override
    public Command firstWaitingOnApply()
    {
        return isWaitingOnApply() ? commandStore.command(waitingOnApply.getView().firstEntry().getValue()) : null;
    }
}
