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

package org.apache.cassandra.distributed.test.sai;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.assertj.core.api.Assertions.assertThat;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class IndexBuildingFailureTest extends TestBaseImpl
{
    @Test
    public void testIndexBuildingFailureDuringImport() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP))
                                           .withInstanceInitializer(ByteBuddyHelper::install)
                                           .start()))
        {
            cluster.disableAutoCompaction(KEYSPACE);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.test (pk int PRIMARY KEY, v text)"));
            
            IInvokableInstance first = cluster.get(1);

            first.executeInternal(withKeyspace("INSERT INTO %s.test(pk, v) VALUES (?, ?)"), 1, "v1");
            first.flush(KEYSPACE);

            Object[][] rs = first.executeInternal(withKeyspace("select pk from %s.test where pk = ?"), 1);
            assertThat(rs.length).isEqualTo(1);

            first.runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("test");
                cfs.clearUnsafe();
            });

            rs = first.executeInternal(withKeyspace("select pk from %s.test where pk = ?"), 1);
            assertThat(rs.length).isEqualTo(0);
            
            cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX test_v_index ON %s.test(v) USING 'StorageAttachedIndex'"));

            rs = first.executeInternal(withKeyspace("select pk from %s.test where v = ?"), "v1");
            assertThat(rs.length).isEqualTo(0);

            first.runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("test");
                
                try
                {
                    cfs.loadNewSSTables();
                }
                catch (RuntimeException e)
                {
                    assertThat(e.getMessage()).contains("Failed adding SSTable");
                }
            });

            rs = first.executeInternal(withKeyspace("select pk from %s.test where pk = ?"), 1);
            assertThat(rs.length).isEqualTo(0);
            rs = first.executeInternal(withKeyspace("select pk from %s.test where v = ?"), "v1");
            assertThat(rs.length).isEqualTo(0);

            // In some cases, the test won't see the compaction interruption from this as an uncought exception, because
            // CompactionExecutor catches it in afterExecute().
        }
    }

    public static class ByteBuddyHelper
    {
        static void install(ClassLoader loader, int node)
        {
            new ByteBuddy().redefine(StorageAttachedIndexBuilder.class)
                           .method(named("isStopRequested"))
                           .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                           .make()
                           .load(loader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static boolean isStopRequested()
        {
            return true;
        }
    }
}