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

package org.apache.cassandra.fuzz.ring;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.IntegrationTestBase;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.net.Verb;

import static java.util.Arrays.asList;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.UNSAFE;
import static org.apache.cassandra.harry.ColumnSpec.asciiType;
import static org.apache.cassandra.harry.ColumnSpec.ck;
import static org.apache.cassandra.harry.ColumnSpec.int64Type;
import static org.apache.cassandra.harry.ColumnSpec.pk;
import static org.apache.cassandra.harry.ColumnSpec.regularColumn;
import static org.apache.cassandra.harry.ColumnSpec.staticColumn;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class LossyNetworkTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(LossyNetworkTest.class);

    @BeforeClass
    public static void before() throws Throwable
    {
        init(3, defaultConfig());
    }

    @Test
    public void lossyNetworkTest()
    {
        double chance = 0.05;
        withRandom(rng -> {
            cluster.filters().outbound().messagesMatching((from, to, msg) -> {
                if (msg.verb() == Verb.READ_REQ.id ||
                    msg.verb() == Verb.READ_RSP.id ||
                    msg.verb() == Verb.MUTATION_REQ.id ||
                    msg.verb() == Verb.MUTATION_RSP.id)
                {
                    if (rng.nextDouble() <= chance)
                    {
                        logger.debug("Dropping {} message from {} to {}", Verb.fromId(msg.verb()), from, to);
                        return true;
                    }
                }

                return false;
            }).drop().on();

            SchemaSpec schema = new SchemaSpec(rng.next(),
                                               10_000,
                                               KEYSPACE, "test_table",
                                               asList(pk("pk1", asciiType), pk("pk2", int64Type)),
                                               asList(ck("ck1", asciiType, false), ck("ck2", int64Type, false)),
                                               asList(regularColumn("regular1", asciiType), regularColumn("regular2", int64Type)),
                                               asList(staticColumn("static1", asciiType), staticColumn("static2", int64Type)));

            cluster.schemaChange(schema.compile());

            HistoryBuilder history = new ReplayingHistoryBuilder(schema.valueGenerators,
                                                                 hb -> InJvmDTestVisitExecutor.builder()
                                                                                              .retryPolicy(retry -> {
                                                                                                  logger.debug("Retrying after {}", retry.getMessage());
                                                                                                  return true;
                                                                                              })
                                                                                              .nodeSelector(lts -> {
                                                                                                  while (true)
                                                                                                  {
                                                                                                      int node = rng.nextInt(0, cluster.size()) + 1;
                                                                                                      if (cluster.get(node).isShutdown())
                                                                                                          continue;
                                                                                                      return node;
                                                                                                  }
                                                                                              })
                                                                                              .consistencyLevel(ConsistencyLevel.QUORUM)
                                                                                              .build(schema, hb, cluster));

            Generator<Integer> pkIdxGen = Generators.int32(0, Math.min(10_000, schema.valueGenerators.ckPopulation()));

            List<Throwable> thrown = new ArrayList<>();
            Interruptible executor = executorFactory().infiniteLoop("R/W Worload",
                                                                    () -> {
                                                                        try
                                                                        {
                                                                            history.insert(pkIdxGen.generate(rng));
                                                                            history.selectPartition(pkIdxGen.generate(rng));
                                                                        }
                                                                        catch (Throwable t)
                                                                        {
                                                                            thrown.add(t);
                                                                        }
                                                                    }, UNSAFE);

            Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);

            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.MINUTES);

            if (!thrown.isEmpty())
            {
                Throwable error = new AssertionError("Caught exceptions");
                for (Throwable throwable : thrown)
                    error.addSuppressed(throwable);
                throw error;
            }
        });
    }

}
