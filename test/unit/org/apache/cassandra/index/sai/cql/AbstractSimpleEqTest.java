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

package org.apache.cassandra.index.sai.cql;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;

import accord.utils.Gen;
import accord.utils.Property;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public abstract class AbstractSimpleEqTest extends CQLTester
{
    static
    {
        // since this test does frequent truncates, the info table gets updated and forced flushed... which is 90% of the cost of this test...
        // this flag disables that flush
        CassandraRelevantProperties.UNSAFE_SYSTEM.setBoolean(true);
        // The plan is to migrate away from SAI, so rather than hacking around timeout issues; just disable for now
        CassandraRelevantProperties.SAI_TEST_DISABLE_TIMEOUT.setBoolean(true);
    }

    protected void test(AbstractType<?> termType, @Nullable Long seed, int examples, Gen<Gen<ByteBuffer>> distribution)
    {
        String tbl = KEYSPACE + "." + createTable("CREATE TABLE %s (pk int PRIMARY KEY, value " + termType.asCQL3Type() + ")");
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        store.disableAutoCompaction();
        schemaChange(String.format("CREATE INDEX ON %s(value) USING 'sai'", tbl));

        Property.ForBuilder builder = qt().withExamples(examples);
        if (seed != null)
            builder = builder.withSeed(seed);
        builder.check(rs -> {
            store.truncateBlocking();

            Gen<ByteBuffer> support = distribution.next(rs);

            Map<ByteBuffer, IntArrayList> termIndex = new TreeMap<>();

            for (int i = 0; i < 1000; i++)
            {
                ByteBuffer term = support.next(rs);
                execute(String.format("INSERT INTO %s (pk, value) VALUES (?, ?)", tbl), i, term);
                termIndex.computeIfAbsent(term, ignore -> new IntArrayList()).addInt(i);
            }
            
            flush();

            for (var e : termIndex.entrySet())
            {
                ByteBuffer term = e.getKey();
                IntArrayList expected = e.getValue();
                UntypedResultSet result = execute(String.format("SELECT pk, value FROM %s WHERE value=?", tbl), term);
                IntArrayList actual = new IntArrayList(expected.size(), -1);
                for (var row : result)
                {
                    ByteBuffer readValue = row.getBytes("value");
                    Assertions.assertThat(readValue).describedAs("%s != %s", termType.compose(term), termType.compose(readValue)).isEqualTo(term);
                    actual.add(row.getInt("pk"));
                }
                expected.sort(Comparator.naturalOrder());
                actual.sort(Comparator.naturalOrder());
                Assertions.assertThat(actual).describedAs("Unexpected partitions for term %s", termType.compose(term)).isEqualTo(expected);
            }
        });
    }
}