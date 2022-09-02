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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.AccordGenerators.Txn;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.Generators;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.Generate;
import org.quicktheories.generators.SourceDSL;

import static org.quicktheories.QuickTheory.qt;

public class AccordFuzzTest extends TestBaseImpl
{
    private static final Gen<String> nameGen = Generators.SYMBOL_GEN;

    @Test
    @Ignore("Reported already")
    public void repo() throws IOException
    {
        String createStatement = "CREATE TABLE distributed_test_keyspace.a (\n" +
                                 "    a tinyint,\n" +
                                 "    aaa tinyint,\n" +
                                 "    aaaaa tinyint,\n" +
                                 "    aaaaaa tinyint,\n" +
                                 "    aaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaaaaaa smallint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaaabaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaabaaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaabaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaabaaaaaaaaaaaaabaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaabaaaaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaba tinyint,\n" +
                                 "    aaaaaaaaabaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaabaaaaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaabaaaaaaaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaabaaaaa tinyint,\n" +
                                 "    aaaabaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaabaaa tinyint,\n" +
                                 "    aab tinyint,\n" +
                                 "    aabaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaa tinyint,\n" +
                                 "    abaaaaaaaaaaabaaaaaaaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    b tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab smallint,\n" +
                                 "    aaaaabaaaaa tinyint,\n" +
                                 "    aa tinyint,\n" +
                                 "    aaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaaaaaabaaaaaaabaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaaabaaaaa tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaaabaaba tinyint,\n" +
                                 "    aaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    aaaaaaaaaabaaaa tinyint,\n" +
                                 "    aaaaaabaaaaaaaaaaaaaaaaaaabaaaaaaaaaa tinyint,\n" +
                                 "    aaaaabaa tinyint,\n" +
                                 "    aaaabaaaaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    aabaa tinyint,\n" +
                                 "    aabaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa tinyint,\n" +
                                 "    abaaaa tinyint,\n" +
                                 "    acbbcaccbcabcc float,\n" +
                                 "    aaaaaaaaaaaaaaaabaccacaccbaaacbbcaabac smallint,\n" +
                                 "    babbcbaaacbcccacbcaccccbbcacaccbc int,\n" +
                                 "    aaaaaaaaaaaaacbabaaacbcbabacbbaaabccbbb int,\n" +
                                 "    aaabcacccbbabaccc smallint static,\n" +
                                 "    aabccbcaccccaacbbccacabccabbaabbabbbbbbabcbcaab tinyint static,\n" +
                                 "    aacabba double static,\n" +
                                 "    abacccababcbcccaabaacb tinyint static,\n" +
                                 "    abcbcbcacabbcbcbca double static,\n" +
                                 "    abccaabcbcabcabcbacbccbacbaaacaacbbcccacaabbb smallint static,\n" +
                                 "    acaabccbbcbbcbbccbbbbabccabbaabaacbbcacacbcbcbaab int static,\n" +
                                 "    accbcbcbcbbbabcacbcbacccaaabacbaabbccaaaabbc float static,\n" +
                                 "    bacacccccbcccbabaabcabaacbaab float static,\n" +
                                 "    bbaacbcbbbaabacbacabbbbbb smallint static,\n" +
                                 "    bbacbacbaccbaccbccaccacbbbccbaccbccaca int static,\n" +
                                 "    bbbccabaabcabcaacbcbcaaba bigint static,\n" +
                                 "    bccbbcabccaaaaacccaacacccaacacbbaaa tinyint static,\n" +
                                 "    bcccaaabacaccbbccbbbbabacbcababca bigint static,\n" +
                                 "    c tinyint static,\n" +
                                 "    caaaacacaaabcbaaccaacbccaba float static,\n" +
                                 "    cabbaacacacbcaa float static,\n" +
                                 "    cacaaacbcccbbcabcbbacbabbcbabbccabbccccbbbaccb bigint static,\n" +
                                 "    cbacacbabcaacacbacacababbacaaacbb bigint static,\n" +
                                 "    cbbb float static,\n" +
                                 "    cbbbabac float static,\n" +
                                 "    cbbbcbcc double static,\n" +
                                 "    cbbcbacccb bigint static,\n" +
                                 "    abbccbccbabccacbacabcbbbccbcbccabbcbacaacab float,\n" +
                                 "    abcccbccacaabbbabbacabccbccabbaacaccab smallint,\n" +
                                 "    acacaacaccbbaabb smallint,\n" +
                                 "    bacabaccbabcabbabacacaabbaacacabcaaaabbac tinyint,\n" +
                                 "    bcbbaacaccb double,\n" +
                                 "    cabacbaca float,\n" +
                                 "    cb int,\n" +
                                 "    PRIMARY KEY ((a, aaa, aaaaa, aaaaaa, aaaaaaa, aaaaaaaaaaa, aaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaabaaaaa, aaaaaaaaaaaaaabaaaaaaaaaaaaaa, aaaaaaaaaaaabaaaaaaaaaaa, aaaaaaaaaaaabaaaaaaaaaaaaabaaaaaa, aaaaaaaaaabaaaaaaaaaaaaaaaaaa, aaaaaaaaaba, aaaaaaaaabaaaaaaaaaaaaa, aaaaaabaaaaaaaaaaaaaaaaaa, aaaaabaaaaaaaaaaaaaaaaaaaaa, aaaabaaaaa, aaaabaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaabaaa, aab, aabaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaa, abaaaaaaaaaaabaaaaaaaaaaaaaaaaaaaaa, b, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab, aaaaabaaaaa), aa, aaaaaaaaa, aaaaaaaaaaaa, aaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabaaa, aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaabaaaaaaabaaaaaaaaa, aaaaaaaaaaaaaaaaaaabaaaaa, aaaaaaaaaaaaaaaaaabaaba, aaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa, aaaaaaaaaabaaaa, aaaaaabaaaaaaaaaaaaaaaaaaabaaaaaaaaaa, aaaaabaa, aaaabaaaaaaaaaaaaaaaaaa, aabaa, aabaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa, abaaaa, acbbcaccbcabcc, aaaaaaaaaaaaaaaabaccacaccbaaacbbcaabac, babbcbaaacbcccacbcaccccbbcacaccbc, aaaaaaaaaaaaacbabaaacbcbabacbbaaabccbbb)\n" +
                                 ") WITH CLUSTERING ORDER BY (aa ASC, aaaaaaaaa ASC, aaaaaaaaaaaa ASC, aaaaaaaaaaaaaaaaaa ASC, aaaaaaaaaaaaaaaaaaaa ASC, aaaaaaaaaaaaaaaaaaaaaaaaaa ASC, aaaaaaaaaaaaaaaaaaaaaaaaaaaa ASC, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ASC, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabaaa ASC, aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaa ASC, aaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa ASC, aaaaaaaaaaaaaaaaaaaaaaabaaaaaaabaaaaaaaaa ASC, aaaaaaaaaaaaaaaaaaabaaaaa ASC, aaaaaaaaaaaaaaaaaabaaba ASC, aaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa ASC, aaaaaaaaaabaaaa ASC, aaaaaabaaaaaaaaaaaaaaaaaaabaaaaaaaaaa ASC, aaaaabaa ASC, aaaabaaaaaaaaaaaaaaaaaa ASC, aabaa ASC, aabaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa ASC, abaaaa ASC, acbbcaccbcabcc ASC, aaaaaaaaaaaaaaaabaccacaccbaaacbbcaabac ASC, babbcbaaacbcccacbcaccccbbcacaccbc DESC, aaaaaaaaaaaaacbabaaacbcbabacbbaaabccbbb ASC)\n" +
                                 "    AND additional_write_policy = '99p'\n" +
                                 "    AND bloom_filter_fp_chance = 0.01\n" +
                                 "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
                                 "    AND cdc = false\n" +
                                 "    AND comment = ''\n" +
                                 "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}\n" +
                                 "    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n" +
                                 "    AND memtable = 'default'\n" +
                                 "    AND crc_check_chance = 1.0\n" +
                                 "    AND default_time_to_live = 0\n" +
                                 "    AND extensions = {}\n" +
                                 "    AND gc_grace_seconds = 864000\n" +
                                 "    AND max_index_interval = 2048\n" +
                                 "    AND memtable_flush_period_in_ms = 0\n" +
                                 "    AND min_index_interval = 128\n" +
                                 "    AND read_repair = 'BLOCKING'\n" +
                                 "    AND speculative_retry = '99p';\n";
        String cql = "BEGIN TRANSACTION\n" +
                     "  LET caaabcabaacccbcacccaacb = (SELECT aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa, acaabccbbcbbcbbccbbbbabccabbaabaacbbcacacbcbcbaab, cb, bcbbaacaccb, aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaa, abbccbccbabccacbacabcbbbccbcbccabbcbacaacab, aaaaaaaaaaaaaaaaaaaaaaaaaa, bccbbcabccaaaaacccaacacccaacacbbaaa, aaaabaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaacbabaaacbcbabacbbaaabccbbb, aaaaaaaaaaa, aaaaaaaaaaaa, aaaaaaaaaaaaa, cbbbcbcc, bacacccccbcccbabaabcabaacbaab, bbacbacbaccbaccbccaccacbbbccbaccbccaca, aaaaaaaaaaaabaaaaaaaaaaaaabaaaaaa, aaa, aabaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaabaaaaaaaaaaa, aaabcacccbbabaccc, abaaaa, abacccababcbcccaabaacb, aaaaabaa, aabaa, bacabaccbabcabbabacacaabbaacacabcaaaabbac, aaaaaaaaaabaaaa, bbaacbcbbbaabacbacabbbbbb, aaaaaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, abcccbccacaabbbabbacabccbccabbaacaccab, aaaaaabaaaaaaaaaaaaaaaaaa, cabacbaca, aaaaaaaaaba, aaaaaaa, aaaaaaaaaaaaaaaaaabaaba, cbbcbacccb, acbbcaccbcabcc, aaaaaaaaaaaaaabaaaaaaaaaaaaaa, aaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaabaaaaaaabaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab, aaaaabaaaaa, aaaaaaaaaaaaaaaaaa, aaaaaaaaaabaaaaaaaaaaaaaaaaaa, bbbccabaabcabcaacbcbcaaba, aabaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaa, aaaaaaaaabaaaaaaaaaaaaa, babbcbaaacbcccacbcaccccbbcacaccbc, abcbcbcacabbcbcbca, aab, aa, cabbaacacacbcaa, aaaaaaaaaaaaaaaaaaabaaaaa, aabccbcaccccaacbbccacabccabbaabbabbbbbbabcbcaab, aacabba, aaaaaabaaaaaaaaaaaaaaaaaaabaaaaaaaaaa, aaaabaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaabaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaabaccacaccbaaacbbcaabac, aaaaa, cbacacbabcaacacbacacababbacaaacbb, acacaacaccbbaabb\n" +
                     "                                 FROM distributed_test_keyspace.a\n" +
                     "                                 WHERE a = 87 AND aaa = 34 AND aaaaa = 3 AND aaaaaa = -12 AND aaaaaaa = -118 AND aaaaaaaaaaa = 101 AND aaaaaaaaaaaaa = 8 AND aaaaaaaaaaaaaaaaaaaaaaa = 27492 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa = -116 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa = -92 AND aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa = 125 AND aaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaaaa = 0 AND aaaaaaaaaaaaaaaaaaaabaaaaa = 70 AND aaaaaaaaaaaaaabaaaaaaaaaaaaaa = -37 AND aaaaaaaaaaaabaaaaaaaaaaa = -27 AND aaaaaaaaaaaabaaaaaaaaaaaaabaaaaaa = 62 AND aaaaaaaaaabaaaaaaaaaaaaaaaaaa = -70 AND aaaaaaaaaba = 122 AND aaaaaaaaabaaaaaaaaaaaaa = 70 AND aaaaaabaaaaaaaaaaaaaaaaaa = -15 AND aaaaabaaaaaaaaaaaaaaaaaaaaa = -5 AND aaaabaaaaa = 23 AND aaaabaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaabaaa = 80 AND aab = 122 AND aabaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaa = 63 AND abaaaaaaaaaaabaaaaaaaaaaaaaaaaaaaaa = 5 AND b = 73 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab = 24901 AND aaaaabaaaaa = 112\n" +
                     "                                 LIMIT 1);\n" +
                     "  LET acabbcccabacbbcbacbccab = (SELECT a, aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa, b, c, acaabccbbcbbcbbccbbbbabccabbaabaacbbcacacbcbcbaab, cb, bcbbaacaccb, aaaaaaaaaaaaaaaaaaaaaaaaaa, bccbbcabccaaaaacccaacacccaacacbbaaa, aaaabaaaaaaaaaaaaaaaaaa, caaaacacaaabcbaaccaacbccaba, aaaaaaaaaaaaacbabaaacbcbabacbbaaabccbbb, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaa, aaaaaaaaaaaa, aaaaaaaaaaaaa, bcccaaabacaccbbccbbbbabacbcababca, bacacccccbcccbabaabcabaacbaab, bbacbacbaccbaccbccaccacbbbccbaccbccaca, aaa, aabaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa, aaabcacccbbabaccc, abaaaa, abacccababcbcccaabaacb, aaaaabaa, aabaa, bbaacbcbbbaabacbacabbbbbb, aaaaaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, abcccbccacaabbbabbacabccbccabbaacaccab, cbbb, aaaaaaaaaba, aaaaaaaaaaaaaaaaaaaa, cbbbabac, aaaaaaaaaaaaaaaaaabaaba, acbbcaccbcabcc, aaaaaaaaaaaaaabaaaaaaaaaaaaaa, abccaabcbcabcabcbacbccbacbaaacaacbbcccacaabbb, aaaaaaaaa, aaaaaa, aaaaaaaaaaaaaaaaaaaaaaabaaaaaaabaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab, aaaaabaaaaa, aaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaa, aaaaaaaaaabaaaaaaaaaaaaaaaaaa, bbbccabaabcabcaacbcbcaaba, aabaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaa, aaaaaaaaabaaaaaaaaaaaaa, babbcbaaacbcccacbcaccccbbcacaccbc, abcbcbcacabbcbcbca, aaaaaaaaaaaaaaaaaaaabaaaaa, aabccbcaccccaacbbccacabccabbaabbabbbbbbabcbcaab, abaaaaaaaaaaabaaaaaaaaaaaaaaaaaaaaa, aacabba, aaaaabaaaaaaaaaaaaaaaaaaaaa, aaaaaabaaaaaaaaaaaaaaaaaaabaaaaaaaaaa, aaaabaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaabaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaabaccacaccbaaacbbcaabac, aaaaa, cbacacbabcaacacbacacababbacaaacbb, acacaacaccbbaabb\n" +
                     "                                 FROM distributed_test_keyspace.a\n" +
                     "                                 WHERE a = -84 AND aaa = -123 AND aaaaa = -82 AND aaaaaa = -127 AND aaaaaaa = 83 AND aaaaaaaaaaa = 41 AND aaaaaaaaaaaaa = 34 AND aaaaaaaaaaaaaaaaaaaaaaa = 23019 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa = -55 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa = 49 AND aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa = -68 AND aaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaaaa = 107 AND aaaaaaaaaaaaaaaaaaaabaaaaa = 117 AND aaaaaaaaaaaaaabaaaaaaaaaaaaaa = 36 AND aaaaaaaaaaaabaaaaaaaaaaa = -53 AND aaaaaaaaaaaabaaaaaaaaaaaaabaaaaaa = 95 AND aaaaaaaaaabaaaaaaaaaaaaaaaaaa = -39 AND aaaaaaaaaba = 50 AND aaaaaaaaabaaaaaaaaaaaaa = -74 AND aaaaaabaaaaaaaaaaaaaaaaaa = 89 AND aaaaabaaaaaaaaaaaaaaaaaaaaa = 27 AND aaaabaaaaa = 43 AND aaaabaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaabaaa = 64 AND aab = 89 AND aabaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaa = 40 AND abaaaaaaaaaaabaaaaaaaaaaaaaaaaaaaaa = 0 AND b = 88 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab = 10527 AND aaaaabaaaaa = -71\n" +
                     "                                 LIMIT 1);\n" +
                     "  LET ccaaabcbac = (SELECT aabaa, cbacacbabcaacacbacacababbacaaacbb, aaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaaaa\n" +
                     "                    FROM distributed_test_keyspace.a\n" +
                     "                    WHERE a = 36 AND aaa = 121 AND aaaaa = -110 AND aaaaaa = -71 AND aaaaaaa = 74 AND aaaaaaaaaaa = 16 AND aaaaaaaaaaaaa = -115 AND aaaaaaaaaaaaaaaaaaaaaaa = -24653 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa = 58 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa = -128 AND aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa = 23 AND aaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaaaa = 42 AND aaaaaaaaaaaaaaaaaaaabaaaaa = 49 AND aaaaaaaaaaaaaabaaaaaaaaaaaaaa = 39 AND aaaaaaaaaaaabaaaaaaaaaaa = 25 AND aaaaaaaaaaaabaaaaaaaaaaaaabaaaaaa = -28 AND aaaaaaaaaabaaaaaaaaaaaaaaaaaa = -33 AND aaaaaaaaaba = -21 AND aaaaaaaaabaaaaaaaaaaaaa = 13 AND aaaaaabaaaaaaaaaaaaaaaaaa = 52 AND aaaaabaaaaaaaaaaaaaaaaaaaaa = 74 AND aaaabaaaaa = 9 AND aaaabaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaabaaa = -62 AND aab = -122 AND aabaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaa = 53 AND abaaaaaaaaaaabaaaaaaaaaaaaaaaaaaaaa = -41 AND b = -58 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab = -11127 AND aaaaabaaaaa = -39\n" +
                     "                    LIMIT 1);\n" +
                     "  LET bccaccacccabbca = (SELECT abcbcbcacabbcbcbca, b, acaabccbbcbbcbbccbbbbabccabbaabaacbbcacacbcbcbaab, aa, cb, bcbbaacaccb, cabbaacacacbcaa, aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaa, aaaabaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaacbabaaacbcbabacbbaaabccbbb, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabaaa, aaaaaaaaaaaaaaaabaccacaccbaaacbbcaabac, aaaaa, cbacacbabcaacacbacacababbacaaacbb, aaa, abacccababcbcccaabaacb, aaaaabaa, aabaa, bacabaccbabcabbabacacaabbaacacabcaaaabbac, bbaacbcbbbaabacbacabbbbbb, cabacbaca, cbbcbacccb, acbbcaccbcabcc, aaaaaaaaaaaaaabaaaaaaaaaaaaaa, aaaaaaaaa, aaaaaaaaaaaaaaaaaa\n" +
                     "                         FROM distributed_test_keyspace.a\n" +
                     "                         WHERE a = 105 AND aaa = 76 AND aaaaa = -128 AND aaaaaa = -61 AND aaaaaaa = 62 AND aaaaaaaaaaa = -79 AND aaaaaaaaaaaaa = -17 AND aaaaaaaaaaaaaaaaaaaaaaa = 20488 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa = 73 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa = -44 AND aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa = 8 AND aaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaaaa = -114 AND aaaaaaaaaaaaaaaaaaaabaaaaa = 103 AND aaaaaaaaaaaaaabaaaaaaaaaaaaaa = -73 AND aaaaaaaaaaaabaaaaaaaaaaa = 41 AND aaaaaaaaaaaabaaaaaaaaaaaaabaaaaaa = -119 AND aaaaaaaaaabaaaaaaaaaaaaaaaaaa = 23 AND aaaaaaaaaba = -125 AND aaaaaaaaabaaaaaaaaaaaaa = -31 AND aaaaaabaaaaaaaaaaaaaaaaaa = 73 AND aaaaabaaaaaaaaaaaaaaaaaaaaa = -95 AND aaaabaaaaa = 14 AND aaaabaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaabaaa = 105 AND aab = 70 AND aabaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaa = -3 AND abaaaaaaaaaaabaaaaaaaaaaaaaaaaaaaaa = 38 AND b = 74 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab = -14495 AND aaaaabaaaaa = -85\n" +
                     "                         LIMIT 1);\n" +
                     "  LET cbcccccbcccaababcbbcbbbcbccbabcbabc = (SELECT b\n" +
                     "                                             FROM distributed_test_keyspace.a\n" +
                     "                                             WHERE a = 42 AND aaa = 15 AND aaaaa = -33 AND aaaaaa = -54 AND aaaaaaa = 45 AND aaaaaaaaaaa = 23 AND aaaaaaaaaaaaa = -30 AND aaaaaaaaaaaaaaaaaaaaaaa = 1009 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa = 51 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa = -72 AND aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa = -93 AND aaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaaaa = -21 AND aaaaaaaaaaaaaaaaaaaabaaaaa = 6 AND aaaaaaaaaaaaaabaaaaaaaaaaaaaa = -39 AND aaaaaaaaaaaabaaaaaaaaaaa = -112 AND aaaaaaaaaaaabaaaaaaaaaaaaabaaaaaa = -81 AND aaaaaaaaaabaaaaaaaaaaaaaaaaaa = -95 AND aaaaaaaaaba = -63 AND aaaaaaaaabaaaaaaaaaaaaa = 15 AND aaaaaabaaaaaaaaaaaaaaaaaa = 104 AND aaaaabaaaaaaaaaaaaaaaaaaaaa = -93 AND aaaabaaaaa = -92 AND aaaabaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaabaaa = -20 AND aab = -115 AND aabaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaa = -19 AND abaaaaaaaaaaabaaaaaaaaaaaaaaaaaaaaa = -30 AND b = -12 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab = -3607 AND aaaaabaaaaa = -58\n" +
                     "                                             LIMIT 1);\n" +
                     "  LET ccaabcababababbcaccaaaaaccbcca = (SELECT a, aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa, b, c, acaabccbbcbbcbbccbbbbabccabbaabaacbbcacacbcbcbaab, cb, bcbbaacaccb, aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaa, bccbbcabccaaaaacccaacacccaacacbbaaa, aaaabaaaaaaaaaaaaaaaaaa, caaaacacaaabcbaaccaacbccaba, aaaaaaaaaaaaacbabaaacbcbabacbbaaabccbbb, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaa, aaaaaaaaaaaa, aaaaaaaaaaaaa, bcccaaabacaccbbccbbbbabacbcababca, cbbbcbcc, bacacccccbcccbabaabcabaacbaab, bbacbacbaccbaccbccaccacbbbccbaccbccaca, aaaaaaaaaaaabaaaaaaaaaaaaabaaaaaa, aabaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaabaaaaaaaaaaa, aaabcacccbbabaccc, aaaabaaaaa, abacccababcbcccaabaacb, accbcbcbcbbbabcacbcbacccaaabacbaabbccaaaabbc, aabaa, bacabaccbabcabbabacacaabbaacacabcaaaabbac, bbaacbcbbbaabacbacabbbbbb, aaaaaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, abcccbccacaabbbabbacabccbccabbaacaccab, aaaaaabaaaaaaaaaaaaaaaaaa, cabacbaca, aaaaaaaaaba, aaaaaaaaaaaaaaaaaaaa, aaaaaaa, cbbbabac, cbbcbacccb, acbbcaccbcabcc, aaaaaaaaaaaaaabaaaaaaaaaaaaaa, abccaabcbcabcabcbacbccbacbaaacaacbbcccacaabbb, aaaaaaaaa, aaaaaa, aaaaaaaaaaaaaaaaaaaaaaabaaaaaaabaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab, aaaaabaaaaa, aaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaa, aaaaaaaaaabaaaaaaaaaaaaaaaaaa, bbbccabaabcabcaacbcbcaaba, aabaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaa, aaaaaaaaabaaaaaaaaaaaaa, babbcbaaacbcccacbcaccccbbcacaccbc, abcbcbcacabbcbcbca, aab, cacaaacbcccbbcabcbbacbabbcbabbccabbccccbbbaccb, aaaaaaaaaaaaaaaaaaaabaaaaa, cabbaacacacbcaa, aaaaaaaaaaaaaaaaaaabaaaaa, aabccbcaccccaacbbccacabccabbaabbabbbbbbabcbcaab, abaaaaaaaaaaabaaaaaaaaaaaaaaaaaaaaa, aaaaabaaaaaaaaaaaaaaaaaaaaa, aaaabaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaabaaa, aaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaaaa, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, aaaaa, cbacacbabcaacacbacacababbacaaacbb, acacaacaccbbaabb\n" +
                     "                                        FROM distributed_test_keyspace.a\n" +
                     "                                        WHERE a = -36 AND aaa = -71 AND aaaaa = 96 AND aaaaaa = 52 AND aaaaaaa = -71 AND aaaaaaaaaaa = 24 AND aaaaaaaaaaaaa = -35 AND aaaaaaaaaaaaaaaaaaaaaaa = 20952 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa = 115 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa = -13 AND aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa = 65 AND aaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaaaa = -88 AND aaaaaaaaaaaaaaaaaaaabaaaaa = -43 AND aaaaaaaaaaaaaabaaaaaaaaaaaaaa = -25 AND aaaaaaaaaaaabaaaaaaaaaaa = 76 AND aaaaaaaaaaaabaaaaaaaaaaaaabaaaaaa = 25 AND aaaaaaaaaabaaaaaaaaaaaaaaaaaa = -75 AND aaaaaaaaaba = -16 AND aaaaaaaaabaaaaaaaaaaaaa = 84 AND aaaaaabaaaaaaaaaaaaaaaaaa = -114 AND aaaaabaaaaaaaaaaaaaaaaaaaaa = -60 AND aaaabaaaaa = 49 AND aaaabaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaabaaa = -18 AND aab = 17 AND aabaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaa = -56 AND abaaaaaaaaaaabaaaaaaaaaaaaaaaaaaaaa = -10 AND b = -1 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab = 566 AND aaaaabaaaaa = 7\n" +
                     "                                        LIMIT 1);\n" +
                     "  LET cbbbcaabbcbacababbbabaacbacccc = (SELECT b, aabaa, abcbcbcacabbcbcbca, aaaaa, bcccaaabacaccbbccbbbbabacbcababca, bccbbcabccaaaaacccaacacccaacacbbaaa, aaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa, aabaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaa\n" +
                     "                                        FROM distributed_test_keyspace.a\n" +
                     "                                        WHERE a = -106 AND aaa = 17 AND aaaaa = 91 AND aaaaaa = 34 AND aaaaaaa = 41 AND aaaaaaaaaaa = 41 AND aaaaaaaaaaaaa = 107 AND aaaaaaaaaaaaaaaaaaaaaaa = -30608 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa = -121 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa = 22 AND aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa = -17 AND aaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaaaa = 87 AND aaaaaaaaaaaaaaaaaaaabaaaaa = 39 AND aaaaaaaaaaaaaabaaaaaaaaaaaaaa = -81 AND aaaaaaaaaaaabaaaaaaaaaaa = 103 AND aaaaaaaaaaaabaaaaaaaaaaaaabaaaaaa = -15 AND aaaaaaaaaabaaaaaaaaaaaaaaaaaa = -119 AND aaaaaaaaaba = -79 AND aaaaaaaaabaaaaaaaaaaaaa = 110 AND aaaaaabaaaaaaaaaaaaaaaaaa = 90 AND aaaaabaaaaaaaaaaaaaaaaaaaaa = -64 AND aaaabaaaaa = -68 AND aaaabaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaabaaa = 7 AND aab = 27 AND aabaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaa = 32 AND abaaaaaaaaaaabaaaaaaaaaaaaaaaaaaaaa = -107 AND b = -66 AND aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab = -4299 AND aaaaabaaaaa = 60\n" +
                     "                                        LIMIT 1);\n" +
                     "  SELECT bccaccacccabbca.b, ccaabcababababbcaccaaaaaccbcca.aaaaaa, cbbbcaabbcbacababbbabaacbacccc.aaaaa, cbbbcaabbcbacababbbabaacbacccc.aabaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa, acabbcccabacbbcbacbccab.abcbcbcacabbcbcbca, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaabaaaaaaaaaaaaa, bccaccacccabbca.aabaa, bccaccacccabbca.cb, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaa, caaabcabaacccbcacccaacb.aaaaaaaaaaaabaaaaaaaaaaa, bccaccacccabbca.cabacbaca, ccaabcababababbcaccaaaaaccbcca.bacacccccbcccbabaabcabaacbaab, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaa, cbbbcaabbcbacababbbabaacbacccc.aabaa, acabbcccabacbbcbacbccab.aaa, bccaccacccabbca.bcbbaacaccb, bccaccacccabbca.abacccababcbcccaabaacb, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaaaaaaaaa, acabbcccabacbbcbacbccab.aaaaaaaaaaaaa, acabbcccabacbbcbacbccab.aaaaaa, ccaabcababababbcaccaaaaaccbcca.acaabccbbcbbcbbccbbbbabccabbaabaacbbcacacbcbcbaab, caaabcabaacccbcacccaacb.aaaabaaaaaaaaaaaaaaaaaa, acabbcccabacbbcbacbccab.aaaaaaaaa, acabbcccabacbbcbacbccab.aaaaaaaaaaaaaaaaaaaaaaa, caaabcabaacccbcacccaacb.aaaaaaaaaaaaaaaabaccacaccbaaacbbcaabac, caaabcabaacccbcacccaacb.cabbaacacacbcaa, acabbcccabacbbcbacbccab.acaabccbbcbbcbbccbbbbabccabbaabaacbbcacacbcbcbaab, bccaccacccabbca.bacabaccbabcabbabacacaabbaacacabcaaaabbac, acabbcccabacbbcbacbccab.acacaacaccbbaabb, ccaabcababababbcaccaaaaaccbcca.abccaabcbcabcabcbacbccbacbaaacaacbbcccacaabbb, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaa, bccaccacccabbca.aaa, caaabcabaacccbcacccaacb.aabaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa, bccaccacccabbca.acbbcaccbcabcc, ccaaabcbac.cbacacbabcaacacbacacababbacaaacbb, acabbcccabacbbcbacbccab.aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa, acabbcccabacbbcbacbccab.abaaaaaaaaaaabaaaaaaaaaaaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaabaaaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaaaaaaaaaaaaaaa, acabbcccabacbbcbacbccab.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaabaaaaaaaaaaaaabaaaaaa, bccaccacccabbca.aaaaaaaaaaaaacbabaaacbcbabacbbaaabccbbb, acabbcccabacbbcbacbccab.aaaaaaaaaaaa, bccaccacccabbca.aaaaaaaaaaaaaaaabaccacaccbaaacbbcaabac, acabbcccabacbbcbacbccab.aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa, cbbbcaabbcbacababbbabaacbacccc.b, acabbcccabacbbcbacbccab.aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.bccbbcabccaaaaacccaacacccaacacbbaaa, caaabcabaacccbcacccaacb.aaaaaaaaaba, acabbcccabacbbcbacbccab.aaaabaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaabaaa, bccaccacccabbca.aaa, bccaccacccabbca.cbbcbacccb, bccaccacccabbca.abacccababcbcccaabaacb, bccaccacccabbca.aaaaaaaaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa, cbbbcaabbcbacababbbabaacbacccc.bcccaaabacaccbbccbbbbabacbcababca, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaabaaaaaaaaaaaaa, acabbcccabacbbcbacbccab.aaaaabaaaaa, acabbcccabacbbcbacbccab.acacaacaccbbaabb, ccaabcababababbcaccaaaaaccbcca.bbbccabaabcabcaacbcbcaaba, acabbcccabacbbcbacbccab.aaaaaabaaaaaaaaaaaaaaaaaaabaaaaaaaaaa, bccaccacccabbca.cbbcbacccb, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.accbcbcbcbbbabcacbcbacccaaabacbaabbccaaaabbc, acabbcccabacbbcbacbccab.cbbb, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaaaaaaaaaaaa, cbcccccbcccaababcbbcbbbcbccbabcbabc, caaabcabaacccbcacccaacb.aaaaaaaaaaaabaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.aabaa, acabbcccabacbbcbacbccab.aaaaaaaaaaaaaaaaaaaaaaabaaaaaaabaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaacbabaaacbcbabacbbaaabccbbb, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabaaa, ccaabcababababbcaccaaaaaccbcca.cabacbaca, bccaccacccabbca.aa, acabbcccabacbbcbacbccab.abaaaa, acabbcccabacbbcbacbccab.aaaaaaaaaaaaaaaaaaaaaaaaaa, acabbcccabacbbcbacbccab.aaaaaaaaaaaaaaaaaaaaaaabaaaaaaabaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.aaaabaaaaa, ccaabcababababbcaccaaaaaccbcca.acaabccbbcbbcbbccbbbbabccabbaabaacbbcacacbcbcbaab, acabbcccabacbbcbacbccab.aaaaaaaaa, acabbcccabacbbcbacbccab.aaaaabaaaaaaaaaaaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, caaabcabaacccbcacccaacb.aaaaaaaaaaaaaaaaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaaabaaaaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.bcccaaabacaccbbccbbbbabacbcababca, acabbcccabacbbcbacbccab.cbbbabac, acabbcccabacbbcbacbccab.b, caaabcabaacccbcacccaacb.cb, bccaccacccabbca.aaaabaaaaaaaaaaaaaaaaaa, caaabcabaacccbcacccaacb.aaaaaaaaaaaa, bccaccacccabbca.aaaabaaaaaaaaaaaaaaaaaa, caaabcabaacccbcacccaacb.aaaabaaaaaaaaaaaaaaaaaa, caaabcabaacccbcacccaacb.abcccbccacaabbbabbacabccbccabbaacaccab, acabbcccabacbbcbacbccab.abaaaaaaaaaaabaaaaaaaaaaaaaaaaaaaaa, caaabcabaacccbcacccaacb.aaaaaaaaaaaaaabaaaaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaabaaaaaaaaaaa, caaabcabaacccbcacccaacb.aaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaa, caaabcabaacccbcacccaacb.bbbccabaabcabcaacbcbcaaba, ccaabcababababbcaccaaaaaccbcca.cabbaacacacbcaa, cbbbcaabbcbacababbbabaacbacccc.bccbbcabccaaaaacccaacacccaacacbbaaa, ccaaabcbac.aaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaaaa, acabbcccabacbbcbacbccab.acaabccbbcbbcbbccbbbbabccabbaabaacbbcacacbcbcbaab, ccaabcababababbcaccaaaaaccbcca.abcbcbcacabbcbcbca, acabbcccabacbbcbacbccab.aabaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaa, acabbcccabacbbcbacbccab.aaaaaaaaaaaaacbabaaacbcbabacbbaaabccbbb, acabbcccabacbbcbacbccab.aaabcacccbbabaccc, ccaabcababababbcaccaaaaaccbcca.aaaaaaa, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.aaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaa, caaabcabaacccbcacccaacb.aaaaaaaaaaaaaaaaaabaaba, ccaabcababababbcaccaaaaaccbcca.acbbcaccbcabcc, bccaccacccabbca.abcbcbcacabbcbcbca, bccaccacccabbca.bacabaccbabcabbabacacaabbaacacabcaaaabbac, ccaabcababababbcaccaaaaaccbcca.cbbbcbcc, ccaabcababababbcaccaaaaaccbcca.acaabccbbcbbcbbccbbbbabccabbaabaacbbcacacbcbcbaab, acabbcccabacbbcbacbccab.aaaaaaaaaaaa, ccaabcababababbcaccaaaaaccbcca.b, caaabcabaacccbcacccaacb.bcbbaacaccb, bccaccacccabbca.aaaaabaa, ccaaabcbac.aaaaaaaaaaaaaaaaaaaaaabaaaaaabaaaaaaaaaaaaaaa;\n" +
                     "COMMIT TRANSACTION\n";
        try (Cluster cluster = createCluster())
        {
            cluster.schemaChange(createStatement);
            ClusterUtils.awaitGossipSchemaMatch(cluster);
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.unsafeReloadEpochFromConfig()));

            cluster.coordinator(1).execute(cql, ConsistencyLevel.ANY);
        }
    }

    @Test
    public void test() throws IOException
    {
        class C {
            final TableMetadata metadata;
            final List<Txn> transactions;

            C(TableMetadata metadata, List<Txn> selects)
            {
                this.metadata = metadata;
                this.transactions = selects;
            }
        }
        Gen<TableMetadata> metadataGen = CassandraGenerators.tableMetadataGen(nameGen, AbstractTypeGenerators.numericTypeGen(), Generate.constant(KEYSPACE));
        Set<String> tables = new HashSet<>();
        Gen<C> gen = rnd -> {
            TableMetadata metadata = metadataGen.generate(rnd);
            while (!tables.add(metadata.name))
                metadata = metadata.unbuild(metadata.keyspace, nameGen.generate(rnd)).build();
            List<Txn> selects = SourceDSL.lists().of(AccordGenerators.txnGen(metadata)).ofSize(10).generate(rnd);
            return new C(metadata, selects);
        };
        try (Cluster cluster = createCluster())
        {
            qt().withExamples(10).withShrinkCycles(0).forAll(gen).checkAssert(c -> {
                String createStatement = c.metadata.toCqlString(false, false);
                LoggerFactory.getLogger(AccordFuzzTest.class).info("Creating table\n{}", createStatement);
                cluster.schemaChange(createStatement);
                ClusterUtils.awaitGossipSchemaMatch(cluster);
                cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.unsafeReloadEpochFromConfig()));

                for (Txn t : c.transactions)
                {
                    try
                    {
                        cluster.coordinator(1).execute(t.toCQL(), ConsistencyLevel.ANY, t.binds);
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException("Table:\n" + createStatement + "\nCQL:\n" + t.toCQL(), e);
                    }
                }
            });
        }
    }

    private static Cluster createCluster() throws IOException
    {
        return init(Cluster.build(2).withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP).set("write_request_timeout", "30s")).start());
    }


}
