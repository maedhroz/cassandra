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
import java.util.stream.Collectors;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.utils.Generators;

public class InetAddressTest extends AbstractSimpleEqTest
{
    private static final Gen<ByteBuffer> ipv4 = Generators.toGen(Generators.INET_4_ADDRESS_UNRESOLVED_GEN).map(InetAddressType.instance::decompose);
    private static final Gen<ByteBuffer> ipv6 = Generators.toGen(Generators.INET_6_ADDRESS_UNRESOLVED_GEN).map(InetAddressType.instance::decompose);

    private static ByteBuffer mapIpv4ToIpv6(ByteBuffer b)
    {
        ByteBuffer result = ByteBuffer.allocate(16);
        result.put(new byte[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1 });
        result.put(b.duplicate());
        result.flip();
        return result;
    }

    @Test
    public void ipv4()
    {
        test(InetAddressType.instance, null, 200, rs -> {
            var uniq = Gens.lists(ipv4).unique().ofSizeBetween(5, 100).next(rs);
            return r -> r.pick(uniq);
        });
    }

    @Test
    public void ipv6()
    {
        test(InetAddressType.instance, null, 200, rs -> {
            var uniq = Gens.lists(ipv6).unique().ofSizeBetween(5, 100).next(rs);
            return r -> r.pick(uniq);
        });
    }

    @Test
    public void mixIpv6Ipv4()
    {
        test(InetAddressType.instance, null, 200, rs -> {
            var uniqueIpv4 = Gens.lists(ipv4).unique().ofSizeBetween(5, 100).next(rs);
            var uniqueIpv6 = Gens.lists(ipv6).unique().ofSizeBetween(5, 100).next(rs);
            return r -> r.pick(r.nextBoolean() ? uniqueIpv4 : uniqueIpv6);
        });
    }

    @Test
    public void mixIpv6Ipv4AndIpv4AsIpv6()
    {
        test(InetAddressType.instance, null, 200, rs -> {
            var uniqueIpv4 = Gens.lists(ipv4).unique().ofSizeBetween(5, 100).next(rs);
            var uniqueIpv6 = Gens.lists(ipv6).unique().ofSizeBetween(5, 100).next(rs);
            var uniqueIpv4AsIpv6 = Gens.lists(ipv4.map(InetAddressTest::mapIpv4ToIpv6)).unique().ofSizeBetween(5, 100).next(rs);
            return r -> {
                switch (r.nextInt(0, 3))
                {
                    case 0: return r.pick(uniqueIpv4);
                    case 1: return r.pick(uniqueIpv4AsIpv6);
                    case 2: return r.pick(uniqueIpv6);
                    default: throw new AssertionError();
                }
            };
        });
    }

    @Test
    public void ipv4Overlap()
    {
        test(InetAddressType.instance, 8174645098199120693L, 200, rs -> {
            var uniqueIpv4 = Gens.lists(ipv4).unique().ofSizeBetween(5, 100).next(rs);
            var uniqueIpv4AsIpv6 = uniqueIpv4.stream().map(InetAddressTest::mapIpv4ToIpv6).collect(Collectors.toList());
            return r -> r.pick(r.nextBoolean() ? uniqueIpv4 : uniqueIpv4AsIpv6);
        });
    }
}