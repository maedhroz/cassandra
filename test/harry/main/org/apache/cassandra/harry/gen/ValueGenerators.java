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

package org.apache.cassandra.harry.gen;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.util.IteratorsUtil;

import static org.apache.cassandra.harry.gen.InvertibleGenerator.fromType;
import static org.apache.cassandra.harry.SchemaSpec.cumulativeEntropy;
import static org.apache.cassandra.harry.SchemaSpec.forKeys;

public class ValueGenerators
{
    public final Bijections.IndexedBijection<Object[]> pkGen;
    public final Bijections.IndexedBijection<Object[]> ckGen;

    public final List<Bijections.IndexedBijection<Object>> regularColumnGens;
    public final List<Bijections.IndexedBijection<Object>> staticColumnGens;

    public final List<Comparator<Object>> pkComparators;
    public final List<Comparator<Object>> ckComparators;
    public final List<Comparator<Object>> regularComparators;
    public final List<Comparator<Object>> staticComparators;

    public ValueGenerators(Bijections.IndexedBijection<Object[]> pkGen,
                           Bijections.IndexedBijection<Object[]> ckGen,
                           List<Bijections.IndexedBijection<Object>> regularColumnGens,
                           List<Bijections.IndexedBijection<Object>> staticColumnGens,

                           List<Comparator<Object>> pkComparators,
                           List<Comparator<Object>> ckComparators,
                           List<Comparator<Object>> regularComparators,
                           List<Comparator<Object>> staticComparators)
    {
        this.pkGen = pkGen;
        this.ckGen = ckGen;
        this.regularColumnGens = regularColumnGens;
        this.staticColumnGens = staticColumnGens;
        this.pkComparators = pkComparators;
        this.ckComparators = ckComparators;
        this.regularComparators = regularComparators;
        this.staticComparators = staticComparators;
    }

    @SuppressWarnings({ "unchecked" })
    public static ValueGenerators fromSchema(SchemaSpec schema, long seed, int populationPerColumn)
    {
        List<Comparator<Object>> pkComparators = new ArrayList<>();
        List<Comparator<Object>> ckComparators = new ArrayList<>();
        List<Comparator<Object>> regularComparators = new ArrayList<>();
        List<Comparator<Object>> staticComparators = new ArrayList<>();

        for (int i = 0; i < schema.partitionKeys.size(); i++)
            pkComparators.add((Comparator<Object>) schema.partitionKeys.get(i).type.comparator());
        for (int i = 0; i < schema.clusteringKeys.size(); i++)
            ckComparators.add((Comparator<Object>) schema.clusteringKeys.get(i).type.comparator());
        for (int i = 0; i < schema.regularColumns.size(); i++)
            regularComparators.add((Comparator<Object>) schema.regularColumns.get(i).type.comparator());
        for (int i = 0; i < schema.staticColumns.size(); i++)
            staticComparators.add((Comparator<Object>) schema.staticColumns.get(i).type.comparator());

        Map<Generator<Object>, InvertibleGenerator<Object>> map = new HashMap<>();
        for (ColumnSpec<?> column : IteratorsUtil.concat(schema.regularColumns, schema.staticColumns))
        {
            map.computeIfAbsent((Generator<Object>) column.gen(),
                                (a) -> (InvertibleGenerator<Object>) fromType(seed, populationPerColumn, column));
        }

        // TODO: empty gen
        return new ValueGenerators(new InvertibleGenerator<>(seed, cumulativeEntropy(schema.partitionKeys), populationPerColumn, forKeys(schema.partitionKeys), InvertibleGenerator.keyComparator(schema.partitionKeys)),
                                   new InvertibleGenerator<>(seed, cumulativeEntropy(schema.clusteringKeys), populationPerColumn, forKeys(schema.clusteringKeys), InvertibleGenerator.keyComparator(schema.clusteringKeys)),
                                   schema.regularColumns.stream().map(ColumnSpec::gen)
                                                        .map(map::get)
                                                        .collect(Collectors.toList()),
                                   schema.staticColumns.stream().map(ColumnSpec::gen)
                                                       .map(map::get)
                                                       .collect(Collectors.toList()),
                                   pkComparators,
                                   ckComparators,
                                   regularComparators,
                                   staticComparators);
    }

    public int pkPopulation()
    {
        return pkGen.population();
    }

    public int ckPopulation()
    {
        return ckGen.population();
    }

    public int regularPopulation(int i)
    {
        return regularColumnGens.get(i).population();
    }

    public int staticPopulation(int i)
    {
        return staticColumnGens.get(i).population();
    }

}