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
package org.apache.cassandra.utils;

import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SchemaCQLHelper;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.PingRequest;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.Generate;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.Constraint;

import static org.apache.cassandra.utils.AbstractTypeGenerators.allowReversed;
import static org.apache.cassandra.utils.AbstractTypeGenerators.estimateSize;
import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.apache.cassandra.utils.Generators.IDENTIFIER_GEN;
import static org.apache.cassandra.utils.Generators.SMALL_TIME_SPAN_NANOS;
import static org.apache.cassandra.utils.Generators.TIMESTAMP_NANOS;
import static org.apache.cassandra.utils.Generators.TINY_TIME_SPAN_NANOS;

public final class CassandraGenerators
{
    private static final Pattern NEWLINE_PATTERN = Pattern.compile("\n", Pattern.LITERAL);

    // utility generators for creating more complex types
    private static final Gen<Integer> SMALL_POSITIVE_SIZE_GEN = SourceDSL.integers().between(1, 30);
    private static final Gen<Integer> NETWORK_PORT_GEN = SourceDSL.integers().between(0, 0xFFFF);
    private static final Gen<Boolean> BOOLEAN_GEN = SourceDSL.booleans().all();

    public static final Gen<InetAddressAndPort> INET_ADDRESS_AND_PORT_GEN = rnd -> {
        InetAddress address = Generators.INET_ADDRESS_GEN.generate(rnd);
        return InetAddressAndPort.getByAddressOverrideDefaults(address, NETWORK_PORT_GEN.generate(rnd));
    };

    private static final Gen<IPartitioner> PARTITIONER_GEN = SourceDSL.arbitrary().pick(Murmur3Partitioner.instance,
                                                                                        ByteOrderedPartitioner.instance,
                                                                                        new LocalPartitioner(TimeUUIDType.instance),
                                                                                        OrderPreservingPartitioner.instance,
                                                                                        RandomPartitioner.instance);


    private static final Gen<TableMetadata.Kind> TABLE_KIND_GEN = SourceDSL.arbitrary().pick(TableMetadata.Kind.REGULAR, TableMetadata.Kind.INDEX, TableMetadata.Kind.VIRTUAL);
    public static final Gen<TableMetadata> TABLE_METADATA_GEN = tableMetadataGen(IDENTIFIER_GEN, AbstractTypeGenerators.typeGen(), IDENTIFIER_GEN);

    public static Gen<TableMetadata> tableMetadataGen(Gen<String> identifierGen, Gen<AbstractType<?>> typeGen, Gen<String> keyspaceGen)
    {
        return tableMetadataGenBuilder()
        .withKeyspace(keyspaceGen).withName(identifierGen).withColumnName(identifierGen)
        .withType(typeGen)
        .build();
    }

    private static final Gen<SinglePartitionReadCommand> SINGLE_PARTITION_READ_COMMAND_GEN = gen(rnd -> {
        TableMetadata metadata = TABLE_METADATA_GEN.generate(rnd);
        int nowInSec = (int) rnd.next(Constraint.between(1, Integer.MAX_VALUE));
        ByteBuffer key = partitionKeyDataGen(metadata).generate(rnd);
        //TODO support all fields of SinglePartitionReadCommand
        return SinglePartitionReadCommand.create(metadata, nowInSec, key, Slices.ALL);
    }).describedAs(CassandraGenerators::toStringRecursive);
    private static final Gen<? extends ReadCommand> READ_COMMAND_GEN = Generate.oneOf(SINGLE_PARTITION_READ_COMMAND_GEN)
                                                                               .describedAs(CassandraGenerators::toStringRecursive);

    // Outbound messages
    private static final Gen<ConnectionType> CONNECTION_TYPE_GEN = SourceDSL.arbitrary().pick(ConnectionType.URGENT_MESSAGES, ConnectionType.SMALL_MESSAGES, ConnectionType.LARGE_MESSAGES);
    public static final Gen<Message<PingRequest>> MESSAGE_PING_GEN = CONNECTION_TYPE_GEN
                                                                     .map(t -> Message.builder(Verb.PING_REQ, PingRequest.get(t)).build())
                                                                     .describedAs(CassandraGenerators::toStringRecursive);
    public static final Gen<Message<? extends ReadCommand>> MESSAGE_READ_COMMAND_GEN = READ_COMMAND_GEN
                                                                                       .<Message<? extends ReadCommand>>map(c -> Message.builder(Verb.READ_REQ, c).build())
                                                                                       .describedAs(CassandraGenerators::toStringRecursive);

    private static Gen<Message<NoPayload>> responseGen(Verb verb)
    {
        return gen(rnd -> {
            long timeSpan = SMALL_TIME_SPAN_NANOS.generate(rnd);
            long delay = TINY_TIME_SPAN_NANOS.generate(rnd); // network & processing delay
            long requestCreatedAt = TIMESTAMP_NANOS.generate(rnd);
            long createdAt = requestCreatedAt + delay;
            long expiresAt = requestCreatedAt + timeSpan;
            return Message.builder(verb, NoPayload.noPayload)
                          .withCreatedAt(createdAt)
                          .withExpiresAt(expiresAt)
                          .from(INET_ADDRESS_AND_PORT_GEN.generate(rnd))
                          .build();
        }).describedAs(CassandraGenerators::toStringRecursive);
    }

    public static final Gen<Message<NoPayload>> MUTATION_RSP_GEN = responseGen(Verb.MUTATION_RSP);
    public static final Gen<Message<NoPayload>> READ_REPAIR_RSP_GEN = responseGen(Verb.READ_REPAIR_RSP);

    public static final Gen<Message<?>> MESSAGE_GEN = Generate.oneOf(cast(MESSAGE_PING_GEN),
                                                                     cast(MESSAGE_READ_COMMAND_GEN),
                                                                     cast(MUTATION_RSP_GEN),
                                                                     cast(READ_REPAIR_RSP_GEN))
                                                              .describedAs(CassandraGenerators::toStringRecursive);

    private CassandraGenerators()
    {

    }

    public static TableMetadataGenBuilder tableMetadataGenBuilder()
    {
        return new TableMetadataGenBuilder();
    }

    public static class TableMetadataGenBuilder
    {
        Gen<IPartitioner> partitionerGen = PARTITIONER_GEN;
        Gen<TableId> idGen = Generators.UUID_RANDOM_GEN.map(TableId::fromUUID);
        Gen<String> keyspaceGen = IDENTIFIER_GEN, nameGen = IDENTIFIER_GEN, columnNameGen = IDENTIFIER_GEN;
        Gen<TableMetadata.Kind> kindGen = TABLE_KIND_GEN;
        Gen<AbstractType<?>> typeGen = AbstractTypeGenerators.typeGen();
        Gen<Boolean> counterGen = BOOLEAN_GEN;
        boolean respectSizeLimits = true;
        boolean allowReversed = true;

        public TableMetadataGenBuilder withSizeLimitsRespected()
        {
            respectSizeLimits = true;
            return this;
        }

        public TableMetadataGenBuilder withSizeLimitsNotRespected()
        {
            respectSizeLimits = false;
            return this;
        }

        public TableMetadataGenBuilder withPartitioner(Gen<IPartitioner> partitionerGen)
        {
            this.partitionerGen = partitionerGen;
            return this;
        }

        public TableMetadataGenBuilder withPartitioner(IPartitioner partitioner)
        {
            this.partitionerGen = Generate.constant(partitioner);
            return this;
        }

        public TableMetadataGenBuilder withId(Gen<TableId> idGen)
        {
            this.idGen = idGen;
            return this;
        }

        public TableMetadataGenBuilder withKeyspace(Gen<String> keyspaceGen)
        {
            this.keyspaceGen = keyspaceGen;
            return this;
        }

        public TableMetadataGenBuilder withKeyspace(String keyspace)
        {
            this.keyspaceGen = Generate.constant(keyspace);
            return this;
        }

        public TableMetadataGenBuilder withName(Gen<String> nameGen)
        {
            this.nameGen = nameGen;
            return this;
        }

        public TableMetadataGenBuilder withName(String name)
        {
            this.nameGen = Generate.constant(name);
            return this;
        }

        public TableMetadataGenBuilder withColumnName(Gen<String> columnNameGen)
        {
            this.columnNameGen = columnNameGen;
            return this;
        }

        public TableMetadataGenBuilder withKind(Gen<TableMetadata.Kind> kindGen)
        {
            this.kindGen = kindGen;
            return this;
        }

        public TableMetadataGenBuilder withKind(TableMetadata.Kind kind)
        {
            this.kindGen = Generate.constant(kind);
            return this;
        }

        public TableMetadataGenBuilder withType(Gen<AbstractType<?>> typeGen)
        {
            this.typeGen = typeGen;
            return this;
        }

        public TableMetadataGenBuilder withCounter(Gen<Boolean> counterGen)
        {
            this.counterGen = counterGen;
            return this;
        }

        public TableMetadataGenBuilder withCounter(boolean counter)
        {
            this.counterGen = Generate.constant(counter);
            return this;
        }

        public TableMetadataGenBuilder withReversed()
        {
            allowReversed = true;
            return this;
        }
        public TableMetadataGenBuilder withoutReversed()
        {
            allowReversed = false;
            return this;
        }

        public Gen<TableMetadata> build()
        {
            return gen(rnd -> {
                String ks = keyspaceGen.generate(rnd);
                String name = nameGen.generate(rnd);

                TableMetadata.Builder builder = TableMetadata.builder(ks, name, idGen.generate(rnd))
                                                             .partitioner(partitionerGen.generate(rnd))
                                                             .kind(kindGen.generate(rnd))
                                                             .isCounter(counterGen.generate(rnd))
                                                             .params(TableParams.builder().build());

                // generate columns
                // must have a non-zero amount of partition columns, but may have 0 for the rest; SMALL_POSSITIVE_SIZE_GEN won't return 0
                int numPartitionColumns = SMALL_POSITIVE_SIZE_GEN.generate(rnd);
                int numClusteringColumns = SMALL_POSITIVE_SIZE_GEN.generate(rnd) - 1;
                int numRegularColumns = SMALL_POSITIVE_SIZE_GEN.generate(rnd) - 1;
                int numStaticColumns = SMALL_POSITIVE_SIZE_GEN.generate(rnd) - 1;

                // make sure all UDT are in the same keyspace
                AbstractTypeGenerators.UDT_KEYSPACE.set(ks);
                try
                {
                    Set<String> createdColumnNames = new HashSet<>();
                    long estimatedPartitionSize = 0;
                    for (int i = 0; i < numPartitionColumns; i++)
                    {
                        ColumnMetadata columnDefinition = createColumnDefinition(ks, name, ColumnMetadata.Kind.PARTITION_KEY, columnNameGen, typeGen, createdColumnNames, rnd);
                        long estimatedColumnSize = estimateSize(columnDefinition.type);
                        if (respectSizeLimits && i > 0 && estimatedPartitionSize + estimatedColumnSize > FBUtilities.MAX_UNSIGNED_SHORT)
                            // can't accept the column
                            break;
                        estimatedPartitionSize += estimatedColumnSize;
                        builder.addColumn(columnDefinition);
                    }
                    for (int i = 0; i < numClusteringColumns; i++)
                        builder.addColumn(createColumnDefinition(ks, name, ColumnMetadata.Kind.CLUSTERING, columnNameGen, typeGen, createdColumnNames, rnd));
                    for (int i = 0; i < numStaticColumns; i++)
                        builder.addColumn(createColumnDefinition(ks, name, ColumnMetadata.Kind.STATIC, columnNameGen, typeGen, createdColumnNames, rnd));
                    for (int i = 0; i < numRegularColumns; i++)
                        builder.addColumn(createColumnDefinition(ks, name, ColumnMetadata.Kind.REGULAR, columnNameGen, typeGen, createdColumnNames, rnd));
                }
                finally
                {
                    AbstractTypeGenerators.UDT_KEYSPACE.remove();
                }

                return builder.build();
            }).describedAs(CassandraGenerators::toStringRecursive);
        }

        private ColumnMetadata createColumnDefinition(String ks, String table,
                                                      ColumnMetadata.Kind kind,
                                                      Gen<String> identifierGen,
                                                      Gen<AbstractType<?>> typeGen,
                                                      Set<String> createdColumnNames, /* This is mutated to check for collisions, so has a side effect outside of normal random generation */
                                                      RandomnessSource rnd)
        {
            switch (kind)
            {
                // partition and clustering keys require frozen types, so make sure all types generated will be frozen
                // empty type is also not supported, so filter out
                case PARTITION_KEY:
                case CLUSTERING:
                    typeGen = Generators.filter(typeGen, t -> t != EmptyType.instance).map(AbstractType::freeze);
                    break;
            }
            if (kind == ColumnMetadata.Kind.CLUSTERING && allowReversed)
            {
                // when working on a clustering column, add in reversed types periodically
                typeGen = allowReversed(typeGen);
            }
            // filter for unique names
            String str;
            while (!createdColumnNames.add(str = identifierGen.generate(rnd)))
            {
            }
            ColumnIdentifier name = new ColumnIdentifier(str, true);
            int position = !kind.isPrimaryKeyKind() ? -1 : (int) rnd.next(Constraint.between(0, 30));
            return new ColumnMetadata(ks, table, name, typeGen.generate(rnd), position, kind);
        }
    }

    public static Gen<ByteBuffer[]> partitionKeyArrayDataGen(TableMetadata metadata)
    {
        ImmutableList<ColumnMetadata> columns = metadata.partitionKeyColumns();
        assert !columns.isEmpty() : "Unable to find partition key columns";
        if (columns.size() == 1)
            return getTypeSupport(columns.get(0).type).bytesGen().map(b -> new ByteBuffer[]{ b });
        List<Gen<ByteBuffer>> columnGens = new ArrayList<>(columns.size());
        for (ColumnMetadata cm : columns)
            columnGens.add(getTypeSupport(cm.type).bytesGen());
        return rnd -> {
            ByteBuffer[] buffers = new ByteBuffer[columnGens.size()];
            for (int i = 0; i < columnGens.size(); i++)
                buffers[i] = columnGens.get(i).generate(rnd);
            return buffers;
        };
    }

    public static Map<ColumnIdentifier, Gen<ByteBuffer>> tableData(TableMetadata metadata)
    {
        Map<ColumnIdentifier, Gen<ByteBuffer>> output = new HashMap<>();
        for (ColumnMetadata column : metadata.columns())
            output.put(column.name, getTypeSupport(column.type).bytesGen());
        return output;
    }

    public static Map<ColumnMetadata, Gen<?>> tableDataComposed(TableMetadata metadata)
    {
        Map<ColumnMetadata, Gen<?>> output = new HashMap<>();
        for (ColumnMetadata column : metadata.columns())
            output.put(column, getTypeSupport(column.type).valueGen);
        return output;
    }

    public static Gen<ByteBuffer> partitionKeyDataGen(TableMetadata metadata)
    {
        return partitionKeyArrayDataGen(metadata).map(buffers -> {
            if (buffers.length == 1)
                return buffers[0];
            return CompositeType.build(ByteBufferAccessor.instance, buffers);
        });
    }

    /**
     * Hacky workaround to make sure different generic MessageOut types can be used for {@link #MESSAGE_GEN}.
     */
    private static Gen<Message<?>> cast(Gen<? extends Message<?>> gen)
    {
        return (Gen<Message<?>>) gen;
    }

    /**
     * Java's type inferrence with chaining doesn't work well, so this is used to infer the root type early in cases
     * where javac can't figure it out
     */
    private static <T> Gen<T> gen(Gen<T> fn)
    {
        return fn;
    }

    /**
     * Uses reflection to generate a toString.  This method is aware of common Cassandra classes and can be used for
     * generators or tests to provide more details for debugging.
     */
    public static String toStringRecursive(Object o)
    {
        return ReflectionToStringBuilder.toString(o, new MultilineRecursiveToStringStyle()
        {
            private String spacer = "";

            {
                // common lang uses start/end chars that are not the common ones used, so switch to the common ones
                setArrayStart("[");
                setArrayEnd("]");
                setContentStart("{");
                setContentEnd("}");
                setUseIdentityHashCode(false);
                setUseShortClassName(true);
            }

            protected boolean accept(Class<?> clazz)
            {
                return !clazz.isEnum() // toString enums
                       && Stream.of(clazz.getDeclaredFields()).anyMatch(f -> !Modifier.isStatic(f.getModifiers())); // if no fields, just toString
            }

            public void appendDetail(StringBuffer buffer, String fieldName, Object value)
            {
                if (value instanceof ByteBuffer)
                {
                    value = ByteBufferUtil.bytesToHex((ByteBuffer) value);
                }
                else if (value instanceof AbstractType)
                {
                    value = SchemaCQLHelper.toCqlType((AbstractType) value);
                }
                else if (value instanceof Token || value instanceof InetAddressAndPort || value instanceof FieldIdentifier)
                {
                    value = value.toString();
                }
                else if (value instanceof TableMetadata)
                {
                    // to make sure the correct indents are taken, convert to CQL, then replace newlines with the indents
                    // then prefix with the indents.
                    String cql = SchemaCQLHelper.getTableMetadataAsCQL((TableMetadata) value, null);
                    cql = NEWLINE_PATTERN.matcher(cql).replaceAll(Matcher.quoteReplacement("\n  " + spacer));
                    cql = "\n  " + spacer + cql;
                    value = cql;
                }
                super.appendDetail(buffer, fieldName, value);
            }

            // MultilineRecursiveToStringStyle doesn't look at what was set and instead hard codes the values when it "resets" the level
            protected void setArrayStart(String arrayStart)
            {
                super.setArrayStart(arrayStart.replace("{", "["));
            }

            protected void setArrayEnd(String arrayEnd)
            {
                super.setArrayEnd(arrayEnd.replace("}", "]"));
            }

            protected void setContentStart(String contentStart)
            {
                // use this to infer the spacer since it isn't exposed.
                String[] split = contentStart.split("\n", 2);
                spacer = split.length == 2 ? split[1] : "";
                super.setContentStart(contentStart.replace("[", "{"));
            }

            protected void setContentEnd(String contentEnd)
            {
                super.setContentEnd(contentEnd.replace("]", "}"));
            }
        }, true);
    }
}
