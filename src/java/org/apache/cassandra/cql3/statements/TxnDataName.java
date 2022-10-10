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

package org.apache.cassandra.cql3.statements;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.google.common.io.ByteStreams;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TxnDataName implements Comparable<TxnDataName>
{
    public enum Kind
    {
        USER((byte) 1),
        RETURNING((byte) 2),
        AUTO_READ((byte) 3);

        private final byte value;

        Kind(byte value)
        {
            this.value = value;
        }

        public static Kind from(byte b)
        {
            switch (b)
            {
                case 1:
                    return USER;
                case 2:
                    return RETURNING;
                case 3:
                    return AUTO_READ;
                default:
                    throw new IllegalArgumentException("Unknown kind: " + b);
            }
        }
    }

    private final Kind kind;
    private final String[] parts;

    public TxnDataName(Kind kind, String... parts)
    {
        this.kind = kind;
        this.parts = parts;
    }

    public static TxnDataName user(String name)
    {
        return new TxnDataName(Kind.USER, name);
    }

    public static TxnDataName returning()
    {
        return new TxnDataName(Kind.RETURNING);
    }

    public static TxnDataName partitionRead(TableMetadata metadata, DecoratedKey key)
    {
        return new TxnDataName(Kind.AUTO_READ, metadata.keyspace, metadata.name, bytesToString(key.getKey()));
    }

    private static String bytesToString(ByteBuffer bytes)
    {
        byte[] array = ByteBufferUtil.getArray(bytes);
        byte[] compressed = compress(array);
        byte[] chosen = array;
        if (compressed.length < array.length)
            chosen = compressed;
        return ByteBufferUtil.bytesToHex(ByteBuffer.wrap(chosen));
    }

    private static ByteBuffer stringToBytes(String string)
    {
        ByteBuffer bytes = ByteBufferUtil.hexToBytes(string);
        if (bytes.remaining() > 2 && maybeGzip(bytes.get(0), bytes.get(1)))
        {
            // might be gzip
            bytes = ByteBuffer.wrap(decompress(ByteBufferUtil.getArray(bytes)));
        }
        return bytes;
    }

    private static byte[] compress(byte[] array)
    {
        ByteArrayOutputStream obj = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(obj))
        {
            gzip.write(array);
            gzip.flush();
        }
        catch (IOException e)
        {
            // failed to gzip... just return the original array
            return array;
        }
        return obj.toByteArray();
    }

    private static boolean maybeGzip(byte first, byte second)
    {
        return first == (byte) (GZIPInputStream.GZIP_MAGIC) && second == (byte) (GZIPInputStream.GZIP_MAGIC >> 8);
    }

    private static byte[] decompress(byte[] array)
    {
        if (array.length < 2)
            return array; // can't be gzip
        if (maybeGzip(array[0], array[1]))
        {
            // looks like gzip...
            try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(array)))
            {
                return ByteStreams.toByteArray(gis);
            }
            catch (IOException e)
            {
                // wasn't really gzip...
                return array;
            }
        }
        return array;
    }

    public Kind getKind()
    {
        return kind;
    }

    public List<String> getParts()
    {
        return Collections.unmodifiableList(Arrays.asList(parts));
    }

    public boolean isFor(TableMetadata metadata)
    {
        if (kind != Kind.AUTO_READ)
            return false;
        return metadata.keyspace.equals(parts[0])
               && metadata.name.equals(parts[1]);
    }

    public DecoratedKey getDecoratedKey(TableMetadata metadata)
    {
        checkKind(Kind.AUTO_READ);
        ByteBuffer data = stringToBytes(parts[2]);
        return metadata.partitioner.decorateKey(data);
    }

    private void checkKind(Kind expected)
    {
        if (kind != expected)
            throw new IllegalStateException("Expected kind " + expected + " but is " + kind);
    }

    public long estimatedSizeOnHeap()
    {
        long size = 0;
        for (String part : parts)
            size += part.length();
        return size;
    }

    @Override
    public int compareTo(TxnDataName o)
    {
        int rc = kind.compareTo(o.kind);
        if (rc != 0)
            return rc;
        // same kind has same length
        int size = parts.length;
        assert o.parts.length == size : String.format("Expected other.parts.length == %d but was %d", size, o.parts.length);
        for (int i = 0; i < size; i++)
        {
            rc = parts[i].compareTo(o.parts[i]);
            if (rc != 0)
                return rc;
        }
        return 0;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnDataName that = (TxnDataName) o;
        return kind == that.kind && Arrays.equals(parts, that.parts);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(kind);
        result = 31 * result + Arrays.hashCode(parts);
        return result;
    }

    public String name()
    {
        return String.join(":", parts);
    }

    @Override
    public String toString()
    {
        return kind.name() + ":" + name();
    }

    public static final IVersionedSerializer<TxnDataName> serializer = new IVersionedSerializer<TxnDataName>()
    {
        @Override
        public void serialize(TxnDataName t, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(t.kind.value);
            out.writeInt(t.parts.length);
            for (String part : t.parts)
                out.writeUTF(part);
        }

        @Override
        public TxnDataName deserialize(DataInputPlus in, int version) throws IOException
        {
            Kind kind = Kind.from(in.readByte());
            int length = in.readInt();
            String[] parts = new String[length];
            for (int i = 0; i < length; i++)
                parts[i] = in.readUTF();
            return new TxnDataName(kind, parts);
        }

        @Override
        public long serializedSize(TxnDataName t, int version)
        {
            int size = Byte.BYTES + Integer.BYTES;
            for (String part : t.parts)
                size += TypeSizes.sizeof(part);
            return size;
        }
    };
}
