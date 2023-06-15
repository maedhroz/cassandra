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

package org.apache.cassandra.index.sai.disk.format;

import java.util.regex.Pattern;

import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryWriter;
import org.apache.cassandra.io.sstable.Component;

import static org.apache.cassandra.index.sai.disk.format.Version.SAI_DESCRIPTOR;
import static org.apache.cassandra.index.sai.disk.format.Version.SAI_SEPARATOR;


/**
 * This is a definitive list of all the on-disk components for all versions
 */
public enum IndexComponent
{
    /**
     * Metadata for per-column index components
     */
    META("Meta"),

    /**
     * Term dictionary written by {@link TrieTermsDictionaryWriter} stores mappings of term and
     * file pointer to posting block on posting file.
     */
    TERMS_DATA("TermsData"),

    /**
     * Stores postings written by {@link PostingsWriter}
     */
    POSTING_LISTS("PostingLists"),

    /**
     * If present indicates that the column index build completed successfully
     */
    COLUMN_COMPLETION_MARKER("ColumnComplete"),


    // per-sstable components
    /**
     * Partition key token value for rows including row tombstone and static row. (access key is rowId)
     */
    TOKEN_VALUES("TokenValues"),

    /**
     * An on-disk trie containing the primary keys used for looking up the rowId from a partition key
     */
    PRIMARY_KEY_TRIE("PrimaryKeyTrie"),

    /**
     * Prefix-compressed blocks of primary keys used for rowId to partition key lookups
     */
    PRIMARY_KEY_BLOCKS("PrimaryKeyBlocks"),

    /**
     * Encoded sequence of offsets to primary key blocks
     */
    PRIMARY_KEY_BLOCK_OFFSETS("PrimaryKeyBlockOffsets"),

    /**
     * Metadata for per-SSTable on-disk components.
     */
    GROUP_META("GroupMeta"),

    /**
     * If present indicates that the per-sstable index build completed successfully
     */
    GROUP_COMPLETION_MARKER("GroupComplete");

    public final String name;
    public final Component.Type type;

    IndexComponent(String name)
    {
        this.name = name;
        this.type = componentType(name);
    }

    private static Component.Type componentType(String name)
    {
        String componentName = SAI_DESCRIPTOR + SAI_SEPARATOR + name;
        String repr = Pattern.quote(SAI_DESCRIPTOR + SAI_SEPARATOR)
                      + ".*"
                      + Pattern.quote(SAI_SEPARATOR + name + ".db");
        return Component.Type.create(componentName, repr, true, null);
    }
}
