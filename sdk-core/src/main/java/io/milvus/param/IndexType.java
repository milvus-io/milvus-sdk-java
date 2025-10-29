/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.param;

/**
 * Represents the available index types.
 * For more information: @see <a href="https://milvus.io/docs/v2.0.0/index_selection.md">Index Types</a>
 */
public enum IndexType {
    None(0),
    // Only supported for float vectors
    FLAT(1),
    IVF_FLAT(2),
    IVF_SQ8(3),
    IVF_PQ(4),
    HNSW(5),
    HNSW_SQ(6),
    HNSW_PQ(7),
    HNSW_PRQ(8),
    DISKANN(10),
    AUTOINDEX(11),
    SCANN(12),

    // GPU indexes only for float vectors
    GPU_IVF_FLAT(50),
    GPU_IVF_PQ(51),
    GPU_BRUTE_FORCE(52),
    GPU_CAGRA(53),

    // Only supported for binary vectors
    BIN_FLAT(80),
    BIN_IVF_FLAT(81),

    // Only for varchar type field
    TRIE("Trie", 100),
    // Only for scalar type field
    STL_SORT(200), // only for numeric type field
    INVERTED(201), // works for all scalar fields except JSON type field
    BITMAP(202), // works for all scalar fields except JSON, FLOAT and DOUBLE type fields

    // Only for sparse vectors
    SPARSE_INVERTED_INDEX(300),
    SPARSE_WAND(301)
    ;

    private final String name;
    private final int code;

    IndexType(){
        this.name = this.name();
        this.code = this.ordinal();
    }

    IndexType(int code){
        this.name = this.name();
        this.code = code;
    }

    IndexType(String name, int code){
        this.name = name;
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public int getCode() {
        return code;
    }
}
