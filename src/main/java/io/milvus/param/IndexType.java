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
    INVALID,
    FLAT,
    IVF_FLAT,
    IVF_SQ8,
    IVF_PQ,
    HNSW,
    ANNOY,
    RHNSW_FLAT,
    RHNSW_PQ,
    RHNSW_SQ,
    //Only supported for binary vectors
    BIN_FLAT,
    BIN_IVF_FLAT,

    //Only for varchar type field
    TRIE,
    DISKANN
    ;
}
