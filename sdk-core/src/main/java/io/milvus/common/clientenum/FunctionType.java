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

package io.milvus.common.clientenum;

/**
 * The type of a function used to build a vector index from one or more scalar fields, such as
 * sparse BM25 or dense text embedding.
 */
public enum FunctionType {
    /**
     * The function type is not specified or unknown.
     */
    UNKNOWN("Unknown", 0), // in milvus-proto, the name is "Unknown"
    /**
     * The BM25 sparse vector function, typically built from a text field.
     */
    BM25("BM25", 1),
    /**
     * The text embedding dense vector function, typically built from a text field.
     */
    TEXTEMBEDDING("TextEmbedding", 2), // in milvus-proto, the name is "TextEmbedding"
    /**
     * The reranking function used in hybrid search.
     */
    RERANK("Rerank", 3),
    /**
     * The MinHash function, typically built from text fields to produce sparse vectors.
     */
    MINHASH("MinHash", 4),
    /**
     * The molecular fingerprint function, typically built from SMILES fields.
     */
    MOLFINGERPRINT("MolFingerprint", 5);

    private final String name;
    private final int code;

    FunctionType(String name, int code) {
        this.name = name;
        this.code = code;
    }

    // Getter method to replace @Getter annotation
    /**
     * Returns the protocol code of the function type.
     *
     * @return the function type code
     */
    public int getCode() {
        return code;
    }

    /**
     * Returns the name of the function type.
     *
     * @return the function type name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the function type that matches the given name, or {@link #UNKNOWN} if no type
     * matches.
     *
     * @param name the function type name
     * @return the matching function type
     */
    public static FunctionType fromName(String name) {
        for (FunctionType type : FunctionType.values()) {
            if (type.getName().equals(name) || type.name().equals(name)) {
                return type;
            }
        }
        return UNKNOWN;
    }

    /**
     * Returns the function type that matches the given protocol code, or {@link #UNKNOWN} if no
     * type matches.
     *
     * @param code the function type code
     * @return the matching function type
     */
    public static FunctionType fromCode(int code) {
        for (FunctionType type : FunctionType.values()) {
            if (type.getCode() == code) {
                return type;
            }
        }
        return UNKNOWN;
    }
}
