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

package io.milvus.v2.common;

import java.util.Map;

/**
 * Parameters used to create or describe an index on a collection field.
 */
public class IndexParam {
    private String fieldName;
    private String indexName;
    private IndexType indexType = IndexType.AUTOINDEX;
    private MetricType metricType;
    private Map<String, Object> extraParams;

    // Constructor for builder
    private IndexParam(IndexParamBuilder builder) {
        if (builder.fieldName == null) {
            throw new NullPointerException("fieldName cannot be null");
        }
        this.fieldName = builder.fieldName;
        this.indexName = builder.indexName;
        this.indexType = builder.indexType;
        this.metricType = builder.metricType;
        this.extraParams = builder.extraParams;
    }

    /**
     * Creates a new {@code IndexParam} builder.
     *
     * @return the builder
     */
    public static IndexParamBuilder builder() {
        return new IndexParamBuilder();
    }

    // Getters
    /**
     * Returns the name of the field on which the index is created.
     *
     * @return the field name
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Returns the name of the index.
     *
     * @return the index name
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Returns the index type.
     *
     * @return the index type
     */
    public IndexType getIndexType() {
        return indexType;
    }

    /**
     * Returns the metric type used to measure vector similarity.
     *
     * @return the metric type
     */
    public MetricType getMetricType() {
        return metricType;
    }

    /**
     * Returns the extra index parameters, such as {@code nlist} or {@code M}.
     *
     * @return the extra index parameters
     */
    public Map<String, Object> getExtraParams() {
        return extraParams;
    }

    // Setters
    /**
     * Sets the name of the field on which the index is created.
     *
     * @param fieldName the field name
     */
    public void setFieldName(String fieldName) {
        if (fieldName == null) {
            throw new NullPointerException("fieldName cannot be null");
        }
        this.fieldName = fieldName;
    }

    /**
     * Sets the name of the index.
     *
     * @param indexName the index name
     */
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    /**
     * Sets the index type.
     *
     * @param indexType the index type
     */
    public void setIndexType(IndexType indexType) {
        this.indexType = indexType;
    }

    /**
     * Sets the metric type used to measure vector similarity.
     *
     * @param metricType the metric type
     */
    public void setMetricType(MetricType metricType) {
        this.metricType = metricType;
    }

    /**
     * Sets the extra index parameters.
     *
     * @param extraParams the extra index parameters
     */
    public void setExtraParams(Map<String, Object> extraParams) {
        this.extraParams = extraParams;
    }

    @Override
    public String toString() {
        return "IndexParam{" +
                "fieldName='" + fieldName + '\'' +
                ", indexName='" + indexName + '\'' +
                ", indexType=" + indexType +
                ", metricType=" + metricType +
                ", extraParams=" + extraParams +
                '}';
    }

    // Public Builder class
    /**
     * Builder for {@link IndexParam}.
     */
    public static class IndexParamBuilder {
        private String fieldName;
        private String indexName;
        private IndexType indexType = IndexType.AUTOINDEX;
        private MetricType metricType;
        private Map<String, Object> extraParams;

        /**
         * Sets the name of the field on which the index is created.
         *
         * @param fieldName the field name
         * @return this builder
         */
        public IndexParamBuilder fieldName(String fieldName) {
            if (fieldName == null) {
                throw new NullPointerException("fieldName cannot be null");
            }
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Sets the name of the index.
         *
         * @param indexName the index name
         * @return this builder
         */
        public IndexParamBuilder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        /**
         * Sets the index type.
         *
         * @param indexType the index type
         * @return this builder
         */
        public IndexParamBuilder indexType(IndexType indexType) {
            this.indexType = indexType;
            return this;
        }

        /**
         * Sets the metric type used to measure vector similarity.
         *
         * @param metricType the metric type
         * @return this builder
         */
        public IndexParamBuilder metricType(MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        /**
         * Sets the extra index parameters.
         *
         * @param extraParams the extra index parameters
         * @return this builder
         */
        public IndexParamBuilder extraParams(Map<String, Object> extraParams) {
            this.extraParams = extraParams;
            return this;
        }

        /**
         * Builds the {@link IndexParam}.
         *
         * @return the index parameters
         */
        public IndexParam build() {
            return new IndexParam(this);
        }
    }

    /**
     * Metric type used to measure the similarity between vectors.
     */
    public enum MetricType {
        /**
         * An invalid metric type.
         */
        INVALID,
        // Only for float vectors
        /**
         * Euclidean distance.
         */
        L2,
        /**
         * Inner product.
         */
        IP,
        /**
         * Cosine similarity.
         */
        COSINE,

        // Only for binary vectors
        /**
         * Hamming distance for binary vectors.
         */
        HAMMING,
        /**
         * Jaccard distance for binary vectors.
         */
        JACCARD,
        /**
         * Modified Hamming-Jaccard distance for binary vectors.
         */
        MHJACCARD,

        // Only for sparse vector with BM25
        /**
         * BM25 score for sparse vectors, used in full-text search.
         */
        BM25,

        // Only for float vector inside struct
        /**
         * Maximum similarity, equal to {@code MAX_SIM_COSINE}.
         */
        MAX_SIM, // equal to MAX_SIM_COSINE
        /**
         * Maximum cosine similarity for vectors inside a struct field.
         */
        MAX_SIM_COSINE,
        /**
         * Maximum inner-product similarity for vectors inside a struct field.
         */
        MAX_SIM_IP,
        /**
         * Maximum Euclidean-distance similarity for vectors inside a struct field.
         */
        MAX_SIM_L2,
        // Only for binary vector inside struct
        /**
         * Maximum Jaccard similarity for binary vectors inside a struct field.
         */
        MAX_SIM_JACCARD,
        /**
         * Maximum Hamming similarity for binary vectors inside a struct field.
         */
        MAX_SIM_HAMMING,
        ;
    }

    /**
     * Index type used to create an index on a collection field.
     */
    public enum IndexType {
        /**
         * No index type.
         */
        None(0),
        // Only supported for float vectors
        /**
         * A brute-force index that computes exact distances.
         */
        FLAT(1),
        /**
         * An inverted-file index that partitions vectors into clusters.
         */
        IVF_FLAT(2),
        /**
         * An IVF index that stores scalar-quantized vectors.
         */
        IVF_SQ8(3),
        /**
         * An IVF index that stores product-quantized vectors.
         */
        IVF_PQ(4),
        /**
         * A graph-based index using the Hierarchical Navigable Small World algorithm.
         */
        HNSW(5),
        /**
         * An HNSW index that stores scalar-quantized vectors.
         */
        HNSW_SQ(6),
        /**
         * An HNSW index that stores product-quantized vectors.
         */
        HNSW_PQ(7),
        /**
         * An HNSW index with product-quantized routing and raw-data refinement.
         */
        HNSW_PRQ(8),
        /**
         * A disk-based index designed for large-scale data.
         */
        DISKANN(10),
        /**
         * An index type chosen automatically by Milvus.
         */
        AUTOINDEX(11),
        /**
         * A cluster-based index for efficient approximate search.
         */
        SCANN(12),
        /**
         * An IVF index using residual-adaptive bit quantization.
         */
        IVF_RABITQ(13),
        /**
         * An approximate inverted index using additive quantization.
         */
        AISAQ(14),

        // GPU indexes only for float vectors
        /**
         * A GPU-accelerated IVF-FLAT index.
         */
        GPU_IVF_FLAT(50),
        /**
         * A GPU-accelerated IVF-PQ index.
         */
        GPU_IVF_PQ(51),
        /**
         * A GPU-accelerated brute-force index.
         */
        GPU_BRUTE_FORCE(52),
        /**
         * A GPU-accelerated graph index based on CAGRA.
         */
        GPU_CAGRA(53),

        // Only supported for binary vectors
        /**
         * A brute-force index for binary vectors.
         */
        BIN_FLAT(80),
        /**
         * An inverted-file index for binary vectors.
         */
        BIN_IVF_FLAT(81),
        /**
         * A MinHash-based LSH index for binary vectors.
         */
        MINHASH_LSH(82),

        // Only for varchar type field
        /**
         * A trie index for varchar fields.
         */
        TRIE("Trie", 100),

        // Only for varchar type field and json_path of JSON field
        /**
         * An n-gram index for varchar fields and {@code json_path} of JSON fields.
         */
        NGRAM(101),

        // Only for geometry type field
        /**
         * An R-tree index for geometry fields.
         */
        RTREE(120),

        // Only for scalar type field
        /**
         * A sort-based index for numeric type fields.
         */
        STL_SORT(200), // only for numeric type field
        /**
         * An inverted index that works for all scalar fields and {@code json_path} of JSON fields.
         */
        INVERTED(201), // works for all scalar fields and json_path of JSON field
        /**
         * A bitmap index that works for all scalar fields except JSON, FLOAT and DOUBLE type fields.
         */
        BITMAP(202), // works for all scalar fields except JSON, FLOAT and DOUBLE type fields

        // Only for sparse vectors
        /**
         * An inverted index for sparse vectors.
         */
        SPARSE_INVERTED_INDEX(300),
        // From Milvus 2.5.4 onward, SPARSE_WAND is being deprecated. Instead, it is recommended to
        // use "inverted_index_algo": "DAAT_WAND" for equivalency while maintaining compatibility.
        /**
         * A sparse vector index using the WAND algorithm.
         */
        SPARSE_WAND(301),

        // Appended at the tail rather than grouped with the other varchar-only
        // types: new constants go on the end so no existing entry shifts.
        // Only for varchar type field. Exact byte-level substring index that
        // answers anchored LIKE (prefix/infix/suffix) with no candidate recheck.
        /**
         * An exact byte-level substring index for varchar fields.
         */
        FMINDEX(102),
        ;

        private final String name;
        private final int code;

        IndexType() {
            this.name = this.toString();
            this.code = this.ordinal();
        }

        IndexType(int code) {
            this.name = this.toString();
            this.code = code;
        }

        IndexType(String name, int code) {
            this.name = name;
            this.code = code;
        }

        // Getters for enum
        /**
         * Returns the name of the index type.
         *
         * @return the index type name
         */
        public String getName() {
            return name;
        }

        /**
         * Returns the numeric code of the index type.
         *
         * @return the numeric code
         */
        public int getCode() {
            return code;
        }
    }
}
