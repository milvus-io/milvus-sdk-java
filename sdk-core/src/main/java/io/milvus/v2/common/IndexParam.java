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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Map;

public class IndexParam {
    private String fieldName;
    private String indexName;
    private IndexType indexType = IndexType.AUTOINDEX;
    private MetricType metricType;
    private Map<String, Object> extraParams;

    // Constructor for builder
    private IndexParam(Builder builder) {
        if (builder.fieldName == null) {
            throw new NullPointerException("fieldName cannot be null");
        }
        this.fieldName = builder.fieldName;
        this.indexName = builder.indexName;
        this.indexType = builder.indexType;
        this.metricType = builder.metricType;
        this.extraParams = builder.extraParams;
    }

    public static Builder builder() {
        return new Builder();
    }

    // Getters
    public String getFieldName() {
        return fieldName;
    }

    public String getIndexName() {
        return indexName;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public MetricType getMetricType() {
        return metricType;
    }

    public Map<String, Object> getExtraParams() {
        return extraParams;
    }

    // Setters
    public void setFieldName(String fieldName) {
        if (fieldName == null) {
            throw new NullPointerException("fieldName cannot be null");
        }
        this.fieldName = fieldName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setIndexType(IndexType indexType) {
        this.indexType = indexType;
    }

    public void setMetricType(MetricType metricType) {
        this.metricType = metricType;
    }

    public void setExtraParams(Map<String, Object> extraParams) {
        this.extraParams = extraParams;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexParam that = (IndexParam) o;

        return new EqualsBuilder()
                .append(fieldName, that.fieldName)
                .append(indexName, that.indexName)
                .append(indexType, that.indexType)
                .append(metricType, that.metricType)
                .append(extraParams, that.extraParams)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(fieldName)
                .append(indexName)
                .append(indexType)
                .append(metricType)
                .append(extraParams)
                .toHashCode();
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
    public static class Builder {
        private String fieldName;
        private String indexName;
        private IndexType indexType = IndexType.AUTOINDEX;
        private MetricType metricType;
        private Map<String, Object> extraParams;

        public Builder fieldName(String fieldName) {
            if (fieldName == null) {
                throw new NullPointerException("fieldName cannot be null");
            }
            this.fieldName = fieldName;
            return this;
        }

        public Builder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder indexType(IndexType indexType) {
            this.indexType = indexType;
            return this;
        }

        public Builder metricType(MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        public Builder extraParams(Map<String, Object> extraParams) {
            this.extraParams = extraParams;
            return this;
        }

        public IndexParam build() {
            return new IndexParam(this);
        }
    }

    public enum MetricType {
        INVALID,
        // Only for float vectors
        L2,
        IP,
        COSINE,

        // Only for binary vectors
        HAMMING,
        JACCARD,
        MHJACCARD,

        // Only for sparse vector with BM25
        BM25,

        // Only for float vector inside struct
        MAX_SIM, // equal to MAX_SIM_COSINE
        MAX_SIM_COSINE,
        MAX_SIM_IP,
        MAX_SIM_L2,
        // Only for binary vector inside struct
        MAX_SIM_JACCARD,
        MAX_SIM_HAMMING,
        ;
    }

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
        IVF_RABITQ(13),

        // GPU indexes only for float vectors
        GPU_IVF_FLAT(50),
        GPU_IVF_PQ(51),
        GPU_BRUTE_FORCE(52),
        GPU_CAGRA(53),

        // Only supported for binary vectors
        BIN_FLAT(80),
        BIN_IVF_FLAT(81),
        MINHASH_LSH(82),

        // Only for varchar type field
        TRIE("Trie", 100),

        // Only for varchar type field and json_path of JSON field
        NGRAM(101),

        // Only for geometry type field
        RTREE(120),

        // Only for scalar type field
        STL_SORT(200), // only for numeric type field
        INVERTED(201), // works for all scalar fields and json_path of JSON field
        BITMAP(202), // works for all scalar fields except JSON, FLOAT and DOUBLE type fields

        // Only for sparse vectors
        SPARSE_INVERTED_INDEX(300),
        // From Milvus 2.5.4 onward, SPARSE_WAND is being deprecated. Instead, it is recommended to
        // use "inverted_index_algo": "DAAT_WAND" for equivalency while maintaining compatibility.
        SPARSE_WAND(301),
        ;

        private final String name;
        private final int code;

        IndexType(){
            this.name = this.toString();
            this.code = this.ordinal();
        }

        IndexType(int code){
            this.name = this.toString();
            this.code = code;
        }

        IndexType(String name, int code){
            this.name = name;
            this.code = code;
        }

        // Getters for enum
        public String getName() {
            return name;
        }

        public int getCode() {
            return code;
        }
    }
}
