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

import io.milvus.exception.ParamException;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Defined single search for query node listener send heartbeat.
 */
public class QueryNodeSingleSearch {

    private final String collectionName;
    private final MetricType metricType;
    private final String vectorFieldName;
    private final List<?> vectors;
    private final String params;

    private QueryNodeSingleSearch(Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("Builder cannot be null");
        }
        this.collectionName = builder.collectionName;
        this.metricType = builder.metricType;
        this.vectorFieldName = builder.vectorFieldName;
        this.vectors = builder.vectors;
        this.params = builder.params;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getCollectionName() {
        return collectionName;
    }

    public MetricType getMetricType() {
        return metricType;
    }

    public String getVectorFieldName() {
        return vectorFieldName;
    }

    public List<?> getVectors() {
        return vectors;
    }

    public String getParams() {
        return params;
    }

    /**
     * Builder for {@link QueryNodeSingleSearch}
     */
    public static class Builder {
        private String collectionName;
        private MetricType metricType = MetricType.L2;
        private String vectorFieldName;
        private List<?> vectors;
        private String params = "{}";

        private Builder() {
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("Collection name cannot be null");
            }
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets metric type of ANN searching.
         *
         * @param metricType metric type
         * @return <code>Builder</code>
         */
        public Builder withMetricType(MetricType metricType) {
            if (metricType == null) {
                throw new IllegalArgumentException("Metric type cannot be null");
            }
            this.metricType = metricType;
            return this;
        }

        /**
         * Sets target vector field by name. Field name cannot be empty or null.
         *
         * @param vectorFieldName vector field name
         * @return <code>Builder</code>
         */
        public Builder withVectorFieldName(String vectorFieldName) {
            if (vectorFieldName == null) {
                throw new IllegalArgumentException("Vector field name cannot be null");
            }
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        /**
         * Sets the target vectors.
         *
         * @param vectors list of target vectors:
         *                if vector type is FloatVector, vectors is List of List Float
         *                if vector type is BinaryVector/Float16Vector/BFloat16Vector, vectors is List of ByteBuffer
         *                if vector type is SparseFloatVector, values is List of SortedMap[Long, Float]
         * @return <code>Builder</code>
         */
        public Builder withVectors(List<?> vectors) {
            if (vectors == null) {
                throw new IllegalArgumentException("Vectors cannot be null");
            }
            this.vectors = vectors;
            return this;
        }

        /**
         * Sets the search parameters specific to the index type.
         * <p>
         * For example: IVF index, the search parameters can be "{\"nprobe\":10}"
         * For more information: @see <a href="https://milvus.io/docs/v2.0.0/index_selection.md">Index Selection</a>
         *
         * @param params extra parameters in json format
         * @return <code>Builder</code>
         */
        public Builder withParams(String params) {
            if (params == null) {
                throw new IllegalArgumentException("Params cannot be null");
            }
            this.params = params;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link QueryNodeSingleSearch} instance.
         *
         * @return {@link QueryNodeSingleSearch}
         */
        public QueryNodeSingleSearch build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(vectorFieldName, "Target field name");

            if (vectors == null || vectors.isEmpty()) {
                throw new ParamException("Target vectors can not be empty");
            }

            if (vectors.get(0) instanceof List) {
                // float vectors
                List<?> first = (List<?>) vectors.get(0);
                if (!(first.get(0) instanceof Float)) {
                    throw new ParamException("Float vector field's value must be List<Float>");
                }

                int dim = first.size();
                for (int i = 1; i < vectors.size(); ++i) {
                    List<?> temp = (List<?>) vectors.get(i);
                    if (dim != temp.size()) {
                        throw new ParamException("Target vector dimension must be equal");
                    }
                }
            } else if (vectors.get(0) instanceof ByteBuffer) {
                // binary vectors
                ByteBuffer first = (ByteBuffer) vectors.get(0);
                int dim = first.limit();
                for (int i = 1; i < vectors.size(); ++i) {
                    ByteBuffer temp = (ByteBuffer) vectors.get(i);
                    if (dim != temp.limit()) {
                        throw new ParamException("Target vector dimension must be equal");
                    }
                }
            } else {
                throw new ParamException("Target vector type must be List<Float> or ByteBuffer");
            }

            return new QueryNodeSingleSearch(this);
        }
    }
}
