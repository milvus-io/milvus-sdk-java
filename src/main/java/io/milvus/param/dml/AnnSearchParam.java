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

package io.milvus.param.dml;

import io.milvus.exception.ParamException;
import io.milvus.param.MetricType;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.SortedMap;

/**
 * Parameters for <code>hybridSearch</code> interface.
 */
@Getter
@ToString
public class AnnSearchParam {

    private final String metricType;
    private final String vectorFieldName;
    private final int topK;
    private final String expr;
    private final List<?> vectors;
    private final Long NQ;
    private final String params;

    private AnnSearchParam(@NonNull Builder builder) {
        this.metricType = builder.metricType.name();
        this.vectorFieldName = builder.vectorFieldName;
        this.topK = builder.topK;
        this.expr = builder.expr;
        this.vectors = builder.vectors;
        this.NQ = builder.NQ;
        this.params = builder.params;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link AnnSearchParam} class.
     */
    public static class Builder {
        private MetricType metricType = MetricType.None;
        private String vectorFieldName;
        private Integer topK;
        private String expr = "";
        private List<?> vectors;
        private Long NQ;
        private String params = "{}";

        Builder() {
        }

        /**
         * Sets metric type of ANN searching.
         *
         * @param metricType metric type
         * @return <code>Builder</code>
         */
        public Builder withMetricType(@NonNull MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        /**
         * Sets target vector field by name. Field name cannot be empty or null.
         *
         * @param vectorFieldName vector field name
         * @return <code>Builder</code>
         */
        public Builder withVectorFieldName(@NonNull String vectorFieldName) {
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        /**
         * Sets topK value of ANN search.
         *
         * @param topK topK value
         * @return <code>Builder</code>
         */
        public Builder withTopK(@NonNull Integer topK) {
            this.topK = topK;
            return this;
        }

        /**
         * Sets expression to filter out entities before searching (Optional).
         * @see <a href="https://milvus.io/docs/v2.0.0/boolean.md">Boolean Expression Rules</a>
         *
         * @param expr filtering expression
         * @return <code>Builder</code>
         */
        public Builder withExpr(@NonNull String expr) {
            this.expr = expr;
            return this;
        }

        /**
         * Sets the target vectors.
         * Note: currently, only support one vector for search.
         *
         * @param vectors list of target vectors:
         *                if vector type is FloatVector, vectors is List of List Float;
         *                if vector type is BinaryVector/Float16Vector/BFloat16Vector, vectors is List of ByteBuffer;
         *                if vector type is SparseFloatVector, values is List of SortedMap[Long, Float];
         * @return <code>Builder</code>
         */
        public Builder withVectors(@NonNull List<?> vectors) {
            this.vectors = vectors;
            this.NQ = (long) vectors.size();
            return this;
        }


        /**
         * Sets the search parameters specific to the index type.
         *
         * For example: IVF index, the search parameters can be "{\"nprobe\":10}"
         * For more information: @see <a href="https://milvus.io/docs/v2.0.0/index_selection.md">Index Selection</a>
         *
         * @param params extra parameters in json format
         * @return <code>Builder</code>
         */
        public Builder withParams(@NonNull String params) {
            this.params = params;
            return this;
        }


        /**
         * Verifies parameters and creates a new {@link AnnSearchParam} instance.
         *
         * @return {@link AnnSearchParam}
         */
        public AnnSearchParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(vectorFieldName, "Target field name");

            if (topK <= 0) {
                throw new ParamException("TopK value is illegal");
            }

            if (vectors == null || vectors.isEmpty()) {
                throw new ParamException("Target vectors can not be empty");
            }

            if (vectors.size() != 1) {
                throw new ParamException("Only support one vector for each AnnSearchParam");
            }

            if (vectors.get(0) instanceof List) {
                // float vectors
                // TODO: here only check the first element, potential risk
                List<?> first = (List<?>) vectors.get(0);
                if (!(first.get(0) instanceof Float)) {
                    throw new ParamException("Float vector field's value must be Lst<Float>");
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
                // TODO: here only check the first element, potential risk
                ByteBuffer first = (ByteBuffer) vectors.get(0);
                int dim = first.position();
                for (int i = 1; i < vectors.size(); ++i) {
                    ByteBuffer temp = (ByteBuffer) vectors.get(i);
                    if (dim != temp.position()) {
                        throw new ParamException("Target vector dimension must be equal");
                    }
                }
            } else if (vectors.get(0) instanceof SortedMap) {
                // sparse vectors, must be SortedMap<Long, Float>
                // TODO: here only check the first element, potential risk
                SortedMap<?, ?> map = (SortedMap<?, ?>) vectors.get(0);


            } else {
                String msg = "Search target vector type is illegal." +
                        " Only allow List<Float> for FloatVector," +
                        " ByteBuffer for BinaryVector/Float16Vector/BFloat16Vector," +
                        " List<SortedMap<Long, Float>> for SparseFloatVector.";
                throw new ParamException(msg);
            }

            return new AnnSearchParam(this);
        }
    }

}
