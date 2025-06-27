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
import io.milvus.grpc.PlaceholderType;
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
public class AnnSearchParam {

    private final String metricType;
    private final String vectorFieldName;
    private final Long topK;
    private final String expr;
    private final List<?> vectors;
    private final Long NQ;
    private final String params;
    private final PlaceholderType plType;

    private AnnSearchParam(@NonNull Builder builder) {
        this.metricType = builder.metricType.name();
        this.vectorFieldName = builder.vectorFieldName;
        this.topK = builder.topK;
        this.expr = builder.expr;
        this.vectors = builder.vectors;
        this.NQ = builder.NQ;
        this.params = builder.params;
        this.plType = builder.plType;
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
        private Long topK;
        private String expr = "";
        private List<?> vectors;
        private Long NQ;
        private String params = "{}";

        // plType is used to distinct vector type
        // for Float16Vector/BFloat16Vector and BinaryVector, user inputs ByteBuffer
        // the sdk cannot distinct a ByteBuffer is a BinarVector or a Float16Vector
        private PlaceholderType plType = PlaceholderType.None;

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
        @Deprecated
        public Builder withTopK(@NonNull Integer topK) {
            this.topK = topK.longValue();
            return this;
        }

        public Builder withLimit(@NonNull Long limit) {
            this.topK = limit;
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
         * Sets the target vectors to search on FloatVector field.
         *
         * @param vectors target vectors to search
         * @return <code>Builder</code>
         */
        public Builder withFloatVectors(@NonNull List<List<Float>> vectors) {
            this.vectors = vectors;
            this.NQ = (long) vectors.size();
            this.plType = PlaceholderType.FloatVector;
            return this;
        }

        /**
         * Sets the target vectors to search on BinaryVector field.
         *
         * @param vectors target vectors to search
         * @return <code>Builder</code>
         */
        public Builder withBinaryVectors(@NonNull List<ByteBuffer> vectors) {
            this.vectors = vectors;
            this.NQ = (long) vectors.size();
            this.plType = PlaceholderType.BinaryVector;
            return this;
        }

        /**
         * Sets the target vectors to search on Float16Vector field.
         *
         * @param vectors target vectors to search
         * @return <code>Builder</code>
         */
        public Builder withFloat16Vectors(@NonNull List<ByteBuffer> vectors) {
            this.vectors = vectors;
            this.NQ = (long) vectors.size();
            this.plType = PlaceholderType.Float16Vector;
            return this;
        }

        /**
         * Sets the target vectors to search on BFloat16Vector field.
         *
         * @param vectors target vectors to search
         * @return <code>Builder</code>
         */
        public Builder withBFloat16Vectors(@NonNull List<ByteBuffer> vectors) {
            this.vectors = vectors;
            this.NQ = (long) vectors.size();
            this.plType = PlaceholderType.BFloat16Vector;
            return this;
        }

        /**
         * Sets the target vectors to search on SparseFloatVector field.
         *
         * @param vectors target vectors to search
         * @return <code>Builder</code>
         */
        public Builder withSparseFloatVectors(@NonNull List<SortedMap<Long, Float>> vectors) {
            this.vectors = vectors;
            this.NQ = (long) vectors.size();
            this.plType = PlaceholderType.SparseFloatVector;
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

            if (vectors.isEmpty()) {
                throw new ParamException("At lease a vector is required for AnnSearchParam");
            }

            SearchParam.verifyVectors(vectors);

            return new AnnSearchParam(this);
        }
    }

    /**
     *
     * Warning: don't use lombok@ToString to annotate this class
     * because large number of vectors will waste time in toString() method.
     *
     */
    @Override
    public String toString() {
        return "AnnSearchParam{" +
                "metricType=" + metricType +
                ", vectorFieldName='" + vectorFieldName + '\'' +
                ", expr='" + expr + '\'' +
                ", topK=" + topK +
                ", nq=" + NQ +
                ", expr='" + expr + '\'' +
                ", params='" + params + '\'' +
                '}';
    }
}
