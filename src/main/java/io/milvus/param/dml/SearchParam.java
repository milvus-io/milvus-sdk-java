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

import com.google.common.collect.Lists;
import io.milvus.exception.ParamException;
import io.milvus.param.MetricType;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>search</code> interface.
 */
@Getter
public class SearchParam {
    private final String collectionName;
    private final List<String> partitionNames;
    private final String metricType;
    private final String vectorFieldName;
    private final int topK;
    private final String expr;
    private final List<String> outFields;
    private final List<?> vectors;
    private final int roundDecimal;
    private final String params;

    private SearchParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.metricType = builder.metricType.name();
        this.vectorFieldName = builder.vectorFieldName;
        this.topK = builder.topK;
        this.expr = builder.expr;
        this.outFields = builder.outFields;
        this.vectors = builder.vectors;
        this.roundDecimal = builder.roundDecimal;
        this.params = builder.params;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for <code>SearchParam</code> class.
     */
    public static class Builder {
        private String collectionName;
        private List<String> partitionNames = Lists.newArrayList();
        private MetricType metricType = MetricType.L2;
        private String vectorFieldName;
        private Integer topK;
        private String expr = "";
        private List<String> outFields = new ArrayList<>();
        private List<?> vectors;
        private Integer roundDecimal = -1;
        private String params = "{}";

       Builder() {
        }

        /**
         * Set collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Optional. Set partition names list to specify search scope.
         *
         * @param partitionNames partition names list
         * @return <code>Builder</code>
         */
        public Builder withPartitionNames(@NonNull List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        /**
         * Set metric type of ANN searching.
         *
         * @param metricType metric type
         * @return <code>Builder</code>
         */
        public Builder withMetricType(@NonNull MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        /**
         * Set target vector field name. Field name cannot be empty or null.
         *
         * @param vectorFieldName vector field name
         * @return <code>Builder</code>
         */
        public Builder withVectorFieldName(@NonNull String vectorFieldName) {
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        /**
         * Set topK value of ANN search.
         *
         * @param topK topK value
         * @return <code>Builder</code>
         */
        public Builder withTopK(@NonNull Integer topK) {
            this.topK = topK;
            return this;
        }

        /**
         * Optional. Set expression to filter out entities before searching.
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
         * Optional. Specify output fields.
         *
         * @param outFields output fields
         * @return <code>Builder</code>
         */
        public Builder withOutFields(@NonNull List<String> outFields) {
            this.outFields = outFields;
            return this;
        }

        /**
         * Set target vectors.
         *
         * @param vectors list of target vectors
         *                If vector type is FloatVector: vectors is List<List<Float>>
         *                If vector type is BinaryVector: vectors is List<ByteBuffer>
         * @return <code>Builder</code>
         */
        public Builder withVectors(@NonNull List<?> vectors) {
            this.vectors = vectors;
            return this;
        }

        /**
         * Specify how many digits after the decimal point for returned results.
         *
         * @param decimal how many digits after the decimal point
         * @return <code>Builder</code>
         */
        public Builder withRoundDecimal(@NonNull Integer decimal) {
            this.roundDecimal = decimal;
            return this;
        }

        /**
         * Set extra search parameters according to index type.
         *
         * For example: IVF index, the extra parameters can be "{\"nprobe\":10}"
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
         * Verify parameters and create a new <code>SearchParam</code> instance.
         *
         * @return <code>SearchParam</code>
         */
        public SearchParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(vectorFieldName, "Target field name");

            if (metricType == MetricType.INVALID) {
                throw new ParamException("Metric type is illegal");
            }

            if (vectors == null || vectors.isEmpty()) {
                throw new ParamException("Target vectors can not be empty");
            }

            if (vectors.get(0) instanceof List) {
                // float vectors
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
                ByteBuffer first = (ByteBuffer) vectors.get(0);
                int dim = first.position();
                for (int i = 1; i < vectors.size(); ++i) {
                    ByteBuffer temp = (ByteBuffer) vectors.get(i);
                    if (dim != temp.position()) {
                        throw new ParamException("Target vector dimension must be equal");
                    }
                }
            } else {
                throw new ParamException("Target vector type must be Lst<Float> or ByteBuffer");
            }

            return new SearchParam(this);
        }
    }

    /**
     * Construct a <code>String</code> by <code>SearchParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "SearchParam{" +
                "collectionName='" + collectionName + '\'' +
                ", partitionNames='" + partitionNames.toString() + '\'' +
                ", metricType=" + metricType +
                ", target vectors count=" + vectors.size() +
                ", vectorFieldName='" + vectorFieldName + '\'' +
                ", topK=" + topK +
                ", expr='" + expr + '\'' +
                ", params='" + params + '\'' +
                '}';
    }
}
