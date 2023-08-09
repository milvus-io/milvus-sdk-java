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
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.MetricType;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import java.nio.ByteBuffer;
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
    private final Long NQ;
    private final int roundDecimal;
    private final String params;
    private final long travelTimestamp;
    private final long guaranteeTimestamp;
    private final Long gracefulTime;
    private final ConsistencyLevelEnum consistencyLevel;
    private final boolean ignoreGrowing;

    private SearchParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.metricType = builder.metricType.name();
        this.vectorFieldName = builder.vectorFieldName;
        this.topK = builder.topK;
        this.expr = builder.expr;
        this.outFields = builder.outFields;
        this.vectors = builder.vectors;
        this.NQ = builder.NQ;
        this.roundDecimal = builder.roundDecimal;
        this.params = builder.params;
        this.travelTimestamp = builder.travelTimestamp;
        this.guaranteeTimestamp = builder.guaranteeTimestamp;
        this.gracefulTime = builder.gracefulTime;
        this.consistencyLevel = builder.consistencyLevel;
        this.ignoreGrowing = builder.ignoreGrowing;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link SearchParam} class.
     */
    public static class Builder {
        private String collectionName;
        private final List<String> partitionNames = Lists.newArrayList();
        private MetricType metricType = MetricType.L2;
        private String vectorFieldName;
        private Integer topK;
        private String expr = "";
        private final List<String> outFields = Lists.newArrayList();
        private List<?> vectors;
        private Long NQ;
        private Integer roundDecimal = -1;
        private String params = "{}";
        private Long travelTimestamp = 0L;
        private Long guaranteeTimestamp = Constant.GUARANTEE_EVENTUALLY_TS;
        private Long gracefulTime = 5000L;
        private ConsistencyLevelEnum consistencyLevel = null;
        private Boolean ignoreGrowing = Boolean.FALSE;

       Builder() {
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets partition names list to specify search scope (Optional).
         *
         * @param partitionNames partition names list
         * @return <code>Builder</code>
         */
        public Builder withPartitionNames(@NonNull List<String> partitionNames) {
            partitionNames.forEach(this::addPartitionName);
            return this;
        }

        /**
         * ConsistencyLevel of consistency level.
         *
         * @param consistencyLevel consistency level
         * @return <code>Builder</code>
         */
        public Builder withConsistencyLevel(ConsistencyLevelEnum consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        /**
         *  Graceful time for BOUNDED Consistency Level
         *
         * Note: This parameter is deprecated from Milvus v2.2.9, user only input consistency level to search.
         *       The time settings of different consistency levels are determined by the server side.
         *       For this reason, this method is marked as Deprecated in Java SDK v2.2.11
         *
         * @param gracefulTime graceful time
         * @return <code>Builder</code>
         */
        @Deprecated
        public Builder withGracefulTime(Long gracefulTime) {
            this.gracefulTime = gracefulTime;
            return this;
        }

        /**
         * Adds a partition to specify search scope (Optional).
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder addPartitionName(@NonNull String partitionName) {
            if (!this.partitionNames.contains(partitionName)) {
                this.partitionNames.add(partitionName);
            }
            return this;
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
         * Specifies output fields (Optional).
         *
         * @param outFields output fields
         * @return <code>Builder</code>
         */
        public Builder withOutFields(@NonNull List<String> outFields) {
            outFields.forEach(this::addOutField);
            return this;
        }

        /**
         * Specifies an output field (Optional).
         *
         * @param fieldName filed name
         * @return <code>Builder</code>
         */
        public Builder addOutField(@NonNull String fieldName) {
            if (!this.outFields.contains(fieldName)) {
                this.outFields.add(fieldName);
            }
            return this;
        }

        /**
         * Sets the target vectors.
         *
         * @param vectors list of target vectors:
         *                if vector type is FloatVector, vectors is List of List Float;
         *                if vector type is BinaryVector, vectors is List of ByteBuffer;
         * @return <code>Builder</code>
         */
        public Builder withVectors(@NonNull List<?> vectors) {
            this.vectors = vectors;
            this.NQ = (long) vectors.size();
            return this;
        }

        /**
         * Specifies the decimal place of the returned results.
         *
         * @param decimal how many digits after the decimal point
         * @return <code>Builder</code>
         */
        public Builder withRoundDecimal(@NonNull Integer decimal) {
            this.roundDecimal = decimal;
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
         * Specify an absolute timestamp in a search to get results based on a data view at a specified point in time.
         * Default value is 0, server executes search on a full data view.
         *
         * @param ts a timestamp value
         * @return <code>Builder</code>
         */
        public Builder withTravelTimestamp(@NonNull Long ts) {
            this.travelTimestamp = ts;
            return this;
        }

        /**
         * Instructs server to see insert/delete operations performed before a provided timestamp.
         * If no such timestamp is specified, the server will wait for the latest operation to finish and search.
         *
         * Note: The timestamp is not an absolute timestamp, it is a hybrid value combined by UTC time and internal flags.
         *  We call it TSO, for more information please refer to: https://github.com/milvus-io/milvus/blob/master/docs/design_docs/milvus_hybrid_ts_en.md
         *  You can get a TSO from insert/delete operations, see the <code>MutationResultWrapper</code> class.
         *  Use an operation's TSO to set this parameter, the server will execute search after this operation is finished.
         *
         * Default value is GUARANTEE_EVENTUALLY_TS, server executes search immediately.
         *
         * Note: This parameter is deprecated from Milvus v2.2.9, user only input consistency level to search.
         *       The time settings of different consistency levels are determined by the server side.
         *       For this reason, this method is marked as Deprecated in Java SDK v2.2.11
         *
         * @param ts a timestamp value
         * @return <code>Builder</code>
         */
        @Deprecated
        public Builder withGuaranteeTimestamp(@NonNull Long ts) {
            this.guaranteeTimestamp = ts;
            return this;
        }

        /**
         * Ignore the growing segments to get best search performance. Default is False.
         * For the user case that don't require data visibility.
         *
         * @param ignoreGrowing <code>Boolean.TRUE</code> ignore, Boolean.FALSE is not
         * @return <code>Builder</code>
         */
        public Builder withIgnoreGrowing(@NonNull Boolean ignoreGrowing) {
            this.ignoreGrowing = ignoreGrowing;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link SearchParam} instance.
         *
         * @return {@link SearchParam}
         */
        public SearchParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(vectorFieldName, "Target field name");

            if (topK <= 0) {
                throw new ParamException("TopK value is illegal");
            }

            if (travelTimestamp < 0) {
                throw new ParamException("The travel timestamp must be greater than 0");
            }

            if (guaranteeTimestamp < 0) {
                throw new ParamException("The guarantee timestamp must be greater than 0");
            }

            if (metricType == MetricType.INVALID) {
                throw new ParamException("Metric type is invalid");
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

                // check metric type
                if (!ParamUtils.IsFloatMetric(metricType)) {
                    throw new ParamException("Target vector is float but metric type is incorrect");
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

                // check metric type
                if (!ParamUtils.IsBinaryMetric(metricType)) {
                    throw new ParamException("Target vector is binary but metric type is incorrect");
                }
            } else {
                throw new ParamException("Target vector type must be List<Float> or ByteBuffer");
            }

            return new SearchParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link SearchParam} instance.
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
                ", nq=" + NQ +
                ", expr='" + expr + '\'' +
                ", params='" + params + '\'' +
                ", consistencyLevel='" + consistencyLevel + '\'' +
                ", ignoreGrowing='" + ignoreGrowing + '\'' +
                '}';
    }
}
