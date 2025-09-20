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
import io.milvus.grpc.PlaceholderType;
import io.milvus.param.Constant;
import io.milvus.param.MetricType;
import io.milvus.param.ParamUtils;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.SortedMap;

/**
 * Parameters for <code>searchIterator</code> interface.
 */
public class SearchIteratorParam {
    private final String databaseName;
    private final String collectionName;
    private final List<String> partitionNames;
    private final String metricType;
    private final String vectorFieldName;
    private final Long topK;
    private final String expr;
    private final List<String> outFields;
    private final List<?> vectors;
    private final Long NQ;
    private final int roundDecimal;
    private final String params;
    private final long travelTimestamp; // deprecated
    private final long guaranteeTimestamp; // deprecated
    private final Long gracefulTime; // deprecated
    private final ConsistencyLevelEnum consistencyLevel;
    private final boolean ignoreGrowing;
    private final String groupByFieldName;
    private final PlaceholderType plType;

    private final long batchSize;

    private SearchIteratorParam(Builder builder) {
        // Replace @NonNull logic with explicit null check
        if (builder == null) {
            throw new IllegalArgumentException("builder cannot be null");
        }
        this.databaseName = builder.databaseName;
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
        this.groupByFieldName = builder.groupByFieldName;
        this.plType = builder.plType;
        
        this.batchSize = builder.batchSize;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    // Getter methods to replace @Getter annotation
    public String getDatabaseName() {
        return databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public String getMetricType() {
        return metricType;
    }

    public String getVectorFieldName() {
        return vectorFieldName;
    }

    public Long getTopK() {
        return topK;
    }

    public String getExpr() {
        return expr;
    }

    public List<String> getOutFields() {
        return outFields;
    }

    public List<?> getVectors() {
        return vectors;
    }

    public Long getNQ() {
        return NQ;
    }

    public int getRoundDecimal() {
        return roundDecimal;
    }

    public String getParams() {
        return params;
    }

    public long getTravelTimestamp() {
        return travelTimestamp;
    }

    public long getGuaranteeTimestamp() {
        return guaranteeTimestamp;
    }

    public Long getGracefulTime() {
        return gracefulTime;
    }

    public ConsistencyLevelEnum getConsistencyLevel() {
        return consistencyLevel;
    }

    public boolean isIgnoreGrowing() {
        return ignoreGrowing;
    }

    public String getGroupByFieldName() {
        return groupByFieldName;
    }

    public PlaceholderType getPlType() {
        return plType;
    }

    public long getBatchSize() {
        return batchSize;
    }

    // toString method to replace @ToString annotation
    @Override
    public String toString() {
        return "SearchIteratorParam{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionNames=" + partitionNames +
                ", metricType='" + metricType + '\'' +
                ", vectorFieldName='" + vectorFieldName + '\'' +
                ", topK=" + topK +
                ", expr='" + expr + '\'' +
                ", outFields=" + outFields +
                ", NQ=" + NQ +
                ", roundDecimal=" + roundDecimal +
                ", params='" + params + '\'' +
                ", travelTimestamp=" + travelTimestamp +
                ", guaranteeTimestamp=" + guaranteeTimestamp +
                ", gracefulTime=" + gracefulTime +
                ", consistencyLevel=" + consistencyLevel +
                ", ignoreGrowing=" + ignoreGrowing +
                ", groupByFieldName='" + groupByFieldName + '\'' +
                ", plType=" + plType +
                ", batchSize=" + batchSize +
                '}';
    }

    /**
     * Builder for {@link SearchIteratorParam} class.
     */
    public static class Builder {
        private String databaseName;
        private String collectionName;
        private final List<String> partitionNames = Lists.newArrayList();
        private MetricType metricType = MetricType.None;
        private String vectorFieldName;
        private Long topK = Constant.UNLIMITED_L;
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
        private String groupByFieldName;

        // plType is used to distinct vector type
        // for Float16Vector/BFloat16Vector and BinaryVector, user inputs ByteBuffer
        // the sdk cannot distinct a ByteBuffer is a BinarVector or a Float16Vector
        private PlaceholderType plType = PlaceholderType.None;

        private Long batchSize = 1000L;

        Builder() {
        }

        /**
         * Sets the database name. database name can be nil.
         *
         * @param databaseName database name
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(String collectionName) {
            // Replace @NonNull logic with explicit null check
            if (collectionName == null) {
                throw new IllegalArgumentException("collectionName cannot be null");
            }
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets partition names list to specify search scope (Optional).
         *
         * @param partitionNames partition names list
         * @return <code>Builder</code>
         */
        public Builder withPartitionNames(List<String> partitionNames) {
            // Replace @NonNull logic with explicit null check
            if (partitionNames == null) {
                throw new IllegalArgumentException("partitionNames cannot be null");
            }
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
         * Adds a partition to specify search scope (Optional).
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder addPartitionName(String partitionName) {
            // Replace @NonNull logic with explicit null check
            if (partitionName == null) {
                throw new IllegalArgumentException("partitionName cannot be null");
            }
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
        public Builder withMetricType(MetricType metricType) {
            // Replace @NonNull logic with explicit null check
            if (metricType == null) {
                throw new IllegalArgumentException("metricType cannot be null");
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
            // Replace @NonNull logic with explicit null check
            if (vectorFieldName == null) {
                throw new IllegalArgumentException("vectorFieldName cannot be null");
            }
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        /**
         * Sets topK value of ANN search.
         * withTopK() is deprecated, replaced by withLimit()
         *
         * @param topK topK value
         * @return <code>Builder</code>
         */
        @Deprecated
        public Builder withTopK(Integer topK) {
            // Replace @NonNull logic with explicit null check
            if (topK == null) {
                throw new IllegalArgumentException("topK cannot be null");
            }
            this.topK = topK.longValue();
            return this;
        }

        public Builder withLimit(Long limit) {
            // Replace @NonNull logic with explicit null check
            if (limit == null) {
                throw new IllegalArgumentException("limit cannot be null");
            }
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
        public Builder withExpr(String expr) {
            // Replace @NonNull logic with explicit null check
            if (expr == null) {
                throw new IllegalArgumentException("expr cannot be null");
            }
            this.expr = expr;
            return this;
        }

        /**
         * Specifies output fields (Optional).
         *
         * @param outFields output fields
         * @return <code>Builder</code>
         */
        public Builder withOutFields(List<String> outFields) {
            // Replace @NonNull logic with explicit null check
            if (outFields == null) {
                throw new IllegalArgumentException("outFields cannot be null");
            }
            outFields.forEach(this::addOutField);
            return this;
        }

        /**
         * Specifies an output field (Optional).
         *
         * @param fieldName filed name
         * @return <code>Builder</code>
         */
        public Builder addOutField(String fieldName) {
            // Replace @NonNull logic with explicit null check
            if (fieldName == null) {
                throw new IllegalArgumentException("fieldName cannot be null");
            }
            if (!this.outFields.contains(fieldName)) {
                this.outFields.add(fieldName);
            }
            return this;
        }

        /**
         * Sets the target vectors.
         * Note: Deprecated in v2.4.0, for the reason that the sdk cannot know a ByteBuffer
         *       is a BinarVector or Float16Vector/BFloat16Vector.
         *       Replaced by withFloatVectors/withBinaryVectors/withFloat16Vectors/withBFloat16Vectors/withSparseFloatVectors.
         *       It still works for FloatVector/BinarVector/SparseVector, don't use it for Float16Vector/BFloat16Vector.
         *
         * @param vectors list of target vectors:
         *                if vector type is FloatVector, vectors is List of List Float;
         *                if vector type is BinaryVector, vectors is List of ByteBuffer;
         *                if vector type is SparseFloatVector, values is List of SortedMap[Long, Float];
         * @return <code>Builder</code>
         */
        @Deprecated
        public Builder withVectors(List<?> vectors) {
            // Replace @NonNull logic with explicit null check
            if (vectors == null) {
                throw new IllegalArgumentException("vectors cannot be null");
            }
            this.vectors = vectors;
            this.NQ = (long) vectors.size();
            return this;
        }

        /**
         * Sets the target vectors to search on FloatVector field.
         *
         * @param vectors target vectors to search
         * @return <code>Builder</code>
         */
        public Builder withFloatVectors(List<List<Float>> vectors) {
            // Replace @NonNull logic with explicit null check
            if (vectors == null) {
                throw new IllegalArgumentException("vectors cannot be null");
            }
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
        public Builder withBinaryVectors(List<ByteBuffer> vectors) {
            // Replace @NonNull logic with explicit null check
            if (vectors == null) {
                throw new IllegalArgumentException("vectors cannot be null");
            }
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
        public Builder withFloat16Vectors(List<ByteBuffer> vectors) {
            // Replace @NonNull logic with explicit null check
            if (vectors == null) {
                throw new IllegalArgumentException("vectors cannot be null");
            }
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
        public Builder withBFloat16Vectors(List<ByteBuffer> vectors) {
            // Replace @NonNull logic with explicit null check
            if (vectors == null) {
                throw new IllegalArgumentException("vectors cannot be null");
            }
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
        public Builder withSparseFloatVectors(List<SortedMap<Long, Float>> vectors) {
            // Replace @NonNull logic with explicit null check
            if (vectors == null) {
                throw new IllegalArgumentException("vectors cannot be null");
            }
            this.vectors = vectors;
            this.NQ = (long) vectors.size();
            this.plType = PlaceholderType.SparseFloatVector;
            return this;
        }

        /**
         * Specifies the decimal place of the returned results.
         *
         * @param decimal how many digits after the decimal point
         * @return <code>Builder</code>
         */
        public Builder withRoundDecimal(Integer decimal) {
            // Replace @NonNull logic with explicit null check
            if (decimal == null) {
                throw new IllegalArgumentException("decimal cannot be null");
            }
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
        public Builder withParams(String params) {
            // Replace @NonNull logic with explicit null check
            if (params == null) {
                throw new IllegalArgumentException("params cannot be null");
            }
            this.params = params;
            return this;
        }

        /**
         * Ignore the growing segments to get best search performance. Default is False.
         * For the user case that don't require data visibility.
         *
         * @param ignoreGrowing <code>Boolean.TRUE</code> ignore, Boolean.FALSE is not
         * @return <code>Builder</code>
         */
        public Builder withIgnoreGrowing(Boolean ignoreGrowing) {
            // Replace @NonNull logic with explicit null check
            if (ignoreGrowing == null) {
                throw new IllegalArgumentException("ignoreGrowing cannot be null");
            }
            this.ignoreGrowing = ignoreGrowing;
            return this;
        }

        /**
         * Sets field name to do grouping.
         *
         * @param groupByFieldName field name to do grouping
         * @return <code>Builder</code>
         */
        public Builder withGroupByFieldName(String groupByFieldName) {
            // Replace @NonNull logic with explicit null check
            if (groupByFieldName == null) {
                throw new IllegalArgumentException("groupByFieldName cannot be null");
            }
            this.groupByFieldName = groupByFieldName;
            return this;
        }

        /**
         * Specify a value to control the number of entities returned per batch. Must be a positive value.
         * Default value is 1000, will return without batchSize.
         *
         * @param batchSize a value to define the number of entities returned per batch
         * @return <code>Builder</code>
         */
        public Builder withBatchSize(@NotNull Long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link SearchIteratorParam} instance.
         *
         * @return {@link SearchIteratorParam}
         */
        public SearchIteratorParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(vectorFieldName, "Target field name");

            if (topK != Constant.UNLIMITED_L && topK <= 0) {
                throw new ParamException("TopK value is illegal");
            }

            if (travelTimestamp < 0) {
                throw new ParamException("The travel timestamp must be greater than 0");
            }

            if (guaranteeTimestamp < 0) {
                throw new ParamException("The guarantee timestamp must be greater than 0");
            }

            if (metricType == MetricType.None) {
                throw new ParamException("must specify metricType for search iterator");
            }

            verifyVectors(vectors);
            return new SearchIteratorParam(this);
        }
    }

    public static void verifyVectors(List<?> vectors) {
        if (vectors == null || vectors.isEmpty()) {
            throw new ParamException("Target vectors can not be empty");
        }

        if (vectors.get(0) instanceof List) {
            if (vectors.size() > 1) {
                throw new ParamException("Not support search iteration over multiple vectors at present");
            }

            // float vectors
            List<?> first = (List<?>) vectors.get(0);
            if (!(first.get(0) instanceof Float)) {
                throw new ParamException("Float vector field's value must be Lst<Float>");
            }
        } else if (vectors.get(0) instanceof ByteBuffer) {
            // binary vectors
            if (vectors.size() > 1) {
                throw new ParamException("Not support search iteration over multiple vectors at present");
            }
        } else if (vectors.get(0) instanceof SortedMap) {
            // SparseFloatVector
            if (vectors.size() > 1) {
                throw new ParamException("Not support search iteration over multiple vectors at present");
            }

            // TODO: here only check the first element, potential risk
            SortedMap<?, ?> map = (SortedMap<?, ?>) vectors.get(0);
            if (!(map.firstKey() instanceof Long)) {
                throw new ParamException("key type of SparseFloatVector must be Long");
            }
            if (!(map.get(map.firstKey()) instanceof Float)) {
                throw new ParamException("Value type of SparseFloatVector must be Float");
            }
        } else {
            String msg = "Search target vector type is illegal." +
                    " Only allow List<Float> for FloatVector," +
                    " ByteBuffer for BinaryVector/Float16Vector/BFloat16Vector," +
                    " List<SortedMap<Long, Float>> for SparseFloatVector.";
            throw new ParamException(msg);
        }
    }

}
