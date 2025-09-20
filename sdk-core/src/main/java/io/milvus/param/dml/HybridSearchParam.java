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
import io.milvus.param.ParamUtils;

import io.milvus.param.dml.ranker.BaseRanker;

import java.util.List;

/**
 * Parameters for <code>search</code> interface.
 */
public class HybridSearchParam {
    private String databaseName;
    private final String collectionName;
    private final List<String> partitionNames;
    private final List<AnnSearchParam> searchRequests;
    private final BaseRanker ranker;
    private final Long topK;
    private final List<String> outFields;
    private final long offset;
    private final int roundDecimal;
    private final ConsistencyLevelEnum consistencyLevel;

    private final String groupByFieldName;
    private final Integer groupSize;
    private final Boolean strictGroupSize;

    private HybridSearchParam(Builder builder) {
        // Replace @NonNull logic with explicit null check
        if (builder == null) {
            throw new IllegalArgumentException("builder cannot be null");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.searchRequests = builder.searchRequests;
        this.ranker = builder.ranker;
        this.topK = builder.topK;
        this.outFields = builder.outFields;
        this.offset = builder.offset;
        this.roundDecimal = builder.roundDecimal;
        this.consistencyLevel = builder.consistencyLevel;
        this.groupByFieldName = builder.groupByFieldName;
        this.groupSize = builder.groupSize;
        this.strictGroupSize = builder.strictGroupSize;
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

    public List<AnnSearchParam> getSearchRequests() {
        return searchRequests;
    }

    public BaseRanker getRanker() {
        return ranker;
    }

    public Long getTopK() {
        return topK;
    }

    public List<String> getOutFields() {
        return outFields;
    }

    public long getOffset() {
        return offset;
    }

    public int getRoundDecimal() {
        return roundDecimal;
    }

    public ConsistencyLevelEnum getConsistencyLevel() {
        return consistencyLevel;
    }

    public String getGroupByFieldName() {
        return groupByFieldName;
    }

    public Integer getGroupSize() {
        return groupSize;
    }

    public Boolean getStrictGroupSize() {
        return strictGroupSize;
    }

    // Setter method to replace @Setter annotation
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    // toString method to replace @ToString annotation
    @Override
    public String toString() {
        return "HybridSearchParam{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionNames=" + partitionNames +
                ", searchRequests=" + searchRequests +
                ", ranker=" + ranker +
                ", topK=" + topK +
                ", outFields=" + outFields +
                ", offset=" + offset +
                ", roundDecimal=" + roundDecimal +
                ", consistencyLevel=" + consistencyLevel +
                ", groupByFieldName='" + groupByFieldName + '\'' +
                ", groupSize=" + groupSize +
                ", strictGroupSize=" + strictGroupSize +
                '}';
    }

    /**
     * Builder for {@link SearchParam} class.
     */
    public static class Builder {
        private String databaseName;
        private String collectionName;
        private final List<String> partitionNames = Lists.newArrayList();
        private final List<AnnSearchParam> searchRequests = Lists.newArrayList();
        private BaseRanker ranker = null;
        private Long topK;
        private final List<String> outFields = Lists.newArrayList();
        private Long offset = 0L;
        private Integer roundDecimal = -1;
        private ConsistencyLevelEnum consistencyLevel = null;
        private String groupByFieldName = null;
        private Integer groupSize = null;
        private Boolean strictGroupSize = null;

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
         * Adds a vector search request for a vector field.
         *
         * @param searchParam vector search request
         * @return <code>Builder</code>
         */
        public Builder addSearchRequest(AnnSearchParam searchParam) {
            // Replace @NonNull logic with explicit null check
            if (searchParam == null) {
                throw new IllegalArgumentException("searchParam cannot be null");
            }
            this.searchRequests.add(searchParam);
            return this;
        }

        /**
         * Set a ranker for rearranging number of limit results.
         *
         * @param ranker concrete ranker object
         * @return <code>Builder</code>
         */
        public Builder withRanker(BaseRanker ranker) {
            // Replace @NonNull logic with explicit null check
            if (ranker == null) {
                throw new IllegalArgumentException("ranker cannot be null");
            }
            this.ranker = ranker;
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
         * Specifies the offset place of the returned results.
         *
         * @param offset the offset position
         * @return <code>Builder</code>
         */
        public Builder withOffset(Long offset) {
            // Replace @NonNull logic with explicit null check
            if (offset == null) {
                throw new IllegalArgumentException("offset cannot be null");
            }
            this.offset = offset;
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
         * Groups the results by a scalar field name.
         *
         * @param groupByFieldName a scalar field name
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
         * Defines the max number of items for each group, the value must greater than zero.
         *
         * @param groupSize the max number of items
         * @return <code>Builder</code>
         */
        public Builder withGroupSize(Integer groupSize) {
            // Replace @NonNull logic with explicit null check
            if (groupSize == null) {
                throw new IllegalArgumentException("groupSize cannot be null");
            }
            this.groupSize = groupSize;
            return this;
        }

        /**
         * Whether to force the number of each group to be groupSize.
         * Set to false, milvus might return some groups with number of items less than groupSize.
         *
         * @param strictGroupSize whether to force the number of each group to be groupSize
         * @return <code>Builder</code>
         */
        public Builder withStrictGroupSize(Boolean strictGroupSize) {
            // Replace @NonNull logic with explicit null check
            if (strictGroupSize == null) {
                throw new IllegalArgumentException("strictGroupSize cannot be null");
            }
            this.strictGroupSize = strictGroupSize;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link HybridSearchParam} instance.
         *
         * @return {@link HybridSearchParam}
         */
        public HybridSearchParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (ranker == null) {
                throw new ParamException("Must specify a Ranker by withRanker()");
            }

            if (searchRequests.isEmpty()) {
                throw new ParamException("At least a search request is required");
            }

            int vectorSize = 0;
            for (AnnSearchParam req : searchRequests) {
                if (vectorSize == 0) {
                    vectorSize = req.getVectors().size();
                } else if (vectorSize != req.getVectors().size()) {
                    throw new ParamException("Vector number of each AnnSearchParam must be equal");
                }
            }

            if (topK <= 0) {
                throw new ParamException("TopK value is illegal");
            }

            if (groupByFieldName != null && groupSize != null && groupSize <= 0) {
                throw new ParamException("GroupSize value cannot be zero or negative");
            }

            return new HybridSearchParam(this);
        }
    }

}
