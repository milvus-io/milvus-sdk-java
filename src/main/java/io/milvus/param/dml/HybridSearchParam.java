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
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.List;

/**
 * Parameters for <code>search</code> interface.
 */
@Getter
@ToString
public class HybridSearchParam {
    private final String databaseName;
    private final String collectionName;
    private final List<String> partitionNames;
    private final List<AnnSearchParam> searchRequests;
    private final BaseRanker ranker;
    private final int topK;
    private final List<String> outFields;
    private final int roundDecimal;
    private final ConsistencyLevelEnum consistencyLevel;

    private final String groupByFieldName;

    private HybridSearchParam(@NonNull Builder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.searchRequests = builder.searchRequests;
        this.ranker = builder.ranker;
        this.topK = builder.topK;
        this.outFields = builder.outFields;
        this.roundDecimal = builder.roundDecimal;
        this.consistencyLevel = builder.consistencyLevel;
        this.groupByFieldName = builder.groupByFieldName;
    }

    public static Builder newBuilder() {
        return new Builder();
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
        private Integer topK;
        private final List<String> outFields = Lists.newArrayList();
        private Integer roundDecimal = -1;
        private ConsistencyLevelEnum consistencyLevel = null;
        private String groupByFieldName = null;

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
         * Adds a vector search request for a vector field.
         *
         * @param searchParam vector search request
         * @return <code>Builder</code>
         */
        public Builder addSearchRequest(@NonNull AnnSearchParam searchParam) {
            this.searchRequests.add(searchParam);
            return this;
        }

        /**
         * Set a ranker for rearranging number of limit results.
         *
         * @param ranker concrete ranker object
         * @return <code>Builder</code>
         */
        public Builder withRanker(@NonNull BaseRanker ranker) {
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
         *
         * @param topK topK value
         * @return <code>Builder</code>
         */
        public Builder withTopK(@NonNull Integer topK) {
            this.topK = topK;
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
         * Groups the results by a scalar field name.
         *
         * @param fieldName a scalar field name
         * @return <code>Builder</code>
         */
        public Builder withGroupByFieldName(@NonNull String groupByFieldName) {
            this.groupByFieldName = groupByFieldName;
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

            return new HybridSearchParam(this);
        }
    }

}
