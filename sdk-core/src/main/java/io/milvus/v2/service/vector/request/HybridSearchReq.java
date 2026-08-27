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

package io.milvus.v2.service.vector.request;

import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.CreateCollectionReq;

import java.util.List;

/**
 * Request parameters for the {@code hybridSearch} API.
 */
public class HybridSearchReq {
    private String databaseName;
    private String collectionName;
    /**
     * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
     * {@code MilvusClientV2Session}.
     */
    @Deprecated
    private String clusterId;
    private List<String> partitionNames;
    private List<AnnSearchReq> searchRequests;
    @Deprecated
    private int topK; // deprecated, replaced by "limit"
    private long limit;
    private List<String> outFields;
    private long offset;
    private int roundDecimal;
    private ConsistencyLevel consistencyLevel;
    private String groupByFieldName;
    private Integer groupSize;
    private Boolean strictGroupSize;
    @Deprecated
    private CreateCollectionReq.Function ranker;
    // milvus v2.6.1 supports multi-rankers. The "ranker" still works. It is recommended
    // to use functionScore even you have only one ranker. Not allow to set both.
    private FunctionScore functionScore;

    private HybridSearchReq(HybridSearchReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.clusterId = builder.clusterId;
        this.partitionNames = builder.partitionNames;
        this.searchRequests = builder.searchRequests;
        this.ranker = builder.ranker;
        this.functionScore = builder.functionScore;
        this.topK = builder.topK;
        this.limit = builder.limit;
        this.outFields = builder.outFields;
        this.offset = builder.offset;
        this.roundDecimal = builder.roundDecimal;
        this.consistencyLevel = builder.consistencyLevel;
        this.groupByFieldName = builder.groupByFieldName;
        this.groupSize = builder.groupSize;
        this.strictGroupSize = builder.strictGroupSize;
    }

    // Getters and Setters
    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
     * {@code MilvusClientV2Session}.
     */
    @Deprecated
    public String getClusterId() {
        return clusterId;
    }

    /**
     * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
     * {@code MilvusClientV2Session}.
     */
    @Deprecated
    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    /**
     * Returns the partition names to search in.
     *
     * @return the partition names
     */
    public List<String> getPartitionNames() {
        return partitionNames;
    }

    /**
     * Sets the partition names to search in.
     *
     * @param partitionNames the partition names
     */
    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    /**
     * Returns the ANN search requests to be combined in the hybrid search.
     *
     * @return the ANN search requests
     */
    public List<AnnSearchReq> getSearchRequests() {
        return searchRequests;
    }

    /**
     * Sets the ANN search requests to be combined in the hybrid search.
     *
     * @param searchRequests the ANN search requests
     */
    public void setSearchRequests(List<AnnSearchReq> searchRequests) {
        this.searchRequests = searchRequests;
    }

    /**
     * Returns the ranker used to merge the results of the ANN search requests.
     *
     * @return the ranker
     */
    public CreateCollectionReq.Function getRanker() {
        return ranker;
    }

    /**
     * Sets the ranker used to merge the results of the ANN search requests.
     *
     * @param ranker the ranker
     */
    public void setRanker(CreateCollectionReq.Function ranker) {
        this.ranker = ranker;
    }

    /**
     * Returns the function score used for multi-rankers.
     *
     * @return the function score
     */
    public FunctionScore getFunctionScore() {
        return functionScore;
    }

    /**
     * Sets the function score used for multi-rankers.
     *
     * @param functionScore the function score
     */
    public void setFunctionScore(FunctionScore functionScore) {
        this.functionScore = functionScore;
    }

    /**
     * Returns the maximum number of results to return.
     *
     * @deprecated replaced by {@link #getLimit()}
     * @return the topK value
     */
    @Deprecated
    public int getTopK() {
        return topK;
    }

    /**
     * Sets the maximum number of results to return.
     *
     * @deprecated replaced by {@link #setLimit(long)}
     * @param topK the topK value
     */
    @Deprecated
    public void setTopK(int topK) {
        this.topK = topK;
        this.limit = topK;
    }

    /**
     * Returns the maximum number of results to return.
     *
     * @return the limit value
     */
    public long getLimit() {
        return limit;
    }

    /**
     * Sets the maximum number of results to return.
     *
     * @param limit the limit value
     */
    public void setLimit(long limit) {
        this.limit = limit;
        this.topK = (int) limit;
    }

    /**
     * Returns the fields to return for each search result.
     *
     * @return the output fields
     */
    public List<String> getOutFields() {
        return outFields;
    }

    /**
     * Sets the fields to return for each search result.
     *
     * @param outFields the output fields
     */
    public void setOutFields(List<String> outFields) {
        this.outFields = outFields;
    }

    /**
     * Returns the offset of the first result to return.
     *
     * @return the offset
     */
    public long getOffset() {
        return offset;
    }

    /**
     * Sets the offset of the first result to return.
     *
     * @param offset the offset
     */
    public void setOffset(long offset) {
        this.offset = offset;
    }

    /**
     * Returns the number of decimal places to round the scores to.
     *
     * @return the round decimal value
     */
    public int getRoundDecimal() {
        return roundDecimal;
    }

    /**
     * Sets the number of decimal places to round the scores to.
     *
     * @param roundDecimal the round decimal value
     */
    public void setRoundDecimal(int roundDecimal) {
        this.roundDecimal = roundDecimal;
    }

    /**
     * Returns the consistency level for the search.
     *
     * @return the consistency level
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    /**
     * Sets the consistency level for the search.
     *
     * @param consistencyLevel the consistency level
     */
    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    /**
     * Returns the field name used to group the search results.
     *
     * @return the group-by field name
     */
    public String getGroupByFieldName() {
        return groupByFieldName;
    }

    /**
     * Sets the field name used to group the search results.
     *
     * @param groupByFieldName the group-by field name
     */
    public void setGroupByFieldName(String groupByFieldName) {
        this.groupByFieldName = groupByFieldName;
    }

    /**
     * Returns the maximum number of hits per group.
     *
     * @return the group size
     */
    public Integer getGroupSize() {
        return groupSize;
    }

    /**
     * Sets the maximum number of hits per group.
     *
     * @param groupSize the group size
     */
    public void setGroupSize(Integer groupSize) {
        this.groupSize = groupSize;
    }

    /**
     * Returns whether the group size is enforced strictly.
     *
     * @return {@code true} if the group size is strict
     */
    public Boolean getStrictGroupSize() {
        return strictGroupSize;
    }

    /**
     * Sets whether the group size is enforced strictly.
     *
     * @param strictGroupSize {@code true} if the group size is strict
     */
    public void setStrictGroupSize(Boolean strictGroupSize) {
        this.strictGroupSize = strictGroupSize;
    }

    @Override
    public String toString() {
        return "HybridSearchReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", partitionNames=" + partitionNames +
                ", searchRequests=" + searchRequests +
                ", ranker=" + ranker +
                ", functionScore=" + functionScore +
                ", topK=" + topK +
                ", limit=" + limit +
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
     * Creates a new {@code HybridSearchReq} builder.
     *
     * @return the builder
     */
    public static HybridSearchReqBuilder builder() {
        return new HybridSearchReqBuilder();
    }

    public static class HybridSearchReqBuilder {
        private String databaseName;
        private String collectionName;
        private String clusterId;
        private List<String> partitionNames;
        private List<AnnSearchReq> searchRequests;
        private CreateCollectionReq.Function ranker;
        private FunctionScore functionScore;
        private int topK = 0; // default value
        private long limit = 0L; // default value
        private List<String> outFields;
        private long offset;
        private int roundDecimal = -1; // default value
        private ConsistencyLevel consistencyLevel = null; // default value
        private String groupByFieldName;
        private Integer groupSize;
        private Boolean strictGroupSize;

        private HybridSearchReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public HybridSearchReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public HybridSearchReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
         * {@code MilvusClientV2Session}.
         */
        @Deprecated
        public HybridSearchReqBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        /**
         * Sets the partition names to search in.
         *
         * @param partitionNames the partition names
         * @return this builder
         */
        public HybridSearchReqBuilder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        /**
         * Sets the ANN search requests to be combined in the hybrid search.
         *
         * @param searchRequests the ANN search requests
         * @return this builder
         */
        public HybridSearchReqBuilder searchRequests(List<AnnSearchReq> searchRequests) {
            this.searchRequests = searchRequests;
            return this;
        }

        /**
         * Sets the ranker used to merge the results of the ANN search requests.
         *
         * @param ranker the ranker
         * @return this builder
         */
        public HybridSearchReqBuilder ranker(CreateCollectionReq.Function ranker) {
            this.ranker = ranker;
            return this;
        }

        /**
         * Sets the function score used for multi-rankers.
         *
         * @param functionScore the function score
         * @return this builder
         */
        public HybridSearchReqBuilder functionScore(FunctionScore functionScore) {
            this.functionScore = functionScore;
            return this;
        }

        // topK is deprecated, topK and limit must be the same value
        @Deprecated
        public HybridSearchReqBuilder topK(int topK) {
            this.topK = topK;
            this.limit = topK;
            return this;
        }

        /**
         * Sets the maximum number of results to return.
         *
         * @param limit the limit value
         * @return this builder
         */
        public HybridSearchReqBuilder limit(long limit) {
            this.topK = (int) limit;
            this.limit = limit;
            return this;
        }

        /**
         * Sets the fields to return for each search result.
         *
         * @param outFields the output fields
         * @return this builder
         */
        public HybridSearchReqBuilder outFields(List<String> outFields) {
            this.outFields = outFields;
            return this;
        }

        /**
         * Sets the offset of the first result to return.
         *
         * @param offset the offset
         * @return this builder
         */
        public HybridSearchReqBuilder offset(long offset) {
            this.offset = offset;
            return this;
        }

        /**
         * Sets the number of decimal places to round the scores to.
         *
         * @param roundDecimal the round decimal value
         * @return this builder
         */
        public HybridSearchReqBuilder roundDecimal(int roundDecimal) {
            this.roundDecimal = roundDecimal;
            return this;
        }

        /**
         * Sets the consistency level for the search.
         *
         * @param consistencyLevel the consistency level
         * @return this builder
         */
        public HybridSearchReqBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        /**
         * Sets the field name used to group the search results.
         *
         * @param groupByFieldName the group-by field name
         * @return this builder
         */
        public HybridSearchReqBuilder groupByFieldName(String groupByFieldName) {
            this.groupByFieldName = groupByFieldName;
            return this;
        }

        /**
         * Sets the maximum number of hits per group.
         *
         * @param groupSize the group size
         * @return this builder
         */
        public HybridSearchReqBuilder groupSize(Integer groupSize) {
            this.groupSize = groupSize;
            return this;
        }

        /**
         * Sets whether the group size is enforced strictly.
         *
         * @param strictGroupSize {@code true} if the group size is strict
         * @return this builder
         */
        public HybridSearchReqBuilder strictGroupSize(Boolean strictGroupSize) {
            this.strictGroupSize = strictGroupSize;
            return this;
        }

        /**
         * Builds the {@link HybridSearchReq}.
         *
         * @return the request
         */
        public HybridSearchReq build() {
            return new HybridSearchReq(this);
        }
    }
}
