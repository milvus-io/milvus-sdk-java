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

public class HybridSearchReq {
    private String databaseName;
    private String collectionName;
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
    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    public List<AnnSearchReq> getSearchRequests() {
        return searchRequests;
    }

    public void setSearchRequests(List<AnnSearchReq> searchRequests) {
        this.searchRequests = searchRequests;
    }

    public CreateCollectionReq.Function getRanker() {
        return ranker;
    }

    public void setRanker(CreateCollectionReq.Function ranker) {
        this.ranker = ranker;
    }

    public FunctionScore getFunctionScore() {
        return functionScore;
    }

    public void setFunctionScore(FunctionScore functionScore) {
        this.functionScore = functionScore;
    }

    @Deprecated
    public int getTopK() {
        return topK;
    }

    @Deprecated
    public void setTopK(int topK) {
        this.topK = topK;
        this.limit = topK;
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
        this.topK = (int) limit;
    }

    public List<String> getOutFields() {
        return outFields;
    }

    public void setOutFields(List<String> outFields) {
        this.outFields = outFields;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getRoundDecimal() {
        return roundDecimal;
    }

    public void setRoundDecimal(int roundDecimal) {
        this.roundDecimal = roundDecimal;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    public String getGroupByFieldName() {
        return groupByFieldName;
    }

    public void setGroupByFieldName(String groupByFieldName) {
        this.groupByFieldName = groupByFieldName;
    }

    public Integer getGroupSize() {
        return groupSize;
    }

    public void setGroupSize(Integer groupSize) {
        this.groupSize = groupSize;
    }

    public Boolean getStrictGroupSize() {
        return strictGroupSize;
    }

    public void setStrictGroupSize(Boolean strictGroupSize) {
        this.strictGroupSize = strictGroupSize;
    }

    @Override
    public String toString() {
        return "HybridSearchReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
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

    public static HybridSearchReqBuilder builder() {
        return new HybridSearchReqBuilder();
    }

    public static class HybridSearchReqBuilder {
        private String databaseName;
        private String collectionName;
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

        public HybridSearchReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public HybridSearchReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public HybridSearchReqBuilder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public HybridSearchReqBuilder searchRequests(List<AnnSearchReq> searchRequests) {
            this.searchRequests = searchRequests;
            return this;
        }

        public HybridSearchReqBuilder ranker(CreateCollectionReq.Function ranker) {
            this.ranker = ranker;
            return this;
        }

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

        public HybridSearchReqBuilder limit(long limit) {
            this.topK = (int) limit;
            this.limit = limit;
            return this;
        }

        public HybridSearchReqBuilder outFields(List<String> outFields) {
            this.outFields = outFields;
            return this;
        }

        public HybridSearchReqBuilder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public HybridSearchReqBuilder roundDecimal(int roundDecimal) {
            this.roundDecimal = roundDecimal;
            return this;
        }

        public HybridSearchReqBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        public HybridSearchReqBuilder groupByFieldName(String groupByFieldName) {
            this.groupByFieldName = groupByFieldName;
            return this;
        }

        public HybridSearchReqBuilder groupSize(Integer groupSize) {
            this.groupSize = groupSize;
            return this;
        }

        public HybridSearchReqBuilder strictGroupSize(Boolean strictGroupSize) {
            this.strictGroupSize = strictGroupSize;
            return this;
        }

        public HybridSearchReq build() {
            return new HybridSearchReq(this);
        }
    }
}
