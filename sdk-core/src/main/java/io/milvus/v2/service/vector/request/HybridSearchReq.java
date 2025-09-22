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
import org.apache.commons.lang3.builder.EqualsBuilder;

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

    private HybridSearchReq(Builder builder) {
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
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
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
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        HybridSearchReq that = (HybridSearchReq) obj;
        return new EqualsBuilder()
                .append(topK, that.topK)
                .append(limit, that.limit)
                .append(offset, that.offset)
                .append(roundDecimal, that.roundDecimal)
                .append(databaseName, that.databaseName)
                .append(collectionName, that.collectionName)
                .append(partitionNames, that.partitionNames)
                .append(searchRequests, that.searchRequests)
                .append(ranker, that.ranker)
                .append(functionScore, that.functionScore)
                .append(outFields, that.outFields)
                .append(consistencyLevel, that.consistencyLevel)
                .append(groupByFieldName, that.groupByFieldName)
                .append(groupSize, that.groupSize)
                .append(strictGroupSize, that.strictGroupSize)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = databaseName != null ? databaseName.hashCode() : 0;
        result = 31 * result + (collectionName != null ? collectionName.hashCode() : 0);
        result = 31 * result + (partitionNames != null ? partitionNames.hashCode() : 0);
        result = 31 * result + (searchRequests != null ? searchRequests.hashCode() : 0);
        result = 31 * result + (ranker != null ? ranker.hashCode() : 0);
        result = 31 * result + (functionScore != null ? functionScore.hashCode() : 0);
        result = 31 * result + topK;
        result = 31 * result + (int) (limit ^ (limit >>> 32));
        result = 31 * result + (outFields != null ? outFields.hashCode() : 0);
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + roundDecimal;
        result = 31 * result + (consistencyLevel != null ? consistencyLevel.hashCode() : 0);
        result = 31 * result + (groupByFieldName != null ? groupByFieldName.hashCode() : 0);
        result = 31 * result + (groupSize != null ? groupSize.hashCode() : 0);
        result = 31 * result + (strictGroupSize != null ? strictGroupSize.hashCode() : 0);
        return result;
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
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

        private Builder() {}

        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public Builder searchRequests(List<AnnSearchReq> searchRequests) {
            this.searchRequests = searchRequests;
            return this;
        }

        public Builder ranker(CreateCollectionReq.Function ranker) {
            this.ranker = ranker;
            return this;
        }

        public Builder functionScore(FunctionScore functionScore) {
            this.functionScore = functionScore;
            return this;
        }

        // topK is deprecated, topK and limit must be the same value
        @Deprecated
        public Builder topK(int topK) {
            this.topK = topK;
            this.limit = topK;
            return this;
        }

        public Builder limit(long limit) {
            this.topK = (int) limit;
            this.limit = limit;
            return this;
        }

        public Builder outFields(List<String> outFields) {
            this.outFields = outFields;
            return this;
        }

        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder roundDecimal(int roundDecimal) {
            this.roundDecimal = roundDecimal;
            return this;
        }

        public Builder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        public Builder groupByFieldName(String groupByFieldName) {
            this.groupByFieldName = groupByFieldName;
            return this;
        }

        public Builder groupSize(Integer groupSize) {
            this.groupSize = groupSize;
            return this;
        }

        public Builder strictGroupSize(Boolean strictGroupSize) {
            this.strictGroupSize = strictGroupSize;
            return this;
        }

        public HybridSearchReq build() {
            return new HybridSearchReq(this);
        }
    }
}
