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
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.data.BaseVector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SearchReq {
    private String databaseName;
    private String collectionName;
    private List<String> partitionNames;
    private String annsField;
    private IndexParam.MetricType metricType;
    @Deprecated
    private int topK;
    private String filter;
    private List<String> outputFields;
    private List<BaseVector> data;
    private long offset;
    private long limit;
    private int roundDecimal;
    private Map<String, Object> searchParams;
    private long guaranteeTimestamp; // deprecated
    private Long gracefulTime; // deprecated
    private ConsistencyLevel consistencyLevel;
    private boolean ignoreGrowing;
    private String timezone;
    private String groupByFieldName;
    private Integer groupSize;
    private Boolean strictGroupSize;
    @Deprecated
    private CreateCollectionReq.Function ranker;
    // milvus v2.6.1 supports multi-rankers. The "ranker" still works. It is recommended
    // to use functionScore even you have only one ranker. Not allow to set both.
    private FunctionScore functionScore;

    // Expression template, to improve expression parsing performance in complicated list
    // Assume user has a filter = "pk > 3 and city in ["beijing", "shanghai", ......]
    // The long list of city will increase the time cost to parse this expression.
    // So, we provide exprTemplateValues for this purpose, user can set filter like this:
    //     filter = "pk > {age} and city in {city}"
    //     filterTemplateValues = Map{"age": 3, "city": List<String>{"beijing", "shanghai", ......}}
    // Valid value of this map can be:
    //     Boolean, Long, Double, String, List<Boolean>, List<Long>, List<Double>, List<String>
    private Map<String, Object> filterTemplateValues;

    private SearchReq(SearchReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.annsField = builder.annsField;
        this.metricType = builder.metricType;
        this.topK = builder.topK;
        this.filter = builder.filter;
        this.outputFields = builder.outputFields;
        this.data = builder.data;
        this.offset = builder.offset;
        this.limit = builder.limit;
        this.roundDecimal = builder.roundDecimal;
        this.searchParams = builder.searchParams;
        this.guaranteeTimestamp = builder.guaranteeTimestamp;
        this.gracefulTime = builder.gracefulTime;
        this.consistencyLevel = builder.consistencyLevel;
        this.ignoreGrowing = builder.ignoreGrowing;
        this.groupByFieldName = builder.groupByFieldName;
        this.groupSize = builder.groupSize;
        this.strictGroupSize = builder.strictGroupSize;
        this.ranker = builder.ranker;
        this.functionScore = builder.functionScore;
        this.filterTemplateValues = builder.filterTemplateValues;
        this.timezone = builder.timezone;
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

    public String getAnnsField() {
        return annsField;
    }

    public void setAnnsField(String annsField) {
        this.annsField = annsField;
    }

    public IndexParam.MetricType getMetricType() {
        return metricType;
    }

    public void setMetricType(IndexParam.MetricType metricType) {
        this.metricType = metricType;
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

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public List<String> getOutputFields() {
        return outputFields;
    }

    public void setOutputFields(List<String> outputFields) {
        this.outputFields = outputFields;
    }

    public List<BaseVector> getData() {
        return data;
    }

    public void setData(List<BaseVector> data) {
        this.data = data;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
        this.topK = (int) limit;
    }

    public int getRoundDecimal() {
        return roundDecimal;
    }

    public void setRoundDecimal(int roundDecimal) {
        this.roundDecimal = roundDecimal;
    }

    public Map<String, Object> getSearchParams() {
        return searchParams;
    }

    public void setSearchParams(Map<String, Object> searchParams) {
        this.searchParams = searchParams;
    }

    public long getGuaranteeTimestamp() {
        return guaranteeTimestamp;
    }

    public void setGuaranteeTimestamp(long guaranteeTimestamp) {
        this.guaranteeTimestamp = guaranteeTimestamp;
    }

    public Long getGracefulTime() {
        return gracefulTime;
    }

    public void setGracefulTime(Long gracefulTime) {
        this.gracefulTime = gracefulTime;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    public boolean isIgnoreGrowing() {
        return ignoreGrowing;
    }

    public void setIgnoreGrowing(boolean ignoreGrowing) {
        this.ignoreGrowing = ignoreGrowing;
    }

    public String getTimezone() {
        return timezone;
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

    public Map<String, Object> getFilterTemplateValues() {
        return filterTemplateValues;
    }

    public void setFilterTemplateValues(Map<String, Object> filterTemplateValues) {
        this.filterTemplateValues = filterTemplateValues;
    }

    @Override
    public String toString() {
        return "SearchReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionNames=" + partitionNames +
                ", annsField='" + annsField + '\'' +
                ", metricType=" + metricType +
                ", topK=" + topK +
                ", filter='" + filter + '\'' +
                ", outputFields=" + outputFields +
                ", data=" + data +
                ", offset=" + offset +
                ", limit=" + limit +
                ", roundDecimal=" + roundDecimal +
                ", searchParams=" + searchParams +
                ", guaranteeTimestamp=" + guaranteeTimestamp +
                ", gracefulTime=" + gracefulTime +
                ", consistencyLevel=" + consistencyLevel +
                ", ignoreGrowing=" + ignoreGrowing +
                ", timezone='" + timezone + '\'' +
                ", groupByFieldName='" + groupByFieldName + '\'' +
                ", groupSize=" + groupSize +
                ", strictGroupSize=" + strictGroupSize +
                ", ranker=" + ranker +
                ", functionScore=" + functionScore +
//                ", filterTemplateValues=" + filterTemplateValues +
                '}';
    }

    public static SearchReqBuilder builder() {
        return new SearchReqBuilder();
    }

    public static class SearchReqBuilder {
        private String databaseName;
        private String collectionName;
        private List<String> partitionNames = new ArrayList<>(); // default value
        private String annsField = ""; // default value
        private IndexParam.MetricType metricType;
        private int topK = 0; // default value
        private String filter;
        private List<String> outputFields = new ArrayList<>(); // default value
        private List<BaseVector> data = new ArrayList<>(); // default value
        private long offset;
        private long limit = 0L; // default value
        private int roundDecimal = -1; // default value
        private Map<String, Object> searchParams = new HashMap<>(); // default value
        private long guaranteeTimestamp; // deprecated
        private Long gracefulTime = 5000L; // default value, deprecated
        private ConsistencyLevel consistencyLevel = null; // default value
        private boolean ignoreGrowing;
        private String timezone = "";
        private String groupByFieldName;
        private Integer groupSize;
        private Boolean strictGroupSize;
        private CreateCollectionReq.Function ranker;
        private FunctionScore functionScore;
        private Map<String, Object> filterTemplateValues = new HashMap<>(); // default value

        private SearchReqBuilder() {
        }

        public SearchReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public SearchReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public SearchReqBuilder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public SearchReqBuilder annsField(String annsField) {
            this.annsField = annsField;
            return this;
        }

        public SearchReqBuilder metricType(IndexParam.MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        // topK is deprecated, topK and limit must be the same value
        @Deprecated
        public SearchReqBuilder topK(int topK) {
            this.topK = topK;
            this.limit = topK;
            return this;
        }

        public SearchReqBuilder filter(String filter) {
            this.filter = filter;
            return this;
        }

        public SearchReqBuilder outputFields(List<String> outputFields) {
            this.outputFields = outputFields;
            return this;
        }

        public SearchReqBuilder data(List<BaseVector> data) {
            this.data = data;
            return this;
        }

        public SearchReqBuilder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public SearchReqBuilder limit(long limit) {
            this.topK = (int) limit;
            this.limit = limit;
            return this;
        }

        public SearchReqBuilder roundDecimal(int roundDecimal) {
            this.roundDecimal = roundDecimal;
            return this;
        }

        public SearchReqBuilder searchParams(Map<String, Object> searchParams) {
            this.searchParams = searchParams;
            return this;
        }

        public SearchReqBuilder guaranteeTimestamp(long guaranteeTimestamp) {
            this.guaranteeTimestamp = guaranteeTimestamp;
            return this;
        }

        public SearchReqBuilder gracefulTime(Long gracefulTime) {
            this.gracefulTime = gracefulTime;
            return this;
        }

        public SearchReqBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        public SearchReqBuilder ignoreGrowing(boolean ignoreGrowing) {
            this.ignoreGrowing = ignoreGrowing;
            return this;
        }

        public SearchReqBuilder timezone(String timezone) {
            this.timezone = timezone;
            return this;
        }

        public SearchReqBuilder groupByFieldName(String groupByFieldName) {
            this.groupByFieldName = groupByFieldName;
            return this;
        }

        public SearchReqBuilder groupSize(Integer groupSize) {
            this.groupSize = groupSize;
            return this;
        }

        public SearchReqBuilder strictGroupSize(Boolean strictGroupSize) {
            this.strictGroupSize = strictGroupSize;
            return this;
        }

        public SearchReqBuilder ranker(CreateCollectionReq.Function ranker) {
            this.ranker = ranker;
            return this;
        }

        public SearchReqBuilder functionScore(FunctionScore functionScore) {
            this.functionScore = functionScore;
            return this;
        }

        public SearchReqBuilder filterTemplateValues(Map<String, Object> filterTemplateValues) {
            this.filterTemplateValues = filterTemplateValues;
            return this;
        }

        public SearchReq build() {
            return new SearchReq(this);
        }
    }
}
