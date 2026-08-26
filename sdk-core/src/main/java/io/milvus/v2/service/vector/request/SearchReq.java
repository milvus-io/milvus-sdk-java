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
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.aggregation.OrderByField;
import io.milvus.v2.service.vector.request.aggregation.SearchAggregation;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.highlighter.Highlighter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Request parameters for the {@code search} API.
 */
public class SearchReq {
    private String databaseName;
    private String collectionName;
    /**
     * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
     * {@code MilvusClientV2Session}.
     */
    @Deprecated
    private String clusterId;
    private List<String> partitionNames;
    private String annsField;
    private IndexParam.MetricType metricType;
    @Deprecated
    private int topK;
    private String filter;
    private List<String> outputFields;
    private List<BaseVector> data;
    private List<Object> ids;
    private long offset;
    private long limit;
    private int roundDecimal;
    private Map<String, Object> searchParams;
    private long guaranteeTimestamp; // deprecated
    private Long gracefulTime; // deprecated
    private ConsistencyLevel consistencyLevel;
    private boolean ignoreGrowing;
    private String timezone;
    private List<OrderByField> orderByFields;
    private String groupByFieldName;
    private Integer groupSize;
    private Boolean strictGroupSize;
    @Deprecated
    private CreateCollectionReq.Function ranker;
    // milvus v2.6.1 supports multi-rankers. The "ranker" still works. It is recommended
    // to use functionScore even you have only one ranker. Not allow to set both.
    private FunctionScore functionScore;
    // function chains applied to ordinary search. Mutually exclusive with ranker/functionScore.
    private List<FunctionChain> functionChains;

    // Expression template, to improve expression parsing performance in complicated list
    // Assume user has a filter = "pk > 3 and city in ["beijing", "shanghai", ......]
    // The long list of city will increase the time cost to parse this expression.
    // So, we provide exprTemplateValues for this purpose, user can set filter like this:
    //     filter = "pk > {age} and city in {city}"
    //     filterTemplateValues = Map{"age": 3, "city": List<String>{"beijing", "shanghai", ......}}
    // Valid value of this map can be:
    //     Boolean, Long, Double, String, List<Boolean>, List<Long>, List<Double>, List<String>
    private Map<String, Object> filterTemplateValues;

    // milvus v2.6.9 supports highlighter for search results
    private Highlighter highlighter;

    private SearchAggregation searchAggregation;

    private SearchReq(SearchReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.clusterId = builder.clusterId;
        this.partitionNames = builder.partitionNames;
        this.annsField = builder.annsField;
        this.metricType = builder.metricType;
        this.topK = builder.topK;
        this.filter = builder.filter;
        this.outputFields = builder.outputFields;
        this.data = builder.data;
        this.ids = builder.ids;
        this.offset = builder.offset;
        this.limit = builder.limit;
        this.roundDecimal = builder.roundDecimal;
        this.searchParams = builder.searchParams;
        this.guaranteeTimestamp = builder.guaranteeTimestamp;
        this.gracefulTime = builder.gracefulTime;
        this.consistencyLevel = builder.consistencyLevel;
        this.ignoreGrowing = builder.ignoreGrowing;
        this.orderByFields = builder.orderByFields;
        this.groupByFieldName = builder.groupByFieldName;
        this.groupSize = builder.groupSize;
        this.strictGroupSize = builder.strictGroupSize;
        this.ranker = builder.ranker;
        this.functionScore = builder.functionScore;
        this.functionChains = builder.functionChains;
        this.filterTemplateValues = builder.filterTemplateValues;
        this.timezone = builder.timezone;
        this.highlighter = builder.highlighter;
        this.searchAggregation = builder.searchAggregation;
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
     * Returns the name of the vector field to search.
     *
     * @return the ANN search field name
     */
    public String getAnnsField() {
        return annsField;
    }

    /**
     * Sets the name of the vector field to search.
     *
     * @param annsField the ANN search field name
     */
    public void setAnnsField(String annsField) {
        this.annsField = annsField;
    }

    /**
     * Returns the metric type used for the search.
     *
     * @return the metric type
     */
    public IndexParam.MetricType getMetricType() {
        return metricType;
    }

    /**
     * Sets the metric type used for the search.
     *
     * @param metricType the metric type
     */
    public void setMetricType(IndexParam.MetricType metricType) {
        this.metricType = metricType;
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
     * Returns the filter expression.
     *
     * @return the filter expression
     */
    public String getFilter() {
        return filter;
    }

    /**
     * Sets the filter expression.
     *
     * @param filter the filter expression
     */
    public void setFilter(String filter) {
        this.filter = filter;
    }

    /**
     * Returns the fields to return for each search result.
     *
     * @return the output fields
     */
    public List<String> getOutputFields() {
        return outputFields;
    }

    /**
     * Sets the fields to return for each search result.
     *
     * @param outputFields the output fields
     */
    public void setOutputFields(List<String> outputFields) {
        this.outputFields = outputFields;
    }

    /**
     * Returns the query vectors.
     *
     * @return the query vectors
     */
    public List<BaseVector> getData() {
        return data;
    }

    /**
     * Sets the query vectors.
     *
     * @param data the query vectors
     */
    public void setData(List<BaseVector> data) {
        this.data = data;
    }

    /**
     * Returns the primary key values of the entities to search.
     *
     * @return the primary key values
     */
    public List<Object> getIds() {
        return ids;
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
     * Returns the search parameters.
     *
     * @return the search parameters
     */
    public Map<String, Object> getSearchParams() {
        return searchParams;
    }

    /**
     * Sets the search parameters.
     *
     * @param searchParams the search parameters
     */
    public void setSearchParams(Map<String, Object> searchParams) {
        this.searchParams = searchParams;
    }

    /**
     * Returns the guarantee timestamp.
     *
     * @deprecated no longer used
     * @return the guarantee timestamp
     */
    public long getGuaranteeTimestamp() {
        return guaranteeTimestamp;
    }

    /**
     * Sets the guarantee timestamp.
     *
     * @deprecated no longer used
     * @param guaranteeTimestamp the guarantee timestamp
     */
    public void setGuaranteeTimestamp(long guaranteeTimestamp) {
        this.guaranteeTimestamp = guaranteeTimestamp;
    }

    /**
     * Returns the graceful time in milliseconds.
     *
     * @deprecated no longer used
     * @return the graceful time
     */
    public Long getGracefulTime() {
        return gracefulTime;
    }

    /**
     * Sets the graceful time in milliseconds.
     *
     * @deprecated no longer used
     * @param gracefulTime the graceful time
     */
    public void setGracefulTime(Long gracefulTime) {
        this.gracefulTime = gracefulTime;
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
     * Returns whether growing segments are ignored.
     *
     * @return {@code true} if growing segments are ignored
     */
    public boolean isIgnoreGrowing() {
        return ignoreGrowing;
    }

    /**
     * Sets whether growing segments are ignored.
     *
     * @param ignoreGrowing {@code true} if growing segments are ignored
     */
    public void setIgnoreGrowing(boolean ignoreGrowing) {
        this.ignoreGrowing = ignoreGrowing;
    }

    /**
     * Returns the timezone used for timestamp fields in the filter expression.
     *
     * @return the timezone
     */
    public String getTimezone() {
        return timezone;
    }

    /**
     * Returns the order-by fields used to sort the search results.
     *
     * @return the order-by fields
     */
    public List<OrderByField> getOrderByFields() {
        return orderByFields;
    }

    /**
     * Sets the order-by fields used to sort the search results.
     *
     * @param orderByFields the order-by fields
     */
    public void setOrderByFields(List<OrderByField> orderByFields) {
        this.orderByFields = orderByFields;
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

    /**
     * Returns the ranker used to rerank the search results.
     *
     * @return the ranker
     */
    public CreateCollectionReq.Function getRanker() {
        return ranker;
    }

    /**
     * Sets the ranker used to rerank the search results.
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
     * Returns the function chains applied to an ordinary search.
     *
     * @return the function chains
     */
    public List<FunctionChain> getFunctionChains() {
        return functionChains;
    }

    /**
     * Sets the function chains applied to an ordinary search.
     *
     * @param functionChains the function chains
     */
    public void setFunctionChains(List<FunctionChain> functionChains) {
        this.functionChains = functionChains;
    }

    /**
     * Returns the expression template values used to improve expression parsing performance.
     *
     * @return the filter template values
     */
    public Map<String, Object> getFilterTemplateValues() {
        return filterTemplateValues;
    }

    /**
     * Sets the expression template values used to improve expression parsing performance.
     *
     * @param filterTemplateValues the filter template values
     */
    public void setFilterTemplateValues(Map<String, Object> filterTemplateValues) {
        this.filterTemplateValues = filterTemplateValues;
    }

    /**
     * Returns the highlighter used to highlight matching text in the search results.
     *
     * @return the highlighter
     */
    public Highlighter getHighlighter() {
        return highlighter;
    }

    /**
     * Returns the search aggregation.
     *
     * @return the search aggregation
     */
    public SearchAggregation getSearchAggregation() {
        return searchAggregation;
    }

    /**
     * Sets the search aggregation.
     *
     * @param searchAggregation the search aggregation
     */
    public void setSearchAggregation(SearchAggregation searchAggregation) {
        this.searchAggregation = searchAggregation;
    }

    @Override
    public String toString() {
        return "SearchReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", partitionNames=" + partitionNames +
                ", annsField='" + annsField + '\'' +
                ", metricType=" + metricType +
                ", topK=" + topK +
                ", filter='" + filter + '\'' +
                ", outputFields=" + outputFields +
                (ids == null || ids.isEmpty() ? ", data=" + data : ", ids=" + ids) +
                ", offset=" + offset +
                ", limit=" + limit +
                ", roundDecimal=" + roundDecimal +
                ", searchParams=" + searchParams +
                ", guaranteeTimestamp=" + guaranteeTimestamp +
                ", gracefulTime=" + gracefulTime +
                ", consistencyLevel=" + consistencyLevel +
                ", ignoreGrowing=" + ignoreGrowing +
                ", timezone='" + timezone + '\'' +
                ", orderByFields=" + orderByFields +
                ", groupByFieldName='" + groupByFieldName + '\'' +
                ", groupSize=" + groupSize +
                ", strictGroupSize=" + strictGroupSize +
                ", ranker=" + ranker +
                ", highlighter=" + (highlighter == null ? "null" : (highlighter.highlightType() + ":" + highlighter.getParams())) +
                ", searchAggregation=" + searchAggregation +
                ", functionScore=" + functionScore +
                ", functionChains=" + functionChains +
//                ", filterTemplateValues=" + filterTemplateValues +
                '}';
    }

    /**
     * Creates a new {@code SearchReq} builder.
     *
     * @return the builder
     */
    public static SearchReqBuilder builder() {
        return new SearchReqBuilder();
    }

    public static class SearchReqBuilder {
        private String databaseName;
        private String collectionName;
        private String clusterId;
        private List<String> partitionNames = new ArrayList<>(); // default value
        private String annsField = ""; // default value
        private IndexParam.MetricType metricType;
        private int topK = 0; // default value
        private String filter;
        private List<String> outputFields = new ArrayList<>(); // default value
        private List<BaseVector> data = new ArrayList<>(); // default value
        private List<Object> ids = new ArrayList<>();
        private long offset;
        private long limit = 0L; // default value
        private int roundDecimal = -1; // default value
        private Map<String, Object> searchParams = new HashMap<>(); // default value
        private long guaranteeTimestamp; // deprecated
        private Long gracefulTime = 5000L; // default value, deprecated
        private ConsistencyLevel consistencyLevel = null; // default value
        private boolean ignoreGrowing;
        private String timezone = "";
        private List<OrderByField> orderByFields = new ArrayList<>();
        private String groupByFieldName;
        private Integer groupSize;
        private Boolean strictGroupSize;
        private CreateCollectionReq.Function ranker;
        private FunctionScore functionScore;
        private List<FunctionChain> functionChains;
        private Map<String, Object> filterTemplateValues = new HashMap<>(); // default value
        private Highlighter highlighter;
        private SearchAggregation searchAggregation;

        private SearchReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public SearchReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public SearchReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
         * {@code MilvusClientV2Session}.
         */
        @Deprecated
        public SearchReqBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        /**
         * Sets the partition names to search in.
         *
         * @param partitionNames the partition names
         * @return this builder
         */
        public SearchReqBuilder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        /**
         * Sets the name of the vector field to search.
         *
         * @param annsField the ANN search field name
         * @return this builder
         */
        public SearchReqBuilder annsField(String annsField) {
            this.annsField = annsField;
            return this;
        }

        /**
         * Sets the metric type used for the search.
         *
         * @param metricType the metric type
         * @return this builder
         */
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

        /**
         * Sets the filter expression.
         *
         * @param filter the filter expression
         * @return this builder
         */
        public SearchReqBuilder filter(String filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Sets the fields to return for each search result.
         *
         * @param outputFields the output fields
         * @return this builder
         */
        public SearchReqBuilder outputFields(List<String> outputFields) {
            this.outputFields = outputFields;
            return this;
        }

        /**
         * Sets the query vectors.
         *
         * @param data the query vectors
         * @return this builder
         */
        public SearchReqBuilder data(List<BaseVector> data) {
            this.data = data;
            return this;
        }

        /**
         * Sets the primary key values of the entities to search.
         *
         * @param ids the primary key values
         * @return this builder
         */
        public SearchReqBuilder ids(List<Object> ids) {
            this.ids = ids;
            return this;
        }

        /**
         * Sets the offset of the first result to return.
         *
         * @param offset the offset
         * @return this builder
         */
        public SearchReqBuilder offset(long offset) {
            this.offset = offset;
            return this;
        }

        /**
         * Sets the maximum number of results to return.
         *
         * @param limit the limit value
         * @return this builder
         */
        public SearchReqBuilder limit(long limit) {
            this.topK = (int) limit;
            this.limit = limit;
            return this;
        }

        /**
         * Sets the number of decimal places to round the scores to.
         *
         * @param roundDecimal the round decimal value
         * @return this builder
         */
        public SearchReqBuilder roundDecimal(int roundDecimal) {
            this.roundDecimal = roundDecimal;
            return this;
        }

        /**
         * Sets the search parameters.
         *
         * @param searchParams the search parameters
         * @return this builder
         */
        public SearchReqBuilder searchParams(Map<String, Object> searchParams) {
            this.searchParams = searchParams;
            return this;
        }

        /**
         * Sets the guarantee timestamp.
         *
         * @deprecated no longer used
         * @param guaranteeTimestamp the guarantee timestamp
         * @return this builder
         */
        public SearchReqBuilder guaranteeTimestamp(long guaranteeTimestamp) {
            this.guaranteeTimestamp = guaranteeTimestamp;
            return this;
        }

        /**
         * Sets the graceful time in milliseconds.
         *
         * @deprecated no longer used
         * @param gracefulTime the graceful time
         * @return this builder
         */
        public SearchReqBuilder gracefulTime(Long gracefulTime) {
            this.gracefulTime = gracefulTime;
            return this;
        }

        /**
         * Sets the consistency level for the search.
         *
         * @param consistencyLevel the consistency level
         * @return this builder
         */
        public SearchReqBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        /**
         * Sets whether growing segments are ignored.
         *
         * @param ignoreGrowing {@code true} if growing segments are ignored
         * @return this builder
         */
        public SearchReqBuilder ignoreGrowing(boolean ignoreGrowing) {
            this.ignoreGrowing = ignoreGrowing;
            return this;
        }

        /**
         * Sets the timezone used for timestamp fields in the filter expression.
         *
         * @param timezone the timezone
         * @return this builder
         */
        public SearchReqBuilder timezone(String timezone) {
            this.timezone = timezone;
            return this;
        }

        /**
         * Sets the order-by fields used to sort the search results.
         *
         * @param orderByFields the order-by fields
         * @return this builder
         */
        public SearchReqBuilder orderByFields(List<OrderByField> orderByFields) {
            this.orderByFields = orderByFields;
            return this;
        }

        /**
         * Sets the field name used to group the search results.
         *
         * @param groupByFieldName the group-by field name
         * @return this builder
         */
        public SearchReqBuilder groupByFieldName(String groupByFieldName) {
            this.groupByFieldName = groupByFieldName;
            return this;
        }

        /**
         * Sets the maximum number of hits per group.
         *
         * @param groupSize the group size
         * @return this builder
         */
        public SearchReqBuilder groupSize(Integer groupSize) {
            this.groupSize = groupSize;
            return this;
        }

        /**
         * Sets whether the group size is enforced strictly.
         *
         * @param strictGroupSize {@code true} if the group size is strict
         * @return this builder
         */
        public SearchReqBuilder strictGroupSize(Boolean strictGroupSize) {
            this.strictGroupSize = strictGroupSize;
            return this;
        }

        /**
         * Sets the ranker used to rerank the search results.
         *
         * @param ranker the ranker
         * @return this builder
         */
        public SearchReqBuilder ranker(CreateCollectionReq.Function ranker) {
            this.ranker = ranker;
            return this;
        }

        /**
         * Sets the function score used for multi-rankers.
         *
         * @param functionScore the function score
         * @return this builder
         */
        public SearchReqBuilder functionScore(FunctionScore functionScore) {
            this.functionScore = functionScore;
            return this;
        }

        /**
         * Sets the function chains applied to an ordinary search.
         *
         * @param functionChains the function chains
         * @return this builder
         */
        public SearchReqBuilder functionChains(List<FunctionChain> functionChains) {
            this.functionChains = functionChains;
            return this;
        }

        /**
         * Adds a function chain applied to an ordinary search.
         *
         * @param functionChain the function chain to add
         * @return this builder
         */
        public SearchReqBuilder addFunctionChain(FunctionChain functionChain) {
            if (functionChain == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Function chain must not be null");
            }
            if (this.functionChains == null) {
                this.functionChains = new ArrayList<>();
            }
            this.functionChains.add(functionChain);
            return this;
        }

        /**
         * Sets the expression template values used to improve expression parsing performance.
         *
         * @param filterTemplateValues the filter template values
         * @return this builder
         */
        public SearchReqBuilder filterTemplateValues(Map<String, Object> filterTemplateValues) {
            this.filterTemplateValues = filterTemplateValues;
            return this;
        }

        /**
         * Sets the highlighter used to highlight matching text in the search results.
         *
         * @param highlighter the highlighter
         * @return this builder
         */
        public SearchReqBuilder highlighter(Highlighter highlighter) {
            this.highlighter = highlighter;
            return this;
        }

        /**
         * Sets the search aggregation.
         *
         * @param searchAggregation the search aggregation
         * @return this builder
         */
        public SearchReqBuilder searchAggregation(SearchAggregation searchAggregation) {
            this.searchAggregation = searchAggregation;
            return this;
        }

        /**
         * Builds the {@link SearchReq}.
         *
         * @return the request
         */
        public SearchReq build() {
            return new SearchReq(this);
        }
    }
}
