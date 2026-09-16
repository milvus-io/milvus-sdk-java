package io.milvus.v2.service.vector.request;

import com.google.common.collect.Lists;
import io.milvus.param.Constant;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Request parameters for a V2 search iterator, specifying the collection, vectors, metric type,
 * limit, filter, and other search options. Built via {@link SearchIteratorReqV2Builder}.
 */


public class SearchIteratorReqV2 {
    private String databaseName;
    private String collectionName;
    /**
     * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
     * {@code MilvusClientV2Session}.
     */
    @Deprecated
    private String clusterId;
    private List<String> partitionNames;
    private IndexParam.MetricType metricType;
    private String vectorFieldName;
    @Deprecated
    private int topK;
    private long limit;
    private String filter;
    private List<String> outputFields;
    private List<BaseVector> vectors;
    private int roundDecimal;
    private Map<String, Object> searchParams;
    private ConsistencyLevel consistencyLevel;
    private boolean ignoreGrowing;
    private String timezone;
    private String groupByFieldName;
    private long batchSize;
    private Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> externalFilterFunc;

    // Expression template, to improve expression parsing performance in complicated list
    // Assume user has a filter = "pk > 3 and city in ["beijing", "shanghai", ......]
    // The long list of city will increase the time cost to parse this expression.
    // So, we provide exprTemplateValues for this purpose, user can set filter like this:
    //     filter = "pk > {age} and city in {city}"
    //     filterTemplateValues = Map{"age": 3, "city": List<String>{"beijing", "shanghai", ......}}
    // Valid value of this map can be:
    //     Boolean, Long, Double, String, List<Boolean>, List<Long>, List<Double>, List<String>
    private Map<String, Object> filterTemplateValues;

    private SearchIteratorReqV2(SearchIteratorReqV2Builder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.clusterId = builder.clusterId;
        this.partitionNames = builder.partitionNames;
        this.metricType = builder.metricType;
        this.vectorFieldName = builder.vectorFieldName;
        this.topK = builder.topK;
        this.limit = builder.limit;
        this.filter = builder.filter;
        this.outputFields = builder.outputFields;
        this.vectors = builder.vectors;
        this.roundDecimal = builder.roundDecimal;
        this.searchParams = builder.searchParams;
        this.consistencyLevel = builder.consistencyLevel;
        this.ignoreGrowing = builder.ignoreGrowing;
        this.timezone = builder.timezone;
        this.groupByFieldName = builder.groupByFieldName;
        this.batchSize = builder.batchSize;
        this.externalFilterFunc = builder.externalFilterFunc;
        this.filterTemplateValues = builder.filterTemplateValues;
    }

    /**
     * Creates a new {@code SearchIteratorReqV2} builder.
     *
     * @return the builder
     */


    public static SearchIteratorReqV2Builder builder() {
        return new SearchIteratorReqV2Builder();
    }

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
     * @return the cluster ID
     */
    @Deprecated
    public String getClusterId() {
        return clusterId;
    }

    /**
     * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
     * {@code MilvusClientV2Session}.
     * @param clusterId the cluster ID
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
     * Returns the name of the vector field to search.
     *
     * @return the vector field name
     */


    public String getVectorFieldName() {
        return vectorFieldName;
    }

    /**
     * Sets the name of the vector field to search.
     *
     * @param vectorFieldName the vector field name
     */


    public void setVectorFieldName(String vectorFieldName) {
        this.vectorFieldName = vectorFieldName;
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


    public List<BaseVector> getVectors() {
        return vectors;
    }

    /**
     * Sets the query vectors.
     *
     * @param vectors the query vectors
     */


    public void setVectors(List<BaseVector> vectors) {
        this.vectors = vectors;
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
     * Returns the field name used to group the search results.
     *
     * @return the field name
     */


    public String getGroupByFieldName() {
        return groupByFieldName;
    }

    /**
     * Sets the field name used to group the search results.
     *
     * @param groupByFieldName the field name
     */


    public void setGroupByFieldName(String groupByFieldName) {
        this.groupByFieldName = groupByFieldName;
    }

    /**
     * Returns the batch size used to fetch results per iteration.
     *
     * @return the batch size
     */


    public long getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the batch size used to fetch results per iteration.
     *
     * @param batchSize the batch size
     */


    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Returns the function applied to filter the search results after retrieval.
     *
     * @return the external filter function
     */


    public Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> getExternalFilterFunc() {
        return externalFilterFunc;
    }

    /**
     * Sets the function applied to filter the search results after retrieval.
     *
     * @param externalFilterFunc the external filter function
     */


    public void setExternalFilterFunc(Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> externalFilterFunc) {
        this.externalFilterFunc = externalFilterFunc;
    }

    /**
     * Returns the filter template values.
     *
     * @return the filter template values
     */


    public Map<String, Object> getFilterTemplateValues() {
        return filterTemplateValues;
    }

    @Override
    public String toString() {
        return "SearchIteratorReqV2{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", partitionNames=" + partitionNames +
                ", metricType=" + metricType +
                ", vectorFieldName='" + vectorFieldName + '\'' +
                ", topK=" + topK +
                ", limit=" + limit +
                ", filter='" + filter + '\'' +
                ", outputFields=" + outputFields +
                ", vectors=" + vectors +
                ", roundDecimal=" + roundDecimal +
                ", searchParams=" + searchParams +
                ", consistencyLevel=" + consistencyLevel +
                ", ignoreGrowing=" + ignoreGrowing +
                ", timezone='" + timezone + '\'' +
                ", groupByFieldName='" + groupByFieldName + '\'' +
                ", batchSize=" + batchSize +
                ", externalFilterFunc=" + externalFilterFunc +
                '}';
    }

    /**
     * Builder for {@link SearchIteratorReqV2} class.
     */


    public static class SearchIteratorReqV2Builder {
        private String databaseName;
        private String collectionName;
        private String clusterId;
        private List<String> partitionNames = Lists.newArrayList();
        private IndexParam.MetricType metricType = IndexParam.MetricType.INVALID;
        private String vectorFieldName;
        private int topK = Constant.UNLIMITED;
        private long limit = Constant.UNLIMITED_L;
        private String filter = "";
        private List<String> outputFields = Lists.newArrayList();
        private List<BaseVector> vectors = Lists.newArrayList();
        private int roundDecimal = -1;
        private Map<String, Object> searchParams = new HashMap<>();
        private ConsistencyLevel consistencyLevel = null;
        private boolean ignoreGrowing = false;
        private String timezone = "";
        private String groupByFieldName = "";
        private long batchSize = 1000L;
        private Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> externalFilterFunc = null;
        private Map<String, Object> filterTemplateValues = new HashMap<>();

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */


        public SearchIteratorReqV2Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */


        public SearchIteratorReqV2Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
         * {@code MilvusClientV2Session}.
         * @param clusterId the cluster ID
         * @return this builder
         */
        @Deprecated
        public SearchIteratorReqV2Builder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        /**
         * Sets the partition names to search in.
         *
         * @param partitionNames the partition names
         * @return this builder
         */


        public SearchIteratorReqV2Builder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        /**
         * Sets the metric type used for the search.
         *
         * @param metricType the metric type
         * @return this builder
         */


        public SearchIteratorReqV2Builder metricType(IndexParam.MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        /**
         * Sets the name of the vector field to search.
         *
         * @param vectorFieldName the vector field name
         * @return this builder
         */


        public SearchIteratorReqV2Builder vectorFieldName(String vectorFieldName) {
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        /**
         * Sets the maximum number of results to return.
         *
         * @deprecated replaced by {@link #limit(long)}. {@code topK} and {@code limit} must be the
         * same value.
         * @param val the topK value
         * @return this builder
         */
        @Deprecated
        public SearchIteratorReqV2Builder topK(int val) {
            this.topK = val;
            this.limit = val;
            return this;
        }

        /**
         * Sets the maximum number of results to return.
         *
         * @param val the limit value
         * @return this builder
         */


        public SearchIteratorReqV2Builder limit(long val) {
            this.topK = (int) val;
            this.limit = val;
            return this;
        }

        /**
         * Sets the filter expression.
         *
         * @param filter the filter expression
         * @return this builder
         */


        public SearchIteratorReqV2Builder filter(String filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Sets the fields to return for each search result.
         *
         * @param outputFields the output fields
         * @return this builder
         */


        public SearchIteratorReqV2Builder outputFields(List<String> outputFields) {
            this.outputFields = outputFields;
            return this;
        }

        /**
         * Sets the query vectors.
         *
         * @param vectors the query vectors
         * @return this builder
         */


        public SearchIteratorReqV2Builder vectors(List<BaseVector> vectors) {
            this.vectors = vectors;
            return this;
        }

        /**
         * Sets the number of decimal places to round the scores to.
         *
         * @param roundDecimal the round decimal value
         * @return this builder
         */


        public SearchIteratorReqV2Builder roundDecimal(int roundDecimal) {
            this.roundDecimal = roundDecimal;
            return this;
        }

        /**
         * Sets the search parameters.
         *
         * @param searchParams the search parameters
         * @return this builder
         */


        public SearchIteratorReqV2Builder searchParams(Map<String, Object> searchParams) {
            this.searchParams = searchParams;
            return this;
        }

        /**
         * Sets the consistency level for the search.
         *
         * @param consistencyLevel the consistency level
         * @return this builder
         */


        public SearchIteratorReqV2Builder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        /**
         * Sets whether growing segments are ignored.
         *
         * @param ignoreGrowing {@code true} if growing segments are ignored
         * @return this builder
         */


        public SearchIteratorReqV2Builder ignoreGrowing(boolean ignoreGrowing) {
            this.ignoreGrowing = ignoreGrowing;
            return this;
        }

        /**
         * Sets the timezone used for timestamp fields in the filter expression.
         *
         * @param timezone the timezone
         * @return this builder
         */


        public SearchIteratorReqV2Builder timezone(String timezone) {
            this.timezone = timezone;
            return this;
        }

        /**
         * Sets the field name used to group the search results.
         *
         * @param groupByFieldName the field name
         * @return this builder
         */


        public SearchIteratorReqV2Builder groupByFieldName(String groupByFieldName) {
            this.groupByFieldName = groupByFieldName;
            return this;
        }

        /**
         * Sets the batch size used to fetch results per iteration.
         *
         * @param batchSize the batch size
         * @return this builder
         */


        public SearchIteratorReqV2Builder batchSize(long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Sets the function applied to filter the search results after retrieval.
         *
         * @param externalFilterFunc the external filter function
         * @return this builder
         */


        public SearchIteratorReqV2Builder externalFilterFunc(Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> externalFilterFunc) {
            this.externalFilterFunc = externalFilterFunc;
            return this;
        }

        /**
         * Sets the filter template values.
         *
         * @param filterTemplateValues the filter template values
         * @return this builder
         */


        public SearchIteratorReqV2Builder filterTemplateValues(Map<String, Object> filterTemplateValues) {
            this.filterTemplateValues = filterTemplateValues;
            return this;
        }

        /**
         * Builds the {@link SearchIteratorReqV2}.
         *
         * @return the request
         */


        public SearchIteratorReqV2 build() {
            return new SearchIteratorReqV2(this);
        }
    }
}
