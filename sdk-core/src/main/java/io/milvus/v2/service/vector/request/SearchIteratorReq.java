package io.milvus.v2.service.vector.request;

import com.google.common.collect.Lists;
import io.milvus.param.Constant;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.data.BaseVector;

import java.util.List;

/**
 * Request parameters for the {@code searchIterator} API.
 */
public class SearchIteratorReq {
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
    private String expr;
    private List<String> outputFields;
    private List<BaseVector> vectors;
    private int roundDecimal;
    private String params;
    private ConsistencyLevel consistencyLevel;
    private boolean ignoreGrowing;
    private String groupByFieldName;
    private long batchSize;

    private SearchIteratorReq(SearchIteratorReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.clusterId = builder.clusterId;
        this.partitionNames = builder.partitionNames;
        this.metricType = builder.metricType;
        this.vectorFieldName = builder.vectorFieldName;
        this.topK = builder.topK;
        this.limit = builder.limit;
        this.expr = builder.expr;
        this.outputFields = builder.outputFields;
        this.vectors = builder.vectors;
        this.roundDecimal = builder.roundDecimal;
        this.params = builder.params;
        this.consistencyLevel = builder.consistencyLevel;
        this.ignoreGrowing = builder.ignoreGrowing;
        this.groupByFieldName = builder.groupByFieldName;
        this.batchSize = builder.batchSize;
    }

    /**
     * Creates a new {@code SearchIteratorReq} builder.
     *
     * @return the builder
     */
    public static SearchIteratorReqBuilder builder() {
        return new SearchIteratorReqBuilder();
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
    public String getExpr() {
        return expr;
    }

    /**
     * Sets the filter expression.
     *
     * @param expr the filter expression
     */
    public void setExpr(String expr) {
        this.expr = expr;
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
     * Returns the search parameter string.
     *
     * @return the search parameters
     */
    public String getParams() {
        return params;
    }

    /**
     * Sets the search parameter string.
     *
     * @param params the search parameters
     */
    public void setParams(String params) {
        this.params = params;
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
     * Returns the number of results fetched per iterator batch.
     *
     * @return the batch size
     */
    public long getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of results fetched per iterator batch.
     *
     * @param batchSize the batch size
     */
    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public String toString() {
        return "SearchIteratorReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", partitionNames=" + partitionNames +
                ", metricType=" + metricType +
                ", vectorFieldName='" + vectorFieldName + '\'' +
                ", topK=" + topK +
                ", limit=" + limit +
                ", expr='" + expr + '\'' +
                ", outputFields=" + outputFields +
                ", vectors=" + vectors +
                ", roundDecimal=" + roundDecimal +
                ", params='" + params + '\'' +
                ", consistencyLevel=" + consistencyLevel +
                ", ignoreGrowing=" + ignoreGrowing +
                ", groupByFieldName='" + groupByFieldName + '\'' +
                ", batchSize=" + batchSize +
                '}';
    }

    public static class SearchIteratorReqBuilder {
        private String databaseName;
        private String collectionName;
        private String clusterId;
        private List<String> partitionNames = Lists.newArrayList();
        private IndexParam.MetricType metricType = IndexParam.MetricType.INVALID;
        private String vectorFieldName;
        private int topK = Constant.UNLIMITED;
        private long limit = Constant.UNLIMITED_L;
        private String expr = "";
        private List<String> outputFields = Lists.newArrayList();
        private List<BaseVector> vectors = Lists.newArrayList();
        private int roundDecimal = -1;
        private String params = "{}";
        private ConsistencyLevel consistencyLevel = null;
        private boolean ignoreGrowing = false;
        private String groupByFieldName = "";
        private long batchSize = 1000L;

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public SearchIteratorReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public SearchIteratorReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
         * {@code MilvusClientV2Session}.
         */
        @Deprecated
        public SearchIteratorReqBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        /**
         * Sets the partition names to search in.
         *
         * @param partitionNames the partition names
         * @return this builder
         */
        public SearchIteratorReqBuilder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        /**
         * Sets the metric type used for the search.
         *
         * @param metricType the metric type
         * @return this builder
         */
        public SearchIteratorReqBuilder metricType(IndexParam.MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        /**
         * Sets the name of the vector field to search.
         *
         * @param vectorFieldName the vector field name
         * @return this builder
         */
        public SearchIteratorReqBuilder vectorFieldName(String vectorFieldName) {
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        // topK is deprecated, topK and limit must be the same value
        @Deprecated
        public SearchIteratorReqBuilder topK(int val) {
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
        public SearchIteratorReqBuilder limit(long val) {
            this.topK = (int) val;
            this.limit = val;
            return this;
        }

        /**
         * Sets the filter expression.
         *
         * @param expr the filter expression
         * @return this builder
         */
        public SearchIteratorReqBuilder expr(String expr) {
            this.expr = expr;
            return this;
        }

        /**
         * Sets the fields to return for each search result.
         *
         * @param outputFields the output fields
         * @return this builder
         */
        public SearchIteratorReqBuilder outputFields(List<String> outputFields) {
            this.outputFields = outputFields;
            return this;
        }

        /**
         * Sets the query vectors.
         *
         * @param vectors the query vectors
         * @return this builder
         */
        public SearchIteratorReqBuilder vectors(List<BaseVector> vectors) {
            this.vectors = vectors;
            return this;
        }

        /**
         * Sets the number of decimal places to round the scores to.
         *
         * @param roundDecimal the round decimal value
         * @return this builder
         */
        public SearchIteratorReqBuilder roundDecimal(int roundDecimal) {
            this.roundDecimal = roundDecimal;
            return this;
        }

        /**
         * Sets the search parameter string.
         *
         * @param params the search parameters
         * @return this builder
         */
        public SearchIteratorReqBuilder params(String params) {
            this.params = params;
            return this;
        }

        /**
         * Sets the consistency level for the search.
         *
         * @param consistencyLevel the consistency level
         * @return this builder
         */
        public SearchIteratorReqBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        /**
         * Sets whether growing segments are ignored.
         *
         * @param ignoreGrowing {@code true} if growing segments are ignored
         * @return this builder
         */
        public SearchIteratorReqBuilder ignoreGrowing(boolean ignoreGrowing) {
            this.ignoreGrowing = ignoreGrowing;
            return this;
        }

        /**
         * Sets the field name used to group the search results.
         *
         * @param groupByFieldName the group-by field name
         * @return this builder
         */
        public SearchIteratorReqBuilder groupByFieldName(String groupByFieldName) {
            this.groupByFieldName = groupByFieldName;
            return this;
        }

        /**
         * Sets the number of results fetched per iterator batch.
         *
         * @param batchSize the batch size
         * @return this builder
         */
        public SearchIteratorReqBuilder batchSize(long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Builds the {@link SearchIteratorReq}.
         *
         * @return the request
         */
        public SearchIteratorReq build() {
            return new SearchIteratorReq(this);
        }
    }
}
