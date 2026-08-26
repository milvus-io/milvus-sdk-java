package io.milvus.v2.service.vector.request;

import com.google.common.collect.Lists;
import io.milvus.orm.iterator.QueryIteratorCursor;
import io.milvus.v2.common.ConsistencyLevel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Request parameters for the {@code queryIterator} API.
 */
public class QueryIteratorReq {
    private String databaseName;
    private String collectionName;
    /**
     * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
     * {@code MilvusClientV2Session}.
     */
    @Deprecated
    private String clusterId;
    private List<String> partitionNames;
    private List<String> outputFields;
    private String expr;
    private ConsistencyLevel consistencyLevel;
    private long offset;
    private long limit;
    private boolean ignoreGrowing;
    private String timezone;
    private long batchSize;
    private boolean reduceStopForBest;

    // Expression template, to improve expression parsing performance in complicated list
    // Assume user has a filter = "pk > 3 and city in ["beijing", "shanghai", ......]
    // The long list of city will increase the time cost to parse this expression.
    // So, we provide exprTemplateValues for this purpose, user can set filter like this:
    //     filter = "pk > {age} and city in {city}"
    //     filterTemplateValues = Map{"age": 3, "city": List<String>{"beijing", "shanghai", ......}}
    // Valid value of this map can be:
    //     Boolean, Long, Double, String, List<Boolean>, List<Long>, List<Double>, List<String>
    private Map<String, Object> filterTemplateValues;

    // A previously captured cursor to resume pagination from (pymilvus parity).
    // When set, the iterator continues from the cursor's session ts and pk/element
    // position instead of starting over; offset is ignored in that case.
    private QueryIteratorCursor cursor;

    private QueryIteratorReq(QueryIteratorReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.clusterId = builder.clusterId;
        this.partitionNames = builder.partitionNames;
        this.outputFields = builder.outputFields;
        this.expr = builder.expr;
        this.consistencyLevel = builder.consistencyLevel;
        this.offset = builder.offset;
        this.limit = builder.limit;
        this.ignoreGrowing = builder.ignoreGrowing;
        this.timezone = builder.timezone;
        this.batchSize = builder.batchSize;
        this.reduceStopForBest = builder.reduceStopForBest;
        this.filterTemplateValues = builder.filterTemplateValues;
        this.cursor = builder.cursor;
    }

    /**
     * Creates a new {@code QueryIteratorReq} builder.
     *
     * @return the builder
     */
    public static QueryIteratorReqBuilder builder() {
        return new QueryIteratorReqBuilder();
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
     * Returns the partition names to query.
     *
     * @return the partition names
     */
    public List<String> getPartitionNames() {
        return partitionNames;
    }

    /**
     * Sets the partition names to query.
     *
     * @param partitionNames the partition names
     */
    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    /**
     * Returns the fields to return for each queried entity.
     *
     * @return the output fields
     */
    public List<String> getOutputFields() {
        return outputFields;
    }

    /**
     * Sets the fields to return for each queried entity.
     *
     * @param outputFields the output fields
     */
    public void setOutputFields(List<String> outputFields) {
        this.outputFields = outputFields;
    }

    /**
     * Returns the query filter expression.
     *
     * @return the query expression
     */
    public String getExpr() {
        return expr;
    }

    /**
     * Sets the query filter expression.
     *
     * @param expr the query expression
     */
    public void setExpr(String expr) {
        this.expr = expr;
    }

    /**
     * Returns the consistency level for the query.
     *
     * @return the consistency level
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    /**
     * Sets the consistency level for the query.
     *
     * @param consistencyLevel the consistency level
     */
    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
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
     * Returns the timezone used for timestamp fields in the query expression.
     *
     * @return the timezone
     */
    public String getTimezone() {
        return timezone;
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

    /**
     * Returns whether to stop reducing the query results to the best entities.
     *
     * @return {@code true} if stop-for-best is enabled
     */
    public boolean isReduceStopForBest() {
        return reduceStopForBest;
    }

    /**
     * Sets whether to stop reducing the query results to the best entities.
     *
     * @param reduceStopForBest {@code true} if stop-for-best is enabled
     */
    public void setReduceStopForBest(boolean reduceStopForBest) {
        this.reduceStopForBest = reduceStopForBest;
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
     * Returns a previously captured cursor used to resume pagination from its position.
     *
     * @return the query iterator cursor
     */
    public QueryIteratorCursor getCursor() {
        return cursor;
    }

    /**
     * Sets a previously captured cursor used to resume pagination from its position.
     *
     * @param cursor the query iterator cursor
     */
    public void setCursor(QueryIteratorCursor cursor) {
        this.cursor = cursor;
    }

    @Override
    public String toString() {
        return "QueryIteratorReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", partitionNames=" + partitionNames +
                ", outputFields=" + outputFields +
                ", expr='" + expr + '\'' +
                ", consistencyLevel=" + consistencyLevel +
                ", offset=" + offset +
                ", limit=" + limit +
                ", ignoreGrowing=" + ignoreGrowing +
                ", timezone='" + timezone + '\'' +
                ", batchSize=" + batchSize +
                ", reduceStopForBest=" + reduceStopForBest +
                ", cursor=" + cursor +
                '}';
    }

    public static class QueryIteratorReqBuilder {
        private String databaseName;
        private String collectionName;
        private String clusterId;
        private List<String> partitionNames = Lists.newArrayList();
        private List<String> outputFields = Lists.newArrayList();
        private String expr = "";
        private ConsistencyLevel consistencyLevel = null;
        private long offset = 0;
        private long limit = -1;
        private boolean ignoreGrowing = false;
        private String timezone = "";
        private long batchSize = 1000L;
        private boolean reduceStopForBest = true;
        private Map<String, Object> filterTemplateValues = new HashMap<>();
        private QueryIteratorCursor cursor;

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public QueryIteratorReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public QueryIteratorReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
         * {@code MilvusClientV2Session}.
         */
        @Deprecated
        public QueryIteratorReqBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        /**
         * Sets the partition names to query.
         *
         * @param partitionNames the partition names
         * @return this builder
         */
        public QueryIteratorReqBuilder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        /**
         * Sets the fields to return for each queried entity.
         *
         * @param outputFields the output fields
         * @return this builder
         */
        public QueryIteratorReqBuilder outputFields(List<String> outputFields) {
            this.outputFields = outputFields;
            return this;
        }

        /**
         * Sets the query filter expression.
         *
         * @param expr the query expression
         * @return this builder
         */
        public QueryIteratorReqBuilder expr(String expr) {
            this.expr = expr;
            return this;
        }

        /**
         * Sets the consistency level for the query.
         *
         * @param consistencyLevel the consistency level
         * @return this builder
         */
        public QueryIteratorReqBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        /**
         * Sets the offset of the first result to return.
         *
         * @param offset the offset
         * @return this builder
         */
        public QueryIteratorReqBuilder offset(long offset) {
            this.offset = offset;
            return this;
        }

        /**
         * Sets the maximum number of results to return.
         *
         * @param limit the limit value
         * @return this builder
         */
        public QueryIteratorReqBuilder limit(long limit) {
            this.limit = limit;
            return this;
        }

        /**
         * Sets whether growing segments are ignored.
         *
         * @param ignoreGrowing {@code true} if growing segments are ignored
         * @return this builder
         */
        public QueryIteratorReqBuilder ignoreGrowing(boolean ignoreGrowing) {
            this.ignoreGrowing = ignoreGrowing;
            return this;
        }

        /**
         * Sets the timezone used for timestamp fields in the query expression.
         *
         * @param timezone the timezone
         * @return this builder
         */
        public QueryIteratorReqBuilder timezone(String timezone) {
            this.timezone = timezone;
            return this;
        }

        /**
         * Sets the number of results fetched per iterator batch.
         *
         * @param batchSize the batch size
         * @return this builder
         */
        public QueryIteratorReqBuilder batchSize(long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Sets whether to stop reducing the query results to the best entities.
         *
         * @param reduceStopForBest {@code true} if stop-for-best is enabled
         * @return this builder
         */
        public QueryIteratorReqBuilder reduceStopForBest(boolean reduceStopForBest) {
            this.reduceStopForBest = reduceStopForBest;
            return this;
        }

        /**
         * Sets the expression template values used to improve expression parsing performance.
         *
         * @param filterTemplateValues the filter template values
         * @return this builder
         */
        public QueryIteratorReqBuilder filterTemplateValues(Map<String, Object> filterTemplateValues) {
            this.filterTemplateValues = filterTemplateValues;
            return this;
        }

        /**
         * Sets a previously captured cursor used to resume pagination from its position.
         *
         * @param cursor the query iterator cursor
         * @return this builder
         */
        public QueryIteratorReqBuilder cursor(QueryIteratorCursor cursor) {
            this.cursor = cursor;
            return this;
        }

        /**
         * Builds the {@link QueryIteratorReq}.
         *
         * @return the request
         */
        public QueryIteratorReq build() {
            return new QueryIteratorReq(this);
        }
    }
}
