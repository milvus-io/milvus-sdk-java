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
import io.milvus.v2.service.vector.request.aggregation.OrderByField;

import java.util.*;

/**
 * Request parameters for the {@code query} API.
 */
public class QueryReq {
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
    private List<Object> ids;
    private String filter;
    private ConsistencyLevel consistencyLevel;
    private long offset;
    private long limit;
    private boolean ignoreGrowing;
    private String timezone;
    private List<OrderByField> orderByFields;

    // Extra parameters for query, timezone, time_fields, etc.
    // Make sure the value can be converted to String by String.valueOf().
    // For example: {"timezone": "America/Chicago"}
    private Map<String, Object> queryParams = new HashMap<>();

    // Expression template, to improve expression parsing performance in complicated list
    // Assume user has a filter = "pk > 3 and city in ["beijing", "shanghai", ......]
    // The long list of city will increase the time cost to parse this expression.
    // So, we provide exprTemplateValues for this purpose, user can set filter like this:
    //     filter = "pk > {age} and city in {city}"
    //     filterTemplateValues = Map{"age": 3, "city": List<String>{"beijing", "shanghai", ......}}
    // Valid value of this map can be:
    //     Boolean, Long, Double, String, List<Boolean>, List<Long>, List<Double>, List<String>
    private Map<String, Object> filterTemplateValues;

    private QueryReq(QueryReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.clusterId = builder.clusterId;
        this.partitionNames = builder.partitionNames;
        this.outputFields = builder.outputFields;
        this.ids = builder.ids;
        this.filter = builder.filter;
        this.consistencyLevel = builder.consistencyLevel;
        this.offset = builder.offset;
        this.limit = builder.limit;
        this.ignoreGrowing = builder.ignoreGrowing;
        this.orderByFields = builder.orderByFields;
        this.queryParams = builder.queryParams;
        this.filterTemplateValues = builder.filterTemplateValues;
        this.timezone = builder.timezone;
    }

    /**
     * Creates a new {@code QueryReq} builder.
     *
     * @return the builder
     */
    public static QueryReqBuilder builder() {
        return new QueryReqBuilder();
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
     * Returns the primary key values of the entities to query.
     *
     * @return the primary key values
     */
    public List<Object> getIds() {
        return ids;
    }

    /**
     * Sets the primary key values of the entities to query.
     *
     * @param ids the primary key values
     */
    public void setIds(List<Object> ids) {
        this.ids = ids;
    }

    /**
     * Returns the query filter expression.
     *
     * @return the filter expression
     */
    public String getFilter() {
        return filter;
    }

    /**
     * Sets the query filter expression.
     *
     * @param filter the filter expression
     */
    public void setFilter(String filter) {
        this.filter = filter;
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
     * Returns the order-by fields used to sort the query results.
     *
     * @return the order-by fields
     */
    public List<OrderByField> getOrderByFields() {
        return orderByFields;
    }

    /**
     * Sets the order-by fields used to sort the query results.
     *
     * @param orderByFields the order-by fields
     */
    public void setOrderByFields(List<OrderByField> orderByFields) {
        this.orderByFields = orderByFields;
    }

    /**
     * Returns the extra query parameters.
     *
     * @return the query parameters
     */
    public Map<String, Object> getQueryParams() {
        return queryParams;
    }

    /**
     * Sets the extra query parameters.
     *
     * @param queryParams the query parameters
     */
    public void setQueryParams(Map<String, Object> queryParams) {
        this.queryParams = queryParams;
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

    @Override
    public String toString() {
        return "QueryReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", partitionNames=" + partitionNames +
                ", outputFields=" + outputFields +
                ", ids=" + ids +
                ", filter='" + filter + '\'' +
                ", consistencyLevel=" + consistencyLevel +
                ", offset=" + offset +
                ", limit=" + limit +
                ", ignoreGrowing=" + ignoreGrowing +
                ", timezone='" + timezone + '\'' +
                ", orderByFields=" + orderByFields +
                ", queryParams=" + queryParams +
//                ", filterTemplateValues=" + filterTemplateValues +
                '}';
    }

    public static class QueryReqBuilder {
        private String databaseName;
        private String collectionName;
        private String clusterId;
        private List<String> partitionNames = new ArrayList<>();
        private List<String> outputFields = Collections.singletonList("*");
        private List<Object> ids;
        private String filter = "";
        private ConsistencyLevel consistencyLevel = null;
        private long offset;
        private long limit;
        private boolean ignoreGrowing;
        private String timezone = "";
        private List<OrderByField> orderByFields = new ArrayList<>();
        private Map<String, Object> queryParams = new HashMap<>();
        private Map<String, Object> filterTemplateValues = new HashMap<>();

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public QueryReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public QueryReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * @deprecated Request-level cluster routing is no longer used. {@code clusterId} is passed via
         * {@code MilvusClientV2Session}.
         */
        @Deprecated
        public QueryReqBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        /**
         * Sets the partition names to query.
         *
         * @param partitionNames the partition names
         * @return this builder
         */
        public QueryReqBuilder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        /**
         * Sets the fields to return for each queried entity.
         *
         * @param outputFields the output fields
         * @return this builder
         */
        public QueryReqBuilder outputFields(List<String> outputFields) {
            this.outputFields = outputFields;
            return this;
        }

        /**
         * Sets the primary key values of the entities to query.
         *
         * @param ids the primary key values
         * @return this builder
         */
        public QueryReqBuilder ids(List<Object> ids) {
            this.ids = ids;
            return this;
        }

        /**
         * Sets the query filter expression.
         *
         * @param filter the filter expression
         * @return this builder
         */
        public QueryReqBuilder filter(String filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Sets the consistency level for the query.
         *
         * @param consistencyLevel the consistency level
         * @return this builder
         */
        public QueryReqBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        /**
         * Sets the offset of the first result to return.
         *
         * @param offset the offset
         * @return this builder
         */
        public QueryReqBuilder offset(long offset) {
            this.offset = offset;
            return this;
        }

        /**
         * Sets the maximum number of results to return.
         *
         * @param limit the limit value
         * @return this builder
         */
        public QueryReqBuilder limit(long limit) {
            this.limit = limit;
            return this;
        }

        /**
         * Sets whether growing segments are ignored.
         *
         * @param ignoreGrowing {@code true} if growing segments are ignored
         * @return this builder
         */
        public QueryReqBuilder ignoreGrowing(boolean ignoreGrowing) {
            this.ignoreGrowing = ignoreGrowing;
            return this;
        }

        /**
         * Sets the timezone used for timestamp fields in the query expression.
         *
         * @param timezone the timezone
         * @return this builder
         */
        public QueryReqBuilder timezone(String timezone) {
            this.timezone = timezone;
            return this;
        }

        /**
         * Sets the order-by fields used to sort the query results.
         *
         * @param orderByFields the order-by fields
         * @return this builder
         */
        public QueryReqBuilder orderByFields(List<OrderByField> orderByFields) {
            this.orderByFields = orderByFields;
            return this;
        }

        /**
         * Sets the extra query parameters.
         *
         * @param queryParams the query parameters
         * @return this builder
         */
        public QueryReqBuilder queryParams(Map<String, Object> queryParams) {
            this.queryParams = queryParams;
            return this;
        }

        /**
         * Sets the expression template values used to improve expression parsing performance.
         *
         * @param filterTemplateValues the filter template values
         * @return this builder
         */
        public QueryReqBuilder filterTemplateValues(Map<String, Object> filterTemplateValues) {
            this.filterTemplateValues = filterTemplateValues;
            return this;
        }

        /**
         * Builds the {@link QueryReq}.
         *
         * @return the request
         */
        public QueryReq build() {
            return new QueryReq(this);
        }
    }
}
