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

package io.milvus.v2.service.vector.response;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Response returned by the {@code query} API.
 */
public class QueryResp {
    private List<QueryResult> queryResults;
    private long sessionTs; // default eventually ts
    private Long cost;
    private Long scannedRemoteBytes;
    private Long scannedTotalBytes;
    private Float cacheHitRatio;

    protected QueryResp(QueryRespBuilder builder) {
        this.queryResults = builder.queryResults;
        this.sessionTs = builder.sessionTs;
        this.cost = builder.cost;
        this.scannedRemoteBytes = builder.scannedRemoteBytes;
        this.scannedTotalBytes = builder.scannedTotalBytes;
        this.cacheHitRatio = builder.cacheHitRatio;
    }

    /**
     * Creates a new {@code QueryResp} builder.
     *
     * @return the builder
     */
    public static QueryRespBuilder builder() {
        return new QueryRespBuilder();
    }

    /**
     * Returns the queried entities.
     *
     * @return the query results
     */
    public List<QueryResult> getQueryResults() {
        return queryResults;
    }

    /**
     * Sets the queried entities.
     *
     * @param queryResults the query results
     */
    public void setQueryResults(List<QueryResult> queryResults) {
        this.queryResults = queryResults;
    }

    /**
     * Returns the session timestamp.
     *
     * @return the session timestamp
     */
    public long getSessionTs() {
        return sessionTs;
    }

    /**
     * Sets the session timestamp.
     *
     * @param sessionTs the session timestamp
     */
    public void setSessionTs(long sessionTs) {
        this.sessionTs = sessionTs;
    }

    /**
     * Returns the time cost of the query operation.
     *
     * @return the cost
     */
    public Long getCost() {
        return cost;
    }

    /**
     * Sets the time cost of the query operation.
     *
     * @param cost the cost
     */
    public void setCost(Long cost) {
        this.cost = cost;
    }

    /**
     * Returns the number of bytes scanned remotely during the query.
     *
     * @return the scanned remote bytes, or {@code null} if not reported by the server
     */
    public Long getScannedRemoteBytes() {
        return scannedRemoteBytes;
    }

    /**
     * Sets the number of bytes scanned remotely during the query.
     *
     * @param scannedRemoteBytes the scanned remote bytes
     */
    public void setScannedRemoteBytes(Long scannedRemoteBytes) {
        this.scannedRemoteBytes = scannedRemoteBytes;
    }

    /**
     * Returns the total number of bytes scanned during the query.
     *
     * @return the scanned total bytes, or {@code null} if not reported by the server
     */
    public Long getScannedTotalBytes() {
        return scannedTotalBytes;
    }

    /**
     * Sets the total number of bytes scanned during the query.
     *
     * @param scannedTotalBytes the scanned total bytes
     */
    public void setScannedTotalBytes(Long scannedTotalBytes) {
        this.scannedTotalBytes = scannedTotalBytes;
    }

    /**
     * Returns the cache hit ratio of the query.
     *
     * @return the cache hit ratio, or {@code null} if not reported by the server
     */
    public Float getCacheHitRatio() {
        return cacheHitRatio;
    }

    /**
     * Sets the cache hit ratio of the query.
     *
     * @param cacheHitRatio the cache hit ratio
     */
    public void setCacheHitRatio(Float cacheHitRatio) {
        this.cacheHitRatio = cacheHitRatio;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "queryResults=" + queryResults +
                ", sessionTs=" + sessionTs +
                ", cost=" + cost +
                ", scannedRemoteBytes=" + scannedRemoteBytes +
                ", scannedTotalBytes=" + scannedTotalBytes +
                ", cacheHitRatio=" + cacheHitRatio +
                '}';
    }

    public static class QueryRespBuilder {
        private List<QueryResult> queryResults = new ArrayList<>();
        private long sessionTs = 1L; // default eventually ts
        private Long cost;
        private Long scannedRemoteBytes;
        private Long scannedTotalBytes;
        private Float cacheHitRatio;

        /**
         * Sets the queried entities.
         *
         * @param queryResults the query results
         * @return this builder
         */
        public QueryRespBuilder queryResults(List<QueryResult> queryResults) {
            this.queryResults = queryResults;
            return this;
        }

        /**
         * Sets the session timestamp.
         *
         * @param sessionTs the session timestamp
         * @return this builder
         */
        public QueryRespBuilder sessionTs(long sessionTs) {
            this.sessionTs = sessionTs;
            return this;
        }

        /**
         * Sets the time cost of the query operation.
         *
         * @param cost the cost
         * @return this builder
         */
        public QueryRespBuilder cost(Long cost) {
            this.cost = cost;
            return this;
        }

        /**
         * Sets the number of bytes scanned remotely during the query.
         *
         * @param scannedRemoteBytes the scanned remote bytes
         * @return this builder
         */
        public QueryRespBuilder scannedRemoteBytes(Long scannedRemoteBytes) {
            this.scannedRemoteBytes = scannedRemoteBytes;
            return this;
        }

        /**
         * Sets the total number of bytes scanned during the query.
         *
         * @param scannedTotalBytes the scanned total bytes
         * @return this builder
         */
        public QueryRespBuilder scannedTotalBytes(Long scannedTotalBytes) {
            this.scannedTotalBytes = scannedTotalBytes;
            return this;
        }

        /**
         * Sets the cache hit ratio of the query.
         *
         * @param cacheHitRatio the cache hit ratio
         * @return this builder
         */
        public QueryRespBuilder cacheHitRatio(Float cacheHitRatio) {
            this.cacheHitRatio = cacheHitRatio;
            return this;
        }

        /**
         * Builds the {@link QueryResp}.
         *
         * @return the response
         */
        public QueryResp build() {
            return new QueryResp(this);
        }
    }

    /**
     * A single entity returned by the {@code query} API.
     */
    public static class QueryResult {
        private Map<String, Object> entity;

        /**
         * For struct-array element-level queries (via {@code element_filter}): the matched
         * element's index within the array. Null for ordinary (non-element-level) queries.
         *
         * <p>The source entity index before row expansion is intentionally NOT exposed here:
         * callers identify the source entity by its primary key in the {@code entity} map, so
         * the original index carries no user-meaningful information.
         *
         * <p>Note on the V2 surface: {@code query()} exposes the matched offset as a typed
         * accessor here, while {@code queryIterator()} embeds it as an {@code "offset"} key
         * inside the entity map (see {@code Constant.OFFSET}). The entity map of this class
         * intentionally does not contain an {@code "offset"} key.
         */
        private Long elementOffset;

        private QueryResult(QueryResultBuilder builder) {
            this.entity = builder.entity;
            this.elementOffset = builder.elementOffset;
        }

        /**
         * Creates a new {@code QueryResult} builder.
         *
         * @return the builder
         */
        public static QueryResultBuilder builder() {
            return new QueryResultBuilder();
        }

        /**
         * Returns the entity data.
         *
         * @return the entity map
         */
        public Map<String, Object> getEntity() {
            return entity;
        }

        /**
         * Sets the entity data.
         *
         * @param entity the entity map
         */
        public void setEntity(Map<String, Object> entity) {
            this.entity = entity;
        }

        /**
         * Returns the matched element's index within the array for element-level queries.
         *
         * @return the element offset, or {@code null} for ordinary queries
         */
        public Long getElementOffset() {
            return elementOffset;
        }

        /**
         * Sets the matched element's index within the array for element-level queries.
         *
         * @param elementOffset the element offset
         */
        public void setElementOffset(Long elementOffset) {
            this.elementOffset = elementOffset;
        }

        @Override
        public String toString() {
            return "QueryResult{" +
                    "entity=" + entity +
                    (elementOffset != null ? ", elementOffset=" + elementOffset : "") +
                    '}';
        }

        public static class QueryResultBuilder {
            private Map<String, Object> entity = new HashMap<>();
            private Long elementOffset;

            /**
             * Sets the entity data.
             *
             * @param entity the entity map
             * @return this builder
             */
            public QueryResultBuilder entity(Map<String, Object> entity) {
                this.entity = entity;
                return this;
            }

            /**
             * Sets the matched element's index within the array for element-level queries.
             *
             * @param elementOffset the element offset
             * @return this builder
             */
            public QueryResultBuilder elementOffset(Long elementOffset) {
                this.elementOffset = elementOffset;
                return this;
            }

            /**
             * Builds the {@link QueryResult}.
             *
             * @return the query result
             */
            public QueryResult build() {
                return new QueryResult(this);
            }
        }
    }
}
