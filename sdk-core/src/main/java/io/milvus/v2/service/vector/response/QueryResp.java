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

public class QueryResp {
    private List<QueryResult> queryResults;
    private long sessionTs; // default eventually ts
    private Long cost;

    private QueryResp(QueryRespBuilder builder) {
        this.queryResults = builder.queryResults;
        this.sessionTs = builder.sessionTs;
        this.cost = builder.cost;
    }

    public static QueryRespBuilder builder() {
        return new QueryRespBuilder();
    }

    public List<QueryResult> getQueryResults() {
        return queryResults;
    }

    public void setQueryResults(List<QueryResult> queryResults) {
        this.queryResults = queryResults;
    }

    public long getSessionTs() {
        return sessionTs;
    }

    public void setSessionTs(long sessionTs) {
        this.sessionTs = sessionTs;
    }

    public Long getCost() {
        return cost;
    }

    public void setCost(Long cost) {
        this.cost = cost;
    }

    @Override
    public String toString() {
        return "QueryResp{" +
                "queryResults=" + queryResults +
                ", sessionTs=" + sessionTs +
                ", cost=" + cost +
                '}';
    }

    public static class QueryRespBuilder {
        private List<QueryResult> queryResults = new ArrayList<>();
        private long sessionTs = 1L; // default eventually ts
        private Long cost;

        public QueryRespBuilder queryResults(List<QueryResult> queryResults) {
            this.queryResults = queryResults;
            return this;
        }

        public QueryRespBuilder sessionTs(long sessionTs) {
            this.sessionTs = sessionTs;
            return this;
        }

        public QueryRespBuilder cost(Long cost) {
            this.cost = cost;
            return this;
        }

        public QueryResp build() {
            return new QueryResp(this);
        }
    }

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

        public static QueryResultBuilder builder() {
            return new QueryResultBuilder();
        }

        public Map<String, Object> getEntity() {
            return entity;
        }

        public void setEntity(Map<String, Object> entity) {
            this.entity = entity;
        }

        public Long getElementOffset() {
            return elementOffset;
        }

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

            public QueryResultBuilder entity(Map<String, Object> entity) {
                this.entity = entity;
                return this;
            }

            public QueryResultBuilder elementOffset(Long elementOffset) {
                this.elementOffset = elementOffset;
                return this;
            }

            public QueryResult build() {
                return new QueryResult(this);
            }
        }
    }
}
