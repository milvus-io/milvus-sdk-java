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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryResp {
    private List<QueryResult> queryResults;
    private long sessionTs; // default eventually ts

    private QueryResp(Builder builder) {
        this.queryResults = builder.queryResults;
        this.sessionTs = builder.sessionTs;
    }

    public static Builder builder() {
        return new Builder();
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        QueryResp that = (QueryResp) obj;
        return new EqualsBuilder()
                .append(sessionTs, that.sessionTs)
                .append(queryResults, that.queryResults)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(queryResults)
                .append(sessionTs)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "QueryResp{" +
                "queryResults=" + queryResults +
                ", sessionTs=" + sessionTs +
                '}';
    }

    public static class Builder {
        private List<QueryResult> queryResults = new ArrayList<>();
        private long sessionTs = 1L; // default eventually ts

        public Builder queryResults(List<QueryResult> queryResults) {
            this.queryResults = queryResults;
            return this;
        }

        public Builder sessionTs(long sessionTs) {
            this.sessionTs = sessionTs;
            return this;
        }

        public QueryResp build() {
            return new QueryResp(this);
        }
    }

    public static class QueryResult {
        private Map<String, Object> entity;

        private QueryResult(Builder builder) {
            this.entity = builder.entity;
        }

        public static Builder builder() {
            return new Builder();
        }

        public Map<String, Object> getEntity() {
            return entity;
        }

        public void setEntity(Map<String, Object> entity) {
            this.entity = entity;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            QueryResult that = (QueryResult) obj;
            return new EqualsBuilder()
                    .append(entity, that.entity)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(entity)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return "QueryResult{" +
                    "entity=" + entity +
                    '}';
        }

        public static class Builder {
            private Map<String, Object> entity = new HashMap<>();

            public Builder entity(Map<String, Object> entity) {
                this.entity = entity;
                return this;
            }

            public QueryResult build() {
                return new QueryResult(this);
            }
        }
    }
}
