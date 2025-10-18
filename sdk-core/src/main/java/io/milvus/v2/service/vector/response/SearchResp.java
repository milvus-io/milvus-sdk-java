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

public class SearchResp {
    private List<List<SearchResult>> searchResults;
    private long sessionTs; // default eventually ts
    private List<Float> recalls;

    private SearchResp(Builder builder) {
        this.searchResults = builder.searchResults;
        this.sessionTs = builder.sessionTs;
        this.recalls = builder.recalls;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<List<SearchResult>> getSearchResults() {
        return searchResults;
    }

    public void setSearchResults(List<List<SearchResult>> searchResults) {
        this.searchResults = searchResults;
    }

    public long getSessionTs() {
        return sessionTs;
    }

    public void setSessionTs(long sessionTs) {
        this.sessionTs = sessionTs;
    }

    public List<Float> getRecalls() {
        return recalls;
    }

    public void setRecalls(List<Float> recalls) {
        this.recalls = recalls;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SearchResp that = (SearchResp) obj;
        return new EqualsBuilder()
                .append(sessionTs, that.sessionTs)
                .append(searchResults, that.searchResults)
                .append(recalls, that.recalls)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(searchResults)
                .append(sessionTs)
                .append(recalls)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "SearchResp{" +
                "searchResults=" + searchResults +
                ", sessionTs=" + sessionTs +
                ", recalls=" + recalls +
                '}';
    }

    public static class Builder {
        private List<List<SearchResult>> searchResults = new ArrayList<>();
        private long sessionTs = 1L; // default eventually ts
        private List<Float> recalls = new ArrayList<>();

        public Builder searchResults(List<List<SearchResult>> searchResults) {
            this.searchResults = searchResults;
            return this;
        }

        public Builder sessionTs(long sessionTs) {
            this.sessionTs = sessionTs;
            return this;
        }

        public Builder recalls(List<Float> recalls) {
            this.recalls = recalls;
            return this;
        }

        public SearchResp build() {
            return new SearchResp(this);
        }
    }

    public static class SearchResult {
        private Map<String, Object> entity;
        private Float score;
        private Object id;
        private String primaryKey;

        private SearchResult(Builder builder) {
            this.entity = builder.entity;
            this.score = builder.score;
            this.id = builder.id;
            this.primaryKey = builder.primaryKey;
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

        public Float getScore() {
            return score;
        }

        public void setScore(Float score) {
            this.score = score;
        }

        public Object getId() {
            return id;
        }

        public void setId(Object id) {
            this.id = id;
        }

        public String getPrimaryKey() {
            return primaryKey;
        }

        public void setPrimaryKey(String primaryKey) {
            this.primaryKey = primaryKey;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            SearchResult that = (SearchResult) obj;
            return new EqualsBuilder()
                    .append(entity, that.entity)
                    .append(score, that.score)
                    .append(id, that.id)
                    .append(primaryKey, that.primaryKey)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(entity)
                    .append(score)
                    .append(id)
                    .append(primaryKey)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return "{" + getPrimaryKey() + ": " + getId() + ", Score: " + getScore() + ", OutputFields: " + entity + "}";
        }

        public static class Builder {
            private Map<String, Object> entity = new HashMap<>();
            private Float score;
            private Object id;
            private String primaryKey = "id";

            public Builder entity(Map<String, Object> entity) {
                this.entity = entity;
                return this;
            }

            public Builder score(Float score) {
                this.score = score;
                return this;
            }

            public Builder id(Object id) {
                this.id = id;
                return this;
            }

            public Builder primaryKey(String primaryKey) {
                this.primaryKey = primaryKey;
                return this;
            }

            public SearchResult build() {
                return new SearchResult(this);
            }
        }
    }
}
