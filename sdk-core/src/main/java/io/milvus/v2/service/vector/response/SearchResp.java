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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.*;

public class SearchResp {
    private List<List<SearchResult>> searchResults;
    private long sessionTs; // default eventually ts
    private List<Float> recalls;

    private SearchResp(SearchRespBuilder builder) {
        this.searchResults = builder.searchResults;
        this.sessionTs = builder.sessionTs;
        this.recalls = builder.recalls;
    }

    public static SearchRespBuilder builder() {
        return new SearchRespBuilder();
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
    public String toString() {
        return "SearchResp{" +
                "searchResults=" + searchResults +
                ", sessionTs=" + sessionTs +
                ", recalls=" + recalls +
                '}';
    }

    public static class SearchRespBuilder {
        private List<List<SearchResult>> searchResults = new ArrayList<>();
        private long sessionTs = 1L; // default eventually ts
        private List<Float> recalls = new ArrayList<>();

        public SearchRespBuilder searchResults(List<List<SearchResult>> searchResults) {
            this.searchResults = searchResults;
            return this;
        }

        public SearchRespBuilder sessionTs(long sessionTs) {
            this.sessionTs = sessionTs;
            return this;
        }

        public SearchRespBuilder recalls(List<Float> recalls) {
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
        private Map<String, HighlightResult> highlightResults;

        private SearchResult(SearchResultBuilder builder) {
            this.entity = builder.entity;
            this.score = builder.score;
            this.id = builder.id;
            this.primaryKey = builder.primaryKey;
            this.highlightResults = builder.highlightResults == null ? new HashMap<>() : builder.highlightResults;
        }

        public static SearchResultBuilder builder() {
            return new SearchResultBuilder();
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

        public Map<String, HighlightResult> getHighlightResults() {
            return highlightResults;
        }

        public HighlightResult getHighlightResult(String fieldName) {
            return this.highlightResults.get(fieldName);
        }

        public void addHighlightResult(String fieldName, HighlightResult highlightResult) {
            if (this.highlightResults == null) this.highlightResults = new HashMap<>();
            this.highlightResults.put(fieldName, highlightResult);
        }

        @Override
        public String toString() {
            return "{" + getPrimaryKey() + ": " + getId() + ", Score: " + getScore() + ", OutputFields: " + entity +
                    (MapUtils.isEmpty(highlightResults) ? "" : (", HighlightResults: " + highlightResults)) + "}";
        }

        public static class SearchResultBuilder {
            private Map<String, Object> entity = new HashMap<>();
            private Float score;
            private Object id;
            private String primaryKey = "id";
            private Map<String, HighlightResult> highlightResults = new HashMap<>();

            public SearchResultBuilder entity(Map<String, Object> entity) {
                this.entity = entity;
                return this;
            }

            public SearchResultBuilder score(Float score) {
                this.score = score;
                return this;
            }

            public SearchResultBuilder id(Object id) {
                this.id = id;
                return this;
            }

            public SearchResultBuilder primaryKey(String primaryKey) {
                this.primaryKey = primaryKey;
                return this;
            }

            public SearchResultBuilder highlightResults(Map<String, HighlightResult> highlightResults) {
                this.highlightResults = highlightResults;
                return this;
            }

            public SearchResultBuilder addHighlightResult(String fieldName, HighlightResult highlightResult) {
                if (this.highlightResults == null) this.highlightResults = new HashMap<>();
                this.highlightResults.put(fieldName, highlightResult);
                return this;
            }

            public SearchResult build() {
                return new SearchResult(this);
            }
        }
    }

    public static class HighlightResult {
        private final String fieldName;
        private final List<String> fragments;
        private final List<Float> scores;

        private HighlightResult(HighlightResultBuilder builder) {
            this.fieldName = builder.fieldName;
            this.fragments = builder.fragments;
            this.scores = builder.scores;
        }

        public static HighlightResultBuilder builder() {
            return new HighlightResultBuilder();
        }

        public String getFieldName() {
            return fieldName;
        }

        public List<String> getFragments() {
            return fragments;
        }

        public List<Float> getScores() {
            return scores;
        }

        @Override
        public String toString() {
            return "HighlightResult{" +
                    "fieldName='" + fieldName + '\'' +
                    ", fragments=" + fragments +
                    ", scores=" + scores +
                    '}';
        }

        public static class HighlightResultBuilder {
            private String fieldName = "";
            private List<String> fragments = new ArrayList<>();
            private List<Float> scores = new ArrayList<>();

            public HighlightResultBuilder fieldName(String fieldName) {
                this.fieldName = fieldName;
                return this;
            }

            public HighlightResultBuilder fragments(List<String> fragments) {
                this.fragments = fragments;
                return this;
            }

            public HighlightResultBuilder addFragment(String fragment) {
                if (this.fragments == null) this.fragments = new ArrayList<>();
                this.fragments.add(fragment);
                return this;
            }

            public HighlightResultBuilder scores(List<Float> scores) {
                this.scores = scores;
                return this;
            }

            public HighlightResultBuilder addScore(Float score) {
                if (this.scores == null) this.scores = new ArrayList<>();
                this.scores.add(score);
                return this;
            }

            public HighlightResult build() {
                return new HighlightResult(this);
            }
        }

    }

}
