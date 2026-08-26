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

import io.milvus.v2.service.vector.response.aggregation.AggregationBucket;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.*;

/**
 * Response returned by the {@code search} API.
 */
public class SearchResp {
    private List<List<SearchResult>> searchResults;
    private long sessionTs; // default eventually ts
    private List<Float> recalls;
    private Long cost;
    private Long scannedRemoteBytes;
    private Long scannedTotalBytes;
    private Float cacheHitRatio;
    private List<List<AggregationBucket>> aggregationBuckets;

    private SearchResp(SearchRespBuilder builder) {
        this.searchResults = builder.searchResults;
        this.sessionTs = builder.sessionTs;
        this.recalls = builder.recalls;
        this.cost = builder.cost;
        this.scannedRemoteBytes = builder.scannedRemoteBytes;
        this.scannedTotalBytes = builder.scannedTotalBytes;
        this.cacheHitRatio = builder.cacheHitRatio;
        this.aggregationBuckets = builder.aggregationBuckets;
    }

    /**
     * Creates a new {@code SearchResp} builder.
     *
     * @return the builder
     */
    public static SearchRespBuilder builder() {
        return new SearchRespBuilder();
    }

    /**
     * Returns the search results, one list per query vector.
     *
     * @return the search results
     */
    public List<List<SearchResult>> getSearchResults() {
        return searchResults;
    }

    /**
     * Sets the search results.
     *
     * @param searchResults the search results
     */
    public void setSearchResults(List<List<SearchResult>> searchResults) {
        this.searchResults = searchResults;
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
     * Returns the recall rates for each query.
     *
     * @return the recalls
     */
    public List<Float> getRecalls() {
        return recalls;
    }

    /**
     * Sets the recall rates for each query.
     *
     * @param recalls the recalls
     */
    public void setRecalls(List<Float> recalls) {
        this.recalls = recalls;
    }

    /**
     * Returns the time cost of the search operation.
     *
     * @return the cost
     */
    public Long getCost() {
        return cost;
    }

    /**
     * Sets the time cost of the search operation.
     *
     * @param cost the cost
     */
    public void setCost(Long cost) {
        this.cost = cost;
    }

    /**
     * Returns the number of remote bytes scanned during the search.
     *
     * @return the scanned remote bytes
     */
    public Long getScannedRemoteBytes() {
        return scannedRemoteBytes;
    }

    /**
     * Sets the number of remote bytes scanned during the search.
     *
     * @param scannedRemoteBytes the scanned remote bytes
     */
    public void setScannedRemoteBytes(Long scannedRemoteBytes) {
        this.scannedRemoteBytes = scannedRemoteBytes;
    }

    /**
     * Returns the total number of bytes scanned during the search.
     *
     * @return the scanned total bytes
     */
    public Long getScannedTotalBytes() {
        return scannedTotalBytes;
    }

    /**
     * Sets the total number of bytes scanned during the search.
     *
     * @param scannedTotalBytes the scanned total bytes
     */
    public void setScannedTotalBytes(Long scannedTotalBytes) {
        this.scannedTotalBytes = scannedTotalBytes;
    }

    /**
     * Returns the cache hit ratio of the search.
     *
     * @return the cache hit ratio
     */
    public Float getCacheHitRatio() {
        return cacheHitRatio;
    }

    /**
     * Sets the cache hit ratio of the search.
     *
     * @param cacheHitRatio the cache hit ratio
     */
    public void setCacheHitRatio(Float cacheHitRatio) {
        this.cacheHitRatio = cacheHitRatio;
    }

    /**
     * Returns the aggregation buckets, one list per query.
     *
     * @return the aggregation buckets
     */
    public List<List<AggregationBucket>> getAggregationBuckets() {
        return aggregationBuckets;
    }

    /**
     * Sets the aggregation buckets.
     *
     * @param aggregationBuckets the aggregation buckets
     */
    public void setAggregationBuckets(List<List<AggregationBucket>> aggregationBuckets) {
        this.aggregationBuckets = aggregationBuckets;
    }

    @Override
    public String toString() {
        return "SearchResp{" +
                "searchResults=" + searchResults +
                ", sessionTs=" + sessionTs +
                ", recalls=" + recalls +
                ", cost=" + cost +
                ", scannedRemoteBytes=" + scannedRemoteBytes +
                ", scannedTotalBytes=" + scannedTotalBytes +
                ", cacheHitRatio=" + cacheHitRatio +
                ", aggregationBuckets=" + aggregationBuckets +
                '}';
    }

    public static class SearchRespBuilder {
        private List<List<SearchResult>> searchResults = new ArrayList<>();
        private long sessionTs = 1L; // default eventually ts
        private List<Float> recalls = new ArrayList<>();
        private Long cost;
        private Long scannedRemoteBytes;
        private Long scannedTotalBytes;
        private Float cacheHitRatio;
        private List<List<AggregationBucket>> aggregationBuckets = new ArrayList<>();

        /**
         * Sets the search results.
         *
         * @param searchResults the search results
         * @return this builder
         */
        public SearchRespBuilder searchResults(List<List<SearchResult>> searchResults) {
            this.searchResults = searchResults;
            return this;
        }

        /**
         * Sets the session timestamp.
         *
         * @param sessionTs the session timestamp
         * @return this builder
         */
        public SearchRespBuilder sessionTs(long sessionTs) {
            this.sessionTs = sessionTs;
            return this;
        }

        /**
         * Sets the recall rates for each query.
         *
         * @param recalls the recalls
         * @return this builder
         */
        public SearchRespBuilder recalls(List<Float> recalls) {
            this.recalls = recalls;
            return this;
        }

        /**
         * Sets the time cost of the search operation.
         *
         * @param cost the cost
         * @return this builder
         */
        public SearchRespBuilder cost(Long cost) {
            this.cost = cost;
            return this;
        }

        /**
         * Sets the number of remote bytes scanned during the search.
         *
         * @param scannedRemoteBytes the scanned remote bytes
         * @return this builder
         */
        public SearchRespBuilder scannedRemoteBytes(Long scannedRemoteBytes) {
            this.scannedRemoteBytes = scannedRemoteBytes;
            return this;
        }

        /**
         * Sets the total number of bytes scanned during the search.
         *
         * @param scannedTotalBytes the scanned total bytes
         * @return this builder
         */
        public SearchRespBuilder scannedTotalBytes(Long scannedTotalBytes) {
            this.scannedTotalBytes = scannedTotalBytes;
            return this;
        }

        /**
         * Sets the cache hit ratio of the search.
         *
         * @param cacheHitRatio the cache hit ratio
         * @return this builder
         */
        public SearchRespBuilder cacheHitRatio(Float cacheHitRatio) {
            this.cacheHitRatio = cacheHitRatio;
            return this;
        }

        /**
         * Sets the aggregation buckets.
         *
         * @param aggregationBuckets the aggregation buckets
         * @return this builder
         */
        public SearchRespBuilder aggregationBuckets(List<List<AggregationBucket>> aggregationBuckets) {
            this.aggregationBuckets = aggregationBuckets;
            return this;
        }

        /**
         * Builds the {@link SearchResp}.
         *
         * @return the response
         */
        public SearchResp build() {
            return new SearchResp(this);
        }
    }

    /**
     * A single entity returned by the {@code search} API.
     */
    public static class SearchResult {
        private Map<String, Object> entity;
        private Float score;
        private Object id;
        private String primaryKey;
        private Map<String, HighlightResult> highlightResults;
        private Long elementOffset;

        private SearchResult(SearchResultBuilder builder) {
            this.entity = builder.entity;
            this.score = builder.score;
            this.id = builder.id;
            this.primaryKey = builder.primaryKey;
            this.highlightResults = builder.highlightResults == null ? new HashMap<>() : builder.highlightResults;
            this.elementOffset = builder.elementOffset;
        }

        /**
         * Creates a new {@code SearchResult} builder.
         *
         * @return the builder
         */
        public static SearchResultBuilder builder() {
            return new SearchResultBuilder();
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
         * Returns the similarity score of the result.
         *
         * @return the score
         */
        public Float getScore() {
            return score;
        }

        /**
         * Sets the similarity score of the result.
         *
         * @param score the score
         */
        public void setScore(Float score) {
            this.score = score;
        }

        /**
         * Returns the primary key value of the result.
         *
         * @return the primary key value
         */
        public Object getId() {
            return id;
        }

        /**
         * Sets the primary key value of the result.
         *
         * @param id the primary key value
         */
        public void setId(Object id) {
            this.id = id;
        }

        /**
         * Returns the name of the primary key field.
         *
         * @return the primary key field name
         */
        public String getPrimaryKey() {
            return primaryKey;
        }

        /**
         * Sets the name of the primary key field.
         *
         * @param primaryKey the primary key field name
         */
        public void setPrimaryKey(String primaryKey) {
            this.primaryKey = primaryKey;
        }

        /**
         * Returns the highlight results keyed by field name.
         *
         * @return the highlight results
         */
        public Map<String, HighlightResult> getHighlightResults() {
            return highlightResults;
        }

        /**
         * Returns the highlight result for the given field.
         *
         * @param fieldName the field name
         * @return the highlight result, or {@code null} if none exists
         */
        public HighlightResult getHighlightResult(String fieldName) {
            return this.highlightResults.get(fieldName);
        }

        /**
         * Adds a highlight result for the given field.
         *
         * @param fieldName the field name
         * @param highlightResult the highlight result
         */
        public void addHighlightResult(String fieldName, HighlightResult highlightResult) {
            if (this.highlightResults == null) this.highlightResults = new HashMap<>();
            this.highlightResults.put(fieldName, highlightResult);
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
            return "{" + getPrimaryKey() + ": " + getId() + ", Score: " + getScore() + ", OutputFields: " + entity +
                    (MapUtils.isEmpty(highlightResults) ? "" : (", HighlightResults: " + highlightResults)) +
                    (elementOffset == null ? "" : (", ElementOffset: " + elementOffset)) + "}";
        }

        public static class SearchResultBuilder {
            private Map<String, Object> entity = new HashMap<>();
            private Float score;
            private Object id;
            private String primaryKey = "id";
            private Map<String, HighlightResult> highlightResults = new HashMap<>();
            private Long elementOffset;

            /**
             * Sets the entity data.
             *
             * @param entity the entity map
             * @return this builder
             */
            public SearchResultBuilder entity(Map<String, Object> entity) {
                this.entity = entity;
                return this;
            }

            /**
             * Sets the similarity score of the result.
             *
             * @param score the score
             * @return this builder
             */
            public SearchResultBuilder score(Float score) {
                this.score = score;
                return this;
            }

            /**
             * Sets the primary key value of the result.
             *
             * @param id the primary key value
             * @return this builder
             */
            public SearchResultBuilder id(Object id) {
                this.id = id;
                return this;
            }

            /**
             * Sets the name of the primary key field.
             *
             * @param primaryKey the primary key field name
             * @return this builder
             */
            public SearchResultBuilder primaryKey(String primaryKey) {
                this.primaryKey = primaryKey;
                return this;
            }

            /**
             * Sets the highlight results keyed by field name.
             *
             * @param highlightResults the highlight results
             * @return this builder
             */
            public SearchResultBuilder highlightResults(Map<String, HighlightResult> highlightResults) {
                this.highlightResults = highlightResults;
                return this;
            }

            /**
             * Adds a highlight result for the given field.
             *
             * @param fieldName the field name
             * @param highlightResult the highlight result
             * @return this builder
             */
            public SearchResultBuilder addHighlightResult(String fieldName, HighlightResult highlightResult) {
                if (this.highlightResults == null) this.highlightResults = new HashMap<>();
                this.highlightResults.put(fieldName, highlightResult);
                return this;
            }

            /**
             * Sets the matched element's index within the array for element-level queries.
             *
             * @param elementOffset the element offset
             * @return this builder
             */
            public SearchResultBuilder elementOffset(Long elementOffset) {
                this.elementOffset = elementOffset;
                return this;
            }

            /**
             * Builds the {@link SearchResult}.
             *
             * @return the search result
             */
            public SearchResult build() {
                return new SearchResult(this);
            }
        }
    }

    /**
     * The highlighted fragments of a field in a search result.
     */
    public static class HighlightResult {
        private final String fieldName;
        private final List<String> fragments;
        private final List<Float> scores;

        private HighlightResult(HighlightResultBuilder builder) {
            this.fieldName = builder.fieldName;
            this.fragments = builder.fragments;
            this.scores = builder.scores;
        }

        /**
         * Creates a new {@code HighlightResult} builder.
         *
         * @return the builder
         */
        public static HighlightResultBuilder builder() {
            return new HighlightResultBuilder();
        }

        /**
         * Returns the field name that was highlighted.
         *
         * @return the field name
         */
        public String getFieldName() {
            return fieldName;
        }

        /**
         * Returns the highlighted text fragments.
         *
         * @return the highlighted fragments
         */
        public List<String> getFragments() {
            return fragments;
        }

        /**
         * Returns the relevance scores of the highlighted fragments.
         *
         * @return the fragment scores
         */
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

            /**
             * Sets the field name that was highlighted.
             *
             * @param fieldName the field name
             * @return this builder
             */
            public HighlightResultBuilder fieldName(String fieldName) {
                this.fieldName = fieldName;
                return this;
            }

            /**
             * Sets the highlighted text fragments.
             *
             * @param fragments the highlighted fragments
             * @return this builder
             */
            public HighlightResultBuilder fragments(List<String> fragments) {
                this.fragments = fragments;
                return this;
            }

            /**
             * Adds a highlighted text fragment.
             *
             * @param fragment the highlighted fragment
             * @return this builder
             */
            public HighlightResultBuilder addFragment(String fragment) {
                if (this.fragments == null) this.fragments = new ArrayList<>();
                this.fragments.add(fragment);
                return this;
            }

            /**
             * Sets the relevance scores of the highlighted fragments.
             *
             * @param scores the fragment scores
             * @return this builder
             */
            public HighlightResultBuilder scores(List<Float> scores) {
                this.scores = scores;
                return this;
            }

            /**
             * Adds a relevance score for a highlighted fragment.
             *
             * @param score the fragment score
             * @return this builder
             */
            public HighlightResultBuilder addScore(Float score) {
                if (this.scores == null) this.scores = new ArrayList<>();
                this.scores.add(score);
                return this;
            }

            /**
             * Builds the {@link HighlightResult}.
             *
             * @return the highlight result
             */
            public HighlightResult build() {
                return new HighlightResult(this);
            }
        }

    }

}
