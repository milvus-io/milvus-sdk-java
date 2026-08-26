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

package io.milvus.v2.service.vector.request.highlighter;

import io.milvus.common.utils.JsonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A lexical highlighter used by the {@code search} API to highlight the terms of the
 * search query that match the returned text fields, based on lexical (BM25) matching.
 */
public class LexicalHighlighter implements Highlighter {
    private final List<HighlightQuery> highlightQueries;
    private final Boolean highlightSearchText;
    private final List<String> preTags;
    private final List<String> postTags;
    private final Integer fragmentOffset;
    private final Integer fragmentSize;
    private final Integer numOfFragments;

    /**
     * Constructs a {@link LexicalHighlighter} from the given builder.
     *
     * @param builder the builder holding the highlighter settings
     */
    public LexicalHighlighter(LexicalHighlighterBuilder builder) {
        this.highlightQueries = builder.highlightQueries;
        this.highlightSearchText = builder.highlightSearchText;
        this.preTags = builder.preTags;
        this.postTags = builder.postTags;
        this.fragmentOffset = builder.fragmentOffset;
        this.fragmentSize = builder.fragmentSize;
        this.numOfFragments = builder.numOfFragments;
    }

    /**
     * Returns the highlight type name, {@code Lexical}.
     *
     * @return the highlight type name
     */
    @Override
    public String highlightType() {
        return "Lexical";
    }

    /**
     * Returns the lexical highlight parameters as a map of parameter name to value.
     *
     * @return the highlight parameters
     */
    @Override
    public Map<String, String> getParams() {
        Map<String, String> params = new java.util.HashMap<>();
        if (this.highlightQueries != null) {
            // serialize the list of HighlightQuery to a JSON array string using Gson
            params.put("highlight_query", JsonUtils.toJson(this.highlightQueries));
        }
        if (this.highlightSearchText != null) {
            params.put("highlight_search_text", this.highlightSearchText.toString());
        }
        if (this.preTags != null) {
            params.put("pre_tags", JsonUtils.toJson(this.preTags));
        }
        if (this.postTags != null) {
            params.put("post_tags", JsonUtils.toJson(this.postTags));
        }
        if (this.fragmentOffset != null) {
            params.put("fragment_offset", this.fragmentOffset.toString());
        }
        if (this.fragmentSize != null) {
            params.put("fragment_size", this.fragmentSize.toString());
        }
        if (this.numOfFragments != null) {
            params.put("num_of_fragments", this.numOfFragments.toString());
        }
        return params;
    }

    /**
     * A single highlight query that specifies the field to highlight and the query text
     * used to match the highlighted terms.
     */
    public static class HighlightQuery {
        /**
         * The query type, for example {@code match} or {@code match_phrase}.
         */
        public String type;
        /**
         * The name of the field to highlight.
         */
        public String field;
        /**
         * The query text used for matching.
         */
        public String text;

        /**
         * Constructs a highlight query.
         *
         * @param type  the query type
         * @param field the field to highlight
         * @param query the query text
         */
        public HighlightQuery(String type, String field, String query) {
            this.type = type;
            this.field = field;
            this.text = query;
        }

        @Override
        public String toString() {
            return JsonUtils.toJson(this);
        }
    }

    /**
     * Builder for {@link LexicalHighlighter}.
     */
    public static class LexicalHighlighterBuilder {
        private List<HighlightQuery> highlightQueries;
        private Boolean highlightSearchText;
        private List<String> preTags;
        private List<String> postTags;
        private Integer fragmentOffset;
        private Integer fragmentSize;
        private Integer numOfFragments;

        public LexicalHighlighterBuilder() {
        }

        /**
         * Sets the highlight queries used to highlight the matched terms.
         *
         * @param queries the highlight queries
         * @return this builder
         */
        public LexicalHighlighterBuilder highlightQueries(List<HighlightQuery> queries) {
            this.highlightQueries = queries;
            return this;
        }

        /**
         * Adds a single highlight query to the highlighter.
         *
         * @param q the highlight query
         * @return this builder
         */
        public LexicalHighlighterBuilder addHighlightQuery(HighlightQuery q) {
            if (this.highlightQueries == null) this.highlightQueries = new ArrayList<>();
            this.highlightQueries.add(q);
            return this;
        }

        /**
         * Sets whether the original search text should also be highlighted.
         *
         * @param highlightSearchText {@code true} to highlight the search text
         * @return this builder
         */
        public LexicalHighlighterBuilder highlightSearchText(Boolean highlightSearchText) {
            this.highlightSearchText = highlightSearchText;
            return this;
        }

        /**
         * Sets the tags prepended to the highlighted terms.
         *
         * @param preTags the pre-tags
         * @return this builder
         */
        public LexicalHighlighterBuilder preTags(List<String> preTags) {
            this.preTags = preTags;
            return this;
        }

        /**
         * Adds a single pre-tag to the highlighter.
         *
         * @param tag the pre-tag
         * @return this builder
         */
        public LexicalHighlighterBuilder addPreTag(String tag) {
            if (this.preTags == null) this.preTags = new ArrayList<>();
            this.preTags.add(tag);
            return this;
        }

        /**
         * Sets the tags appended to the highlighted terms.
         *
         * @param postTags the post-tags
         * @return this builder
         */
        public LexicalHighlighterBuilder postTags(List<String> postTags) {
            this.postTags = postTags;
            return this;
        }

        /**
         * Adds a single post-tag to the highlighter.
         *
         * @param tag the post-tag
         * @return this builder
         */
        public LexicalHighlighterBuilder addPostTag(String tag) {
            if (this.postTags == null) this.postTags = new ArrayList<>();
            this.postTags.add(tag);
            return this;
        }

        /**
         * Sets the offset at which a highlighted fragment starts.
         *
         * @param offset the fragment offset
         * @return this builder
         */
        public LexicalHighlighterBuilder fragmentOffset(Integer offset) {
            this.fragmentOffset = offset;
            return this;
        }

        /**
         * Sets the size of each highlighted fragment.
         *
         * @param size the fragment size
         * @return this builder
         */
        public LexicalHighlighterBuilder fragmentSize(Integer size) {
            this.fragmentSize = size;
            return this;
        }

        /**
         * Sets the number of fragments to return per field.
         *
         * @param num the number of fragments
         * @return this builder
         */
        public LexicalHighlighterBuilder numOfFragments(Integer num) {
            this.numOfFragments = num;
            return this;
        }

        /**
         * Builds the {@link LexicalHighlighter}.
         *
         * @return the built highlighter
         */
        public LexicalHighlighter build() {
            return new LexicalHighlighter(this);
        }
    }

    /**
     * Creates a new builder for {@link LexicalHighlighter}.
     *
     * @return a new builder
     */
    public static LexicalHighlighterBuilder builder() {
        return new LexicalHighlighterBuilder();
    }
}
