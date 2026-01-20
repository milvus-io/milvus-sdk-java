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

public class LexicalHighlighter implements Highlighter {
    private final List<HighlightQuery> highlightQueries;
    private final Boolean highlightSearchText;
    private final List<String> preTags;
    private final List<String> postTags;
    private final Integer fragmentOffset;
    private final Integer fragmentSize;
    private final Integer numOfFragments;

    public LexicalHighlighter(LexicalHighlighterBuilder builder) {
        this.highlightQueries = builder.highlightQueries;
        this.highlightSearchText = builder.highlightSearchText;
        this.preTags = builder.preTags;
        this.postTags = builder.postTags;
        this.fragmentOffset = builder.fragmentOffset;
        this.fragmentSize = builder.fragmentSize;
        this.numOfFragments = builder.numOfFragments;
    }

    @Override
    public String highlightType() {
        return "Lexical";
    }

    @Override
    public Map<String, String> getParams() {
        Map<String, String> params = new java.util.HashMap<>();
        if (this.highlightQueries != null) {
            // serialize the list of HighlightQuery to a JSON array string using Gson
            params.put("highlight_queries", JsonUtils.toJson(this.highlightQueries));
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

    public static class HighlightQuery {
        public String type;
        public String field;
        public String text;

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

        public LexicalHighlighterBuilder highlightQueries(List<HighlightQuery> queries) {
            this.highlightQueries = queries;
            return this;
        }

        public LexicalHighlighterBuilder addHighlightQuery(HighlightQuery q) {
            if (this.highlightQueries == null) this.highlightQueries = new ArrayList<>();
            this.highlightQueries.add(q);
            return this;
        }

        public LexicalHighlighterBuilder highlightSearchText(Boolean highlightSearchText) {
            this.highlightSearchText = highlightSearchText;
            return this;
        }

        public LexicalHighlighterBuilder preTags(List<String> preTags) {
            this.preTags = preTags;
            return this;
        }

        public LexicalHighlighterBuilder addPreTag(String tag) {
            if (this.preTags == null) this.preTags = new ArrayList<>();
            this.preTags.add(tag);
            return this;
        }

        public LexicalHighlighterBuilder postTags(List<String> postTags) {
            this.postTags = postTags;
            return this;
        }

        public LexicalHighlighterBuilder addPostTag(String tag) {
            if (this.postTags == null) this.postTags = new ArrayList<>();
            this.postTags.add(tag);
            return this;
        }

        public LexicalHighlighterBuilder fragmentOffset(Integer offset) {
            this.fragmentOffset = offset;
            return this;
        }

        public LexicalHighlighterBuilder fragmentSize(Integer size) {
            this.fragmentSize = size;
            return this;
        }

        public LexicalHighlighterBuilder numOfFragments(Integer num) {
            this.numOfFragments = num;
            return this;
        }

        public LexicalHighlighter build() {
            return new LexicalHighlighter(this);
        }
    }

    public static LexicalHighlighterBuilder builder() {
        return new LexicalHighlighterBuilder();
    }
}
