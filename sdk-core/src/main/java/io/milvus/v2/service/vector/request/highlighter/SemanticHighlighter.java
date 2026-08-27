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
 * A semantic highlighter used by the {@code search} API to highlight the passages of the
 * returned text fields that are semantically relevant to the queries, using an embedding
 * model.
 */
public class SemanticHighlighter implements Highlighter {
    private final List<String> queries;
    private final List<String> inputFields;
    private final List<String> preTags;
    private final List<String> postTags;
    private final Float threshold;
    private final Boolean highlightOnly;
    private final String modelDeploymentID;
    private final Integer maxClientBatchSize;

    /**
     * Constructs a {@link SemanticHighlighter} from the given builder.
     *
     * @param builder the builder holding the highlighter settings
     */
    public SemanticHighlighter(SemanticHighlighterBuilder builder) {
        this.queries = builder.queries;
        this.inputFields = builder.inputFields;
        this.preTags = builder.preTags;
        this.postTags = builder.postTags;
        this.threshold = builder.threshold;
        this.highlightOnly = builder.highlightOnly;
        this.modelDeploymentID = builder.modelDeploymentID;
        this.maxClientBatchSize = builder.maxClientBatchSize;
    }

    /**
     * Returns the highlight type name, {@code Semantic}.
     *
     * @return the highlight type name
     */
    @Override
    public String highlightType() {
        return "Semantic";
    }

    /**
     * Returns the semantic highlight parameters as a map of parameter name to value.
     *
     * @return the highlight parameters
     */
    @Override
    public Map<String, String> getParams() {
        Map<String, String> params = new java.util.HashMap<>();
        if (this.queries != null) {
            params.put("queries", JsonUtils.toJson(this.queries));
        }
        if (this.inputFields != null) {
            params.put("input_fields", JsonUtils.toJson(this.inputFields));
        }
        if (this.preTags != null) {
            params.put("pre_tags", JsonUtils.toJson(this.preTags));
        }
        if (this.postTags != null) {
            params.put("post_tags", JsonUtils.toJson(this.postTags));
        }
        if (this.threshold != null) {
            params.put("threshold", this.threshold.toString());
        }
        if (this.highlightOnly != null) {
            params.put("highlight_only", this.highlightOnly.toString());
        }
        if (this.modelDeploymentID != null) {
            params.put("model_deployment_id", this.modelDeploymentID);
        }
        if (this.maxClientBatchSize != null) {
            params.put("max_client_batch_size", this.maxClientBatchSize.toString());
        }
        return params;
    }

    /**
     * Builder for {@link SemanticHighlighter}.
     */
    public static class SemanticHighlighterBuilder {
        private List<String> queries;
        private List<String> inputFields;
        private List<String> preTags;
        private List<String> postTags;
        private Float threshold;
        private Boolean highlightOnly;
        private String modelDeploymentID;
        private Integer maxClientBatchSize;

        public SemanticHighlighterBuilder() {
        }

        /**
         * Sets the queries whose semantically relevant passages should be highlighted.
         *
         * @param queries the queries
         * @return this builder
         */
        public SemanticHighlighterBuilder queries(List<String> queries) {
            this.queries = queries;
            return this;
        }

        /**
         * Adds a single query to the highlighter.
         *
         * @param q the query text
         * @return this builder
         */
        public SemanticHighlighterBuilder addQuery(String q) {
            if (this.queries == null) this.queries = new ArrayList<>();
            this.queries.add(q);
            return this;
        }

        /**
         * Sets the text fields that should be searched for semantically relevant passages.
         *
         * @param inputFields the input fields
         * @return this builder
         */
        public SemanticHighlighterBuilder inputFields(List<String> inputFields) {
            this.inputFields = inputFields;
            return this;
        }

        /**
         * Adds a single input field to the highlighter.
         *
         * @param f the input field name
         * @return this builder
         */
        public SemanticHighlighterBuilder addInputField(String f) {
            if (this.inputFields == null) this.inputFields = new ArrayList<>();
            this.inputFields.add(f);
            return this;
        }

        /**
         * Sets the tags prepended to the highlighted passages.
         *
         * @param preTags the pre-tags
         * @return this builder
         */
        public SemanticHighlighterBuilder preTags(List<String> preTags) {
            this.preTags = preTags;
            return this;
        }

        /**
         * Adds a single pre-tag to the highlighter.
         *
         * @param tag the pre-tag
         * @return this builder
         */
        public SemanticHighlighterBuilder addPreTag(String tag) {
            if (this.preTags == null) this.preTags = new ArrayList<>();
            this.preTags.add(tag);
            return this;
        }

        /**
         * Sets the tags appended to the highlighted passages.
         *
         * @param postTags the post-tags
         * @return this builder
         */
        public SemanticHighlighterBuilder postTags(List<String> postTags) {
            this.postTags = postTags;
            return this;
        }

        /**
         * Adds a single post-tag to the highlighter.
         *
         * @param tag the post-tag
         * @return this builder
         */
        public SemanticHighlighterBuilder addPostTag(String tag) {
            if (this.postTags == null) this.postTags = new ArrayList<>();
            this.postTags.add(tag);
            return this;
        }

        /**
         * Sets the relevance threshold above which a passage is highlighted.
         *
         * @param threshold the relevance threshold
         * @return this builder
         */
        public SemanticHighlighterBuilder threshold(Float threshold) {
            this.threshold = threshold;
            return this;
        }

        /**
         * Sets whether only the highlighted passages are returned instead of the full text.
         *
         * @param highlightOnly {@code true} to return only highlighted passages
         * @return this builder
         */
        public SemanticHighlighterBuilder highlightOnly(Boolean highlightOnly) {
            this.highlightOnly = highlightOnly;
            return this;
        }

        /**
         * Sets the model deployment ID of the embedding model used for highlighting.
         *
         * @param modelDeploymentID the model deployment ID
         * @return this builder
         */
        public SemanticHighlighterBuilder modelDeploymentID(String modelDeploymentID) {
            this.modelDeploymentID = modelDeploymentID;
            return this;
        }

        /**
         * Sets the maximum number of queries sent to the model per batch.
         *
         * @param size the maximum batch size
         * @return this builder
         */
        public SemanticHighlighterBuilder maxClientBatchSize(Integer size) {
            this.maxClientBatchSize = size;
            return this;
        }

        /**
         * Builds the {@link SemanticHighlighter}.
         *
         * @return the built highlighter
         */
        public SemanticHighlighter build() {
            return new SemanticHighlighter(this);
        }
    }

    /**
     * Creates a new builder for {@link SemanticHighlighter}.
     *
     * @return a new builder
     */
    public static SemanticHighlighterBuilder builder() {
        return new SemanticHighlighterBuilder();
    }
}
