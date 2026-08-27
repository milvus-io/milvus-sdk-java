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

package io.milvus.v2.service.vector.request.ranker;

import com.google.gson.JsonArray;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq.Function;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The Model reranking strategy, which transforms Milvus search by integrating advanced language models
 * that understand semantic relationships between queries and documents.
 * Read the doc for more info: https://milvus.io/docs/model-ranker-overview.md
 * <p>
 * You also can declare a model ranker by Function
 * CreateCollectionReq.Function rr = CreateCollectionReq.Function.builder()
 * .functionType(FunctionType.RERANK)
 * .name("semantic_ranker")
 * .description("semantic ranker")
 * .inputFieldNames(Collections.singletonList("document"))
 * .param("reranker", "model")
 * .param("provider", "tei")
 * .param("queries", "[\"machine learning for time series\"]")
 * .param("endpoint", "http://model-service:8080")
 * .build();
 */
public class ModelRanker extends CreateCollectionReq.Function {
    private String provider;
    private List<String> queries;
    private String endpoint;

    private ModelRanker(ModelRankerBuilder builder) {
        super(builder);
        this.provider = builder.provider;
        this.queries = builder.queries;
        this.endpoint = builder.endpoint;
    }

    /**
     * Returns the provider of the rerank model, for example {@code tei} or {@code vllm}.
     *
     * @return the model provider
     */
    public String getProvider() {
        return provider;
    }

    /**
     * Sets the provider of the rerank model, for example {@code tei} or {@code vllm}.
     *
     * @param provider the model provider
     */
    public void setProvider(String provider) {
        this.provider = provider;
    }

    /**
     * Returns the queries used by the model to compute relevance scores.
     *
     * @return the rerank queries
     */
    public List<String> getQueries() {
        return queries;
    }

    /**
     * Sets the queries used by the model to compute relevance scores.
     *
     * @param queries the rerank queries
     */
    public void setQueries(List<String> queries) {
        this.queries = queries;
    }

    /**
     * Returns the endpoint of the deployed rerank model service.
     *
     * @return the model endpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Sets the endpoint of the deployed rerank model service.
     *
     * @param endpoint the model endpoint
     */
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Returns the function type of this ranker.
     *
     * @return {@link FunctionType#RERANK}
     */
    @Override
    public FunctionType getFunctionType() {
        return FunctionType.RERANK;
    }

    /**
     * Returns the ranker parameters as a map of parameter name to value, including the
     * provider, queries, and endpoint.
     *
     * @return the ranker parameters
     */
    @Override
    public Map<String, String> getParams() {
        // the parent params might contain "offset" and "decay"
        Map<String, String> props = super.getParams();
        props.put("reranker", "model");
        props.put("provider", provider); // "tei" or "vllm"
        JsonArray json = new JsonArray();
        queries.forEach(json::add);
        props.put("queries", json.toString());
        if (endpoint != null) {
            props.put("endpoint", endpoint);
        }
        return props;
    }

    @Override
    public String toString() {
        return "ModelRanker{" +
                "provider='" + provider + '\'' +
                ", queries=" + queries +
                ", endpoint='" + endpoint + '\'' +
                ", name='" + getName() + '\'' +
                ", description='" + getDescription() + '\'' +
                ", functionType=" + getFunctionType() +
                ", inputFieldNames=" + getInputFieldNames() +
                ", outputFieldNames=" + getOutputFieldNames() +
                ", params=" + getParams() +
                '}';
    }

    /**
     * Creates a new builder for {@link ModelRanker}.
     *
     * @return a new builder
     */
    public static ModelRankerBuilder builder() {
        return new ModelRankerBuilder();
    }

    /**
     * Builder for {@link ModelRanker}.
     */
    public static class ModelRankerBuilder extends Function.FunctionBuilder<ModelRankerBuilder> {
        private String provider = "tei";
        private List<String> queries = new ArrayList<>();
        private String endpoint;

        private ModelRankerBuilder() {
        }

        /**
         * Sets the provider of the rerank model, for example {@code tei} or {@code vllm}.
         * Defaults to {@code tei}.
         *
         * @param provider the model provider
         * @return this builder
         */
        public ModelRankerBuilder provider(String provider) {
            this.provider = provider;
            return this;
        }

        /**
         * Sets the queries used by the model to compute relevance scores.
         *
         * @param queries the rerank queries
         * @return this builder
         */
        public ModelRankerBuilder queries(List<String> queries) {
            this.queries = queries;
            return this;
        }

        /**
         * Sets the endpoint of the deployed rerank model service.
         *
         * @param endpoint the model endpoint
         * @return this builder
         */
        public ModelRankerBuilder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        /**
         * Builds the {@link ModelRanker}.
         *
         * @return the built ranker
         */
        @Override
        public ModelRanker build() {
            return new ModelRanker(this);
        }
    }
}
