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
import lombok.Builder;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The Model reranking strategy, which transforms Milvus search by integrating advanced language models
 * that understand semantic relationships between queries and documents.
 * Read the doc for more info: https://milvus.io/docs/model-ranker-overview.md
 *
 * You also can declare a model ranker by Function
 * CreateCollectionReq.Function rr = CreateCollectionReq.Function.builder()
 *                 .functionType(FunctionType.RERANK)
 *                 .name("semantic_ranker")
 *                 .description("semantic ranker")
 *                 .inputFieldNames(Collections.singletonList("document"))
 *                 .param("reranker", "model")
 *                 .param("provider", "tei")
 *                 .param("queries", "[\"machine learning for time series\"]")
 *                 .param("endpoint", "http://model-service:8080")
 *                 .build();
 */
@SuperBuilder
public class ModelRanker extends CreateCollectionReq.Function {
    @Builder.Default
    private String provider = "tei";
    @Builder.Default
    private List<String> queries = new ArrayList<>();
    private String endpoint;

    public FunctionType getFunctionType() {
        return FunctionType.RERANK;
    }

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
}
