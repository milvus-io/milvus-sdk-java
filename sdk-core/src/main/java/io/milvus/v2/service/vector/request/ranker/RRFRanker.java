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

import com.google.gson.JsonObject;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import lombok.Builder;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * The RRF reranking strategy, which merges results from multiple searches, favoring items that consistently appear.
 * Read the doc for more info: https://milvus.io/docs/rrf-ranker.md
 *
 * Note: In v2.6, the Function and Rerank have been unified to support more rerank types: decay and model ranker
 * https://milvus.io/docs/decay-ranker-overview.md
 * https://milvus.io/docs/model-ranker-overview.md
 * So we have to inherit the BaseRanker from Function, this change will lead to uncomfortable issues with
 * RRFRanker/WeightedRanker in some users client code. We will mention it in release note.
 *  * In old client code, to declare a WeightedRanker:
 *  *   RRFRanker ranker = new RRFRanker(20)
 *  * After this change, the client code should be changed accordingly:
 *  *   RRFRanker ranker = RRFRanker.builder().k(20).build()
 *
 * You also can declare a rrf ranker by Function
 * CreateCollectionReq.Function rr = CreateCollectionReq.Function.builder()
 *                 .functionType(FunctionType.RERANK)
 *                 .param("strategy", "rrf")
 *                 .param("params", "{\"k\": 60}")
 *                 .build();
 */
@SuperBuilder
public class RRFRanker extends CreateCollectionReq.Function {
    // This constructor is to compatible with the old client code like:
    //  new RRFRanker(10)
    // Now it is deprecated, user should create a RRFRanker by builder style:
    //  RRFRanker.builder().k(10).build()
    @Deprecated
    public RRFRanker(int k) {
        super(CreateCollectionReq.Function.builder());
        this.k = k;
    }

    @Builder.Default
    private int k = 60;

    public FunctionType getFunctionType() {
        return FunctionType.RERANK;
    }

    public Map<String, String> getParams() {
        JsonObject params = new JsonObject();
        params.addProperty("k", this.k);

        Map<String, String> props = super.getParams();
        props.put("strategy", "rrf");
        props.put("params", params.toString());
        return props;
    }
}
