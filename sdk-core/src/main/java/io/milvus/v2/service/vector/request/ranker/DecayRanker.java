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

import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import lombok.Builder;
import lombok.experimental.SuperBuilder;

import java.util.Map;

/**
 * The Decay reranking strategy, which by adjusting search rankings based on numeric field values.
 * Read the doc for more info: https://milvus.io/docs/decay-ranker-overview.md
 *
 * Example:
 * DecayRanker decay = DecayRanker.builder()
 *                  .name("time_decay")
 *                  .description("time decay")
 *                  .inputFieldNames(Collections.singletonList("timestamp"))
 *                  .function("gauss")
 *                  .origin(100)
 *                  .scale(50)
 *                  .offset(24)
 *                  .decay(0.5)
 *                  .build()
 *
 * You also can declare a decay ranker by Function:
 * CreateCollectionReq.Function decay = CreateCollectionReq.Function.builder()
 *                 .functionType(FunctionType.RERANK)
 *                 .name("time_decay")
 *                 .description("time decay")
 *                 .inputFieldNames(Collections.singletonList("timestamp"))
 *                 .param("reranker", "decay")
 *                 .param("function", "gauss")
 *                 .param("origin", "100")
 *                 .param("scale", "50")
 *                 .param("offset", "24")
 *                 .param("decay", "0.5")
 *                 .build();
 */
@SuperBuilder
public class DecayRanker extends CreateCollectionReq.Function {
    @Builder.Default
    private String function = "gauss";
    private Number origin;
    private Number offset;
    private Number scale;
    private Number decay;

    public FunctionType getFunctionType() {
        return FunctionType.RERANK;
    }

    public Map<String, String> getParams() {
        // the parent params might contain "offset" and "decay"
        Map<String, String> props = super.getParams();
        props.put("reranker", "decay");
        props.put("function", function); // "gauss", "exp", or "linear"
        if (origin != null) {
            props.put("origin", origin.toString());
        }
        if (offset != null) {
            props.put("offset", offset.toString());
        }
        if (scale != null) {
            props.put("scale", scale.toString());
        }
        if (decay != null) {
            props.put("decay", decay.toString());
        }
        return props;
    }
}
