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
import io.milvus.common.utils.JsonUtils;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * The Decay reranking strategy, which by adjusting search rankings based on numeric field values.
 * Read the doc for more info: https://milvus.io/docs/decay-ranker-overview.md
 *
 * Example:
 * BoostRanker boost = BoostRanker.builder()
 *                  .name("xxx_boost")
 *                  .description("boost on xxx")
 *                  .filter("xxx == 2")
 *                  .weight(0.5)
 *                  .randomScoreSeed(123)
 *                  .randomScoreField("id")
 *                  .build()
 *
 * You also can declare a decay ranker by Function
 * CreateCollectionReq.Function boost = CreateCollectionReq.Function.builder()
 *                  .functionType(FunctionType.RERANK)
 *                  .name("xxx_boost")
 *                  .description("boost on xxx")
 *                  .param("reranker", "boost")
 *                  .param("filter", "xxx == 2")
 *                  .param("weight", "0.5")
 *                  .param("random_score", "{\"seed\": 123, \"field\": \"id\"}")
 *                  .build();
 */
@SuperBuilder
public class BoostRanker extends CreateCollectionReq.Function {
    private String filter;
    private Float weight;
    private Long randomScoreSeed;
    private String randomScoreField;

    public FunctionType getFunctionType() {
        return FunctionType.RERANK;
    }

    public Map<String, String> getParams() {
        Map<String, String> props = super.getParams();
        props.put("reranker", "boost");
        if (!StringUtils.isEmpty(filter)) {
            props.put("filter", filter);
        }
        if (weight != null) {
            props.put("weight", weight.toString());
        }

        Map<String, Object> randomScore = new HashMap<>();
        if (randomScoreSeed != null) {
            randomScore.put("seed", randomScoreSeed);
        }
        if (!StringUtils.isEmpty(randomScoreField)) {
            randomScore.put("field", randomScoreField);
        }
        if (!randomScore.isEmpty()) {
            props.put("random_score", JsonUtils.toJson(randomScore));
        }
        return props;
    }
}
