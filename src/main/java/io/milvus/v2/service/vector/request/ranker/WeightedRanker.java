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
import io.milvus.common.utils.JsonUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Average Weighted Scoring reranking strategy, which prioritizes vectors based on relevance,
 * averaging their significance.
 */
public class WeightedRanker extends BaseRanker {
    private List<Float> weights;

    public WeightedRanker(List<Float> weights) {
        this.weights = weights;
    }

    @Override
    public Map<String, String> getProperties() {
        JsonObject params = new JsonObject();
        params.add("weights", JsonUtils.toJsonTree(this.weights).getAsJsonArray());

        Map<String, String> props = new HashMap<>();
        props.put("strategy", "weighted");
        props.put("params", params.toString());
        return props;
    }
}
