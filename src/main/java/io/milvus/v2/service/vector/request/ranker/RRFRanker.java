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

import java.util.HashMap;
import java.util.Map;

/**
 * The RRF reranking strategy, which merges results from multiple searches, favoring items that consistently appear.
 */
public class RRFRanker extends BaseRanker {
    private int k = 60;

    public RRFRanker(int k) {
        this.k = k;
    }

    @Override
    public Map<String, String> getProperties() {
        JsonObject params = new JsonObject();
        params.addProperty("k", this.k);

        Map<String, String> props = new HashMap<>();
        props.put("strategy", "rrf");
        props.put("params", params.toString());
        return props;
    }
}
