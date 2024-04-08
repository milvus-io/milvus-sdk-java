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

package io.milvus.param.dml.ranker;

import com.alibaba.fastjson.JSONObject;
import io.milvus.exception.ParamException;

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.*;

/**
 * The Average Weighted Scoring reranking strategy, which prioritizes vectors based on relevance,
 * averaging their significance.
 */
@Getter
@ToString
public class WeightedRanker extends BaseRanker {
    private final List<Float> weights;

    private WeightedRanker(@NonNull Builder builder) {
        this.weights = builder.weights;
    }

    @Override
    public Map<String, String> getProperties() {
        JSONObject params = new JSONObject();
        params.put("weights", this.weights);

        Map<String, String> props = new HashMap<>();
        props.put("strategy", "weighted");
        props.put("params", params.toString());
        return props;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link WeightedRanker} class.
     */
    public static class Builder {
        private List<Float> weights = new ArrayList<>();

        private Builder() {
        }

        /**
         * Assign weights for each AnnSearchParam. The length of weights must be equal to number of AnnSearchParam.
         * You can assign any float value for weight, the sum of weight values can exceed 1.
         * The distance/similarity values of each field will be mapped into a range of [0,1],
         * and score = sum(weights[i] * distance_i_in_[0,1])
         *
         * @param weights weight values
         * @return <code>Builder</code>
         */
        public Builder withWeights(@NonNull List<Float> weights) {
            this.weights = weights;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link WeightedRanker} instance.
         *
         * @return {@link WeightedRanker}
         */
        public WeightedRanker build() throws ParamException {
            return new WeightedRanker(this);
        }
    }
}
