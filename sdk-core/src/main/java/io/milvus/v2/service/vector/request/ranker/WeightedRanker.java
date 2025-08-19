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
import io.milvus.common.utils.JsonUtils;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq.Function;

import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The Average Weighted Scoring reranking strategy, which prioritizes vectors based on relevance,
 * averaging their significance.
 * Read the doc for more info: https://milvus.io/docs/weighted-ranker.md
 *
 * Note: In v2.6, the Function and Rerank have been unified to support more rerank types: decay and model ranker
 * https://milvus.io/docs/decay-ranker-overview.md
 * https://milvus.io/docs/model-ranker-overview.md
 * So we have to inherit the BaseRanker from Function, this change will lead to uncomfortable issues with
 * RRFRanker/WeightedRanker in some users client code. We will mention it in release note.
 * In old client code, to declare a WeightedRanker:
 *   WeightedRanker ranker = new WeightedRanker(Arrays.asList(0.2f, 0.5f, 0.6f))
 * After this change, the client code should be changed accordingly:
 *   WeightedRanker ranker = WeightedRanker.builder().weights(Arrays.asList(0.2f, 0.5f, 0.6f)).build()
 *
 * You also can declare a weighter ranker by Function
 * CreateCollectionReq.Function rr = CreateCollectionReq.Function.builder()
 *                 .functionType(FunctionType.RERANK)
 *                 .param("strategy", "weighted")
 *                 .param("params", "{\"weights\": [0.4, 0.6]}")
 *                 .build();
 */
public class WeightedRanker extends CreateCollectionReq.Function {
    private List<Float> weights;

    // This constructor is to compatible with the old client code like:
    //  new WeightedRanker(weights)
    // Now it is deprecated, user should create a WeightedRanker by builder style:
    //  WeightedRanker.builder().weights(weights).build()
    @Deprecated
    public WeightedRanker(List<Float> weights) {
        super(CreateCollectionReq.Function.builder());
        this.weights = weights;
    }

    private WeightedRanker(FunctionBuilder builder) {
        super(builder);
        this.weights = builder.weights;
    }

    public List<Float> getWeights() {
        return weights;
    }

    public void setWeights(List<Float> weights) {
        this.weights = weights;
    }

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.RERANK;
    }

    @Override
    public Map<String, String> getParams() {
        JsonObject params = new JsonObject();
        params.add("weights", JsonUtils.toJsonTree(this.weights).getAsJsonArray());

        Map<String, String> props = super.getParams();
        props.put("strategy", "weighted");
        props.put("params", params.toString());
        return props;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (!super.equals(obj)) return false;
        WeightedRanker that = (WeightedRanker) obj;
        return new EqualsBuilder()
                .append(weights, that.weights)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (weights != null ? weights.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "WeightedRanker{" +
                "weights=" + weights +
                ", name='" + getName() + '\'' +
                ", description='" + getDescription() + '\'' +
                ", functionType=" + getFunctionType() +
                ", inputFieldNames=" + getInputFieldNames() +
                ", outputFieldNames=" + getOutputFieldNames() +
                ", params=" + getParams() +
                '}';
    }

    public static FunctionBuilder builder() {
        return new FunctionBuilder();
    }

    public static class FunctionBuilder extends Function.FunctionBuilder {
        private List<Float> weights = new ArrayList<>();

        private FunctionBuilder() {}

        public FunctionBuilder weights(List<Float> weights) {
            this.weights = weights;
            return this;
        }

        @Override
        public FunctionBuilder name(String name) {
            super.name(name);
            return this;
        }

        @Override
        public FunctionBuilder description(String description) {
            super.description(description);
            return this;
        }

        @Override
        public FunctionBuilder functionType(io.milvus.common.clientenum.FunctionType functionType) {
            super.functionType(functionType);
            return this;
        }

        @Override
        public FunctionBuilder inputFieldNames(java.util.List<String> inputFieldNames) {
            super.inputFieldNames(inputFieldNames);
            return this;
        }

        @Override
        public FunctionBuilder outputFieldNames(java.util.List<String> outputFieldNames) {
            super.outputFieldNames(outputFieldNames);
            return this;
        }

        @Override
        public FunctionBuilder params(java.util.Map<String, String> params) {
            super.params(params);
            return this;
        }

        @Override
        public FunctionBuilder param(String key, String value) {
            super.param(key, value);
            return this;
        }

        public WeightedRanker build() {
            return new WeightedRanker(this);
        }
    }
}
