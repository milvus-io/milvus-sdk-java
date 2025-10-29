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
import io.milvus.v2.service.collection.request.CreateCollectionReq.Function;

import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.List;
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
public class DecayRanker extends CreateCollectionReq.Function {
    private String function;
    private Number origin;
    private Number offset;
    private Number scale;
    private Number decay;

    private DecayRanker(FunctionBuilder builder) {
        super(builder);
        this.function = builder.function;
        this.origin = builder.origin;
        this.scale = builder.scale;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }

    public Number getOrigin() {
        return origin;
    }

    public void setOrigin(Number origin) {
        this.origin = origin;
    }

    public Number getScale() {
        return scale;
    }

    public void setScale(Number scale) {
        this.scale = scale;
    }

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.RERANK;
    }

    @Override
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (!super.equals(obj)) return false;
        DecayRanker that = (DecayRanker) obj;
        return new EqualsBuilder()
                .append(function, that.function)
                .append(origin, that.origin)
                .append(scale, that.scale)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (function != null ? function.hashCode() : 0);
        result = 31 * result + (origin != null ? origin.hashCode() : 0);
        result = 31 * result + (scale != null ? scale.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DecayRanker{" +
                "function='" + function + '\'' +
                ", origin=" + origin +
                ", scale=" + scale +
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
        private String function = "gauss";
        private Number origin;
        private Number scale;

        private FunctionBuilder() {}

        public FunctionBuilder function(String function) {
            this.function = function;
            return this;
        }

        public FunctionBuilder origin(Number origin) {
            this.origin = origin;
            return this;
        }

        public FunctionBuilder scale(Number scale) {
            this.scale = scale;
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
        public FunctionBuilder inputFieldNames(List<String> inputFieldNames) {
            super.inputFieldNames(inputFieldNames);
            return this;
        }

        @Override
        public FunctionBuilder outputFieldNames(List<String> outputFieldNames) {
            super.outputFieldNames(outputFieldNames);
            return this;
        }

        @Override
        public FunctionBuilder params(Map<String, String> params) {
            super.params(params);
            return this;
        }

        @Override
        public FunctionBuilder param(String key, String value) {
            super.param(key, value);
            return this;
        }

        public DecayRanker build() {
            return new DecayRanker(this);
        }
    }
}
