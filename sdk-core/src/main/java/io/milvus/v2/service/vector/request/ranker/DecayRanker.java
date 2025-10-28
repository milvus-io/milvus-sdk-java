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

import java.util.Map;

/**
 * The Decay reranking strategy, which by adjusting search rankings based on numeric field values.
 * Read the doc for more info: https://milvus.io/docs/decay-ranker-overview.md
 * <p>
 * Example:
 * DecayRanker decay = DecayRanker.builder()
 * .name("time_decay")
 * .description("time decay")
 * .inputFieldNames(Collections.singletonList("timestamp"))
 * .function("gauss")
 * .origin(100)
 * .scale(50)
 * .offset(24)
 * .decay(0.5)
 * .build()
 * <p>
 * You also can declare a decay ranker by Function:
 * CreateCollectionReq.Function decay = CreateCollectionReq.Function.builder()
 * .functionType(FunctionType.RERANK)
 * .name("time_decay")
 * .description("time decay")
 * .inputFieldNames(Collections.singletonList("timestamp"))
 * .param("reranker", "decay")
 * .param("function", "gauss")
 * .param("origin", "100")
 * .param("scale", "50")
 * .param("offset", "24")
 * .param("decay", "0.5")
 * .build();
 */
public class DecayRanker extends CreateCollectionReq.Function {
    private String function;
    private Number origin;
    private Number offset;
    private Number scale;
    private Number decay;

    private DecayRanker(DecayRankerBuilder builder) {
        super(builder);
        this.function = builder.function;
        this.origin = builder.origin;
        this.offset = builder.offset;
        this.scale = builder.scale;
        this.decay = builder.decay;
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

    public Number getOffset() {
        return offset;
    }

    public void setOffset(Number offset) {
        this.offset = offset;
    }

    public Number getScale() {
        return scale;
    }

    public void setScale(Number scale) {
        this.scale = scale;
    }

    public Number getDecay() {
        return decay;
    }

    public void setDecay(Number decay) {
        this.decay = decay;
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
    public String toString() {
        return "DecayRanker{" +
                "function='" + function + '\'' +
                ", origin=" + origin +
                ", offset=" + offset +
                ", scale=" + scale +
                ", decay=" + decay +
                ", name='" + getName() + '\'' +
                ", description='" + getDescription() + '\'' +
                ", functionType=" + getFunctionType() +
                ", inputFieldNames=" + getInputFieldNames() +
                ", outputFieldNames=" + getOutputFieldNames() +
                ", params=" + getParams() +
                '}';
    }

    public static DecayRankerBuilder builder() {
        return new DecayRankerBuilder();
    }

    public static class DecayRankerBuilder extends Function.FunctionBuilder<DecayRankerBuilder> {
        private String function = "gauss";
        private Number origin;
        private Number offset;
        private Number scale;
        private Number decay;

        private DecayRankerBuilder() {
        }

        public DecayRankerBuilder function(String function) {
            this.function = function;
            return this;
        }

        public DecayRankerBuilder origin(Number origin) {
            this.origin = origin;
            return this;
        }

        public DecayRankerBuilder offset(Number offset) {
            this.offset = offset;
            return this;
        }

        public DecayRankerBuilder scale(Number scale) {
            this.scale = scale;
            return this;
        }

        public DecayRankerBuilder decay(Number decay) {
            this.decay = decay;
            return this;
        }

        @Override
        public DecayRanker build() {
            return new DecayRanker(this);
        }
    }
}
