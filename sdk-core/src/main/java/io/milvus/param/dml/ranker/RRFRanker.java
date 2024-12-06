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

import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * The RRF reranking strategy, which merges results from multiple searches, favoring items that consistently appear.
 */
@Getter
@ToString
public class RRFRanker extends BaseRanker {
    private final Integer k;

    private RRFRanker(@NonNull Builder builder) {
        this.k = builder.k;
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

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link RRFRanker} class.
     */
    public static class Builder {
        private Integer k = 60;

        private Builder() {
        }

        /**
         * Sets k factor for RRF. Value cannot be negative. Default value is 60.
         * score = 1 / (k + float32(rank_i+1))
         * rank_i is the rank in each field
         *
         * @param k factor value
         * @return <code>Builder</code>
         */
        public Builder withK(@NonNull Integer k) {
            this.k = k;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link RRFRanker} instance.
         *
         * @return {@link RRFRanker}
         */
        public RRFRanker build() throws ParamException {
            if (k < 0) {
                throw new ParamException("K value cannot be negative");
            }
            return new RRFRanker(this);
        }
    }
}
