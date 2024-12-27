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

package io.milvus.common.resourcegroup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.milvus.grpc.KeyValuePair;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class ResourceGroupNodeFilter {
    private final Map<String, String> nodeLabels;

    private ResourceGroupNodeFilter(Builder builder) {
        this.nodeLabels = builder.nodeLabels;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private Map<String, String> nodeLabels = new HashMap<>();
        private Builder() {
        }

        /**
         * Set the node label filter
         * @param key label name
         * @param value label value
         * @return <code>Builder</code>
         */
        public Builder withNodeLabel(@NonNull String key, @NonNull String value) {
            this.nodeLabels.put(key, value);
            return this;
        }

        public ResourceGroupNodeFilter build() {
            return new ResourceGroupNodeFilter(this);
        }
    }

    /**
     * Transfer to grpc
     * @return io.milvus.grpc.ResourceGroupNodeFilter
     */
    public @NonNull io.milvus.grpc.ResourceGroupNodeFilter toGRPC() {
        List<KeyValuePair> pair = ParamUtils.AssembleKvPair(nodeLabels);
        return io.milvus.grpc.ResourceGroupNodeFilter.newBuilder()
                .addAllNodeLabels(pair)
               .build();
    }

    /**
     * Constructor from grpc
     * @param filter grpc filter object
     */
    public ResourceGroupNodeFilter(io.milvus.grpc.ResourceGroupNodeFilter filter) {
        this.nodeLabels = filter.getNodeLabelsList().stream().collect(Collectors.toMap(KeyValuePair::getKey, KeyValuePair::getValue));
    }

}