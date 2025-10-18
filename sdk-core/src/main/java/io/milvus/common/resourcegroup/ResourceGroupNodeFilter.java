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
import java.util.Objects;
import java.util.stream.Collectors;

import io.milvus.grpc.KeyValuePair;
import io.milvus.param.ParamUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;

public class ResourceGroupNodeFilter {
    private final Map<String, String> nodeLabels;

    private ResourceGroupNodeFilter(Builder builder) {
        this.nodeLabels = builder.nodeLabels;
    }

    /**
     * Constructor from grpc
     * @param filter grpc filter object
     */
    public ResourceGroupNodeFilter(io.milvus.grpc.ResourceGroupNodeFilter filter) {
        if (filter == null) {
            throw new IllegalArgumentException("filter cannot be null");
        }
        this.nodeLabels = filter.getNodeLabelsList().stream().collect(Collectors.toMap(KeyValuePair::getKey, KeyValuePair::getValue));
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Create ResourceGroupNodeFilter from grpc object
     * @param filter grpc filter object
     * @return ResourceGroupNodeFilter instance
     */
    public static ResourceGroupNodeFilter fromGRPC(io.milvus.grpc.ResourceGroupNodeFilter filter) {
        return new ResourceGroupNodeFilter(filter);
    }

    // Getter method to replace @Getter annotation
    public Map<String, String> getNodeLabels() {
        return nodeLabels;
    }

    public static class Builder {
        private Map<String, String> nodeLabels = new HashMap<>();
        
        private Builder() {
        }

        /**
         * Set the node label filter
         * @param key label name
         * @param value label value
         * @return <code>Builder</code>
         */
        public Builder withNodeLabel(String key, String value) {
            // Replace @NonNull logic with explicit null checks
            if (key == null) {
                throw new IllegalArgumentException("key cannot be null");
            }
            if (value == null) {
                throw new IllegalArgumentException("value cannot be null");
            }
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
    public io.milvus.grpc.ResourceGroupNodeFilter toGRPC() {
        List<KeyValuePair> pair = ParamUtils.AssembleKvPair(nodeLabels);
        io.milvus.grpc.ResourceGroupNodeFilter result = io.milvus.grpc.ResourceGroupNodeFilter.newBuilder()
                .addAllNodeLabels(pair)
                .build();
        
        // Replace @NonNull logic with explicit null check
        if (result == null) {
            throw new IllegalStateException("Failed to create GRPC ResourceGroupNodeFilter");
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ResourceGroupNodeFilter that = (ResourceGroupNodeFilter) obj;
        return new EqualsBuilder()
                .append(nodeLabels, that.nodeLabels)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeLabels);
    }

    @Override
    public String toString() {
        return "ResourceGroupNodeFilter{" +
                "nodeLabels=" + nodeLabels +
                '}';
    }


}
