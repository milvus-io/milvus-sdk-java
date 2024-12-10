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

package io.milvus.param.resourcegroup;

import io.milvus.common.resourcegroup.ResourceGroupConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.NonNull;

@Getter
public class UpdateResourceGroupsParam {
    private final Map<String, ResourceGroupConfig> resourceGroups;

    private UpdateResourceGroupsParam(Builder builder) {
        if (null == builder.resourceGroups || builder.resourceGroups.isEmpty()) {
            throw new IllegalArgumentException("resourceGroups cannot be empty");
        }
        this.resourceGroups = builder.resourceGroups;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link UpdateResourceGroupsParam} class.
     * 
     */
    public static final class Builder {
        private Map<String, ResourceGroupConfig> resourceGroups;

        private Builder() {
        }

        public Builder putResourceGroup(@NonNull String resourceGroupName, @NonNull ResourceGroupConfig resourceGroup) {
            if (null == this.resourceGroups) {
                this.resourceGroups = new HashMap<>();
            }
            this.resourceGroups.put(resourceGroupName, resourceGroup);
            return this;
        }

        /**
         * Builds the UpdateResourceGroupsParam object.
         * 
         * @return {@link UpdateResourceGroupsParam}
         */
        public UpdateResourceGroupsParam build() {
            return new UpdateResourceGroupsParam(this);
        }
    }

    /**
     * Converts to grpc request.
     * 
     * @return io.milvus.grpc.UpdateResourceGroupsRequest
     */
    public @NonNull io.milvus.grpc.UpdateResourceGroupsRequest toGRPC() {
        io.milvus.grpc.UpdateResourceGroupsRequest.Builder builder = io.milvus.grpc.UpdateResourceGroupsRequest
                .newBuilder();
        resourceGroups.forEach((k, v) -> {
            builder.putResourceGroups(k, v.toGRPC());
        });
        return builder.build();
    }

    /**
     * Constructs a <code>String</code> by {@link UpdateResourceGroupsParam}
     * instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return String.format("UpdateResourceGroupsRequest{resourceGroupNames:%s}",
                resourceGroups.keySet().stream().collect(Collectors.joining(",")));
    }
}
