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

import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.List;
import lombok.NonNull;
import lombok.Getter;

@Getter
public class ResourceGroupConfig {
    private final ResourceGroupLimit requests;
    private final ResourceGroupLimit limits;
    private final List<ResourceGroupTransfer> from;
    private final List<ResourceGroupTransfer> to;
    private final ResourceGroupNodeFilter nodeFilter;

    private ResourceGroupConfig(Builder builder) {
        this.requests = builder.requests;
        this.limits = builder.limits;
        this.nodeFilter = builder.nodeFilter;

        if (null == builder.from) {
            this.from = new ArrayList<>();
        } else {
            this.from = builder.from;
        }

        if (null == builder.to) {
            this.to = new ArrayList<>();
        } else {
            this.to = builder.to;
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private ResourceGroupLimit requests;
        private ResourceGroupLimit limits;
        private List<ResourceGroupTransfer> from;
        private List<ResourceGroupTransfer> to;
        private ResourceGroupNodeFilter nodeFilter;

        private Builder() {
        }

        /**
         * Set the requests node num.
         *
         * @param requests requests node num in resource group, if node num is less than requests.nodeNum, it will be transfer from other resource group.
         * @return <code>Builder</code>
         */
        public Builder withRequests(@NonNull ResourceGroupLimit requests) {
            this.requests = requests;
            return this;
        }

        /**
         * Set the limited node num.
         *
         * @param limits limited node num in resource group, if node num is more than limits.nodeNum, it will be transfer to other resource group.
         * @return <code>Builder</code>
         */
        public Builder withLimits(@NonNull ResourceGroupLimit limits) {
            this.limits = limits;
            return this;
        }

        /**
         * Set the transfer from list.
         *
         * @param from missing node should be transfer from given resource group at high priority in repeated list.
         * @return <code>Builder</code>
         */
        public Builder withFrom(@NonNull List<ResourceGroupTransfer> from) {
            this.from = from;
            return this;
        }

        /**
         * Set the transfer to list.
         *
         * @param to redundant node should be transfer to given resource group at high priority in repeated list.
         * @return <code>Builder</code>
         */
        public Builder withTo(@NonNull List<ResourceGroupTransfer> to) {
            this.to = to;
            return this;
        }

        /**
         * Set the node filter.
         * @param nodeFilter if node filter set, resource group will prefer to accept node which match node filter.
         * @return <code>Builder</code>
         */

        public Builder withNodeFilter(@NonNull ResourceGroupNodeFilter nodeFilter) {
            this.nodeFilter = nodeFilter;
            return this;
        }

        public ResourceGroupConfig build() {
            return new ResourceGroupConfig(this);
        }
    }

    public ResourceGroupConfig(@NonNull io.milvus.grpc.ResourceGroupConfig grpcConfig) {
        this.requests = new ResourceGroupLimit(grpcConfig.getRequests());
        this.limits = new ResourceGroupLimit(grpcConfig.getLimits());
        this.from = grpcConfig.getTransferFromList().stream()
                .map(transfer -> new ResourceGroupTransfer(transfer))
                .collect(Collectors.toList());
        this.to = grpcConfig.getTransferToList().stream()
                .map(transfer -> new ResourceGroupTransfer(transfer))
                .collect(Collectors.toList());
        this.nodeFilter = new ResourceGroupNodeFilter(grpcConfig.getNodeFilter());
    }

    public @NonNull io.milvus.grpc.ResourceGroupConfig toGRPC() {
        io.milvus.grpc.ResourceGroupConfig.Builder builder = io.milvus.grpc.ResourceGroupConfig.newBuilder()
                .setRequests(io.milvus.grpc.ResourceGroupLimit.newBuilder().setNodeNum(requests.getNodeNum()))
                .setLimits(io.milvus.grpc.ResourceGroupLimit.newBuilder().setNodeNum(limits.getNodeNum()))
                .setNodeFilter(nodeFilter.toGRPC());
        for (ResourceGroupTransfer transfer : from) {
            builder.addTransferFrom(transfer.toGRPC());
        }
        for (ResourceGroupTransfer transfer : to) {
            builder.addTransferTo(transfer.toGRPC());
        }
        return builder.build();
    }
}
