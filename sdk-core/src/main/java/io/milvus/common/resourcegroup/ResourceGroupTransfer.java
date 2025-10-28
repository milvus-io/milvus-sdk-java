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

public class ResourceGroupTransfer {
    private final String resourceGroupName;

    /**
     * Constructor with resource group name.
     *
     * @param resourceGroupName resource group name
     */
    public ResourceGroupTransfer(String resourceGroupName) {
        if (resourceGroupName == null) {
            throw new IllegalArgumentException("resourceGroupName cannot be null");
        }
        this.resourceGroupName = resourceGroupName;
    }

    /**
     * Constructor from grpc
     *
     * @param grpcTransfer grpc transfer object
     */
    public ResourceGroupTransfer(io.milvus.grpc.ResourceGroupTransfer grpcTransfer) {
        if (grpcTransfer == null) {
            throw new IllegalArgumentException("grpcTransfer cannot be null");
        }
        this.resourceGroupName = grpcTransfer.getResourceGroup();
    }

    /**
     * Get resource group name
     *
     * @return resource group name
     */
    public String getResourceGroupName() {
        return resourceGroupName;
    }

    /**
     * Transfer to grpc
     *
     * @return io.milvus.grpc.ResourceGroupTransfer
     */
    public io.milvus.grpc.ResourceGroupTransfer toGRPC() {
        io.milvus.grpc.ResourceGroupTransfer result = io.milvus.grpc.ResourceGroupTransfer.newBuilder()
                .setResourceGroup(resourceGroupName)
                .build();

        if (result == null) {
            throw new IllegalStateException("Failed to create GRPC ResourceGroupTransfer");
        }
        return result;
    }
}
