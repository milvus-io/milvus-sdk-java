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

import org.apache.commons.lang3.builder.EqualsBuilder;

public class ResourceGroupLimit {
    private Integer nodeNum;

    /**
     * Constructor with node number.
     * 
     * @param nodeNum query node number in this group
     */
    public ResourceGroupLimit(Integer nodeNum) {
        if (nodeNum == null) {
            throw new IllegalArgumentException("Node number cannot be null");
        }
        this.nodeNum = nodeNum;
    }

    /**
     * Constructor from grpc
     * 
     * @param grpcLimit grpc object to set limit of node number
     */
    public ResourceGroupLimit(io.milvus.grpc.ResourceGroupLimit grpcLimit) {
        if (grpcLimit == null) {
            throw new IllegalArgumentException("GRPC limit cannot be null");
        }
        this.nodeNum = grpcLimit.getNodeNum();
    }

    /**
     * Transfer to grpc
     * 
     * @return <code>io.milvus.grpc.ResourceGroupLimit</code>
     */
    public io.milvus.grpc.ResourceGroupLimit toGRPC() {
        io.milvus.grpc.ResourceGroupLimit result = io.milvus.grpc.ResourceGroupLimit.newBuilder().setNodeNum(nodeNum).build();
        if (result == null) {
            throw new IllegalStateException("Failed to create GRPC ResourceGroupLimit");
        }
        return result;
    }

    public Integer getNodeNum() {
        return nodeNum;
    }

    @Override
    public String toString() {
        return "ResourceGroupLimit{" +
                "nodeNum=" + nodeNum +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ResourceGroupLimit that = (ResourceGroupLimit) obj;
        return new EqualsBuilder()
                .append(nodeNum, that.nodeNum)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return nodeNum != null ? nodeNum.hashCode() : 0;
    }
}
