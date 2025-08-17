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

public class NodeInfo {
    private Long nodeId;
    private String address;
    private String hostname;

    private NodeInfo(Builder builder) {
        this.nodeId = builder.nodeId;
        this.address = builder.address;
        this.hostname = builder.hostname;
    }

    public Long getNodeId() {
        return nodeId;
    }

    public void setNodeId(Long nodeId) {
        this.nodeId = nodeId;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        NodeInfo nodeInfo = (NodeInfo) obj;
        return new EqualsBuilder()
                .append(nodeId, nodeInfo.nodeId)
                .append(address, nodeInfo.address)
                .append(hostname, nodeInfo.hostname)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = nodeId != null ? nodeId.hashCode() : 0;
        result = 31 * result + (address != null ? address.hashCode() : 0);
        result = 31 * result + (hostname != null ? hostname.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "NodeInfo{" +
                "nodeId=" + nodeId +
                ", address='" + address + '\'' +
                ", hostname='" + hostname + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Long nodeId;
        private String address;
        private String hostname;

        private Builder() {}

        public Builder nodeId(Long nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder address(String address) {
            this.address = address;
            return this;
        }

        public Builder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public NodeInfo build() {
            return new NodeInfo(this);
        }
    }
}
