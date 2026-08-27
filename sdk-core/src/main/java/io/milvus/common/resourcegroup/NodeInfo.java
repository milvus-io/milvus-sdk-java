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

/**
 * Describes a Milvus query node in a resource group, including its node ID, address and hostname.
 *
 * <p>This class is used to represent the nodes transferred between resource groups, for example in
 * the {@code transferNode} operation.
 */
public class NodeInfo {
    private Long nodeId;
    private String address;
    private String hostname;

    private NodeInfo(Builder builder) {
        this.nodeId = builder.nodeId;
        this.address = builder.address;
        this.hostname = builder.hostname;
    }

    /**
     * Returns the ID of the query node.
     *
     * @return the node ID
     */
    public Long getNodeId() {
        return nodeId;
    }

    /**
     * Sets the ID of the query node.
     *
     * @param nodeId the node ID
     */
    public void setNodeId(Long nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Returns the address of the query node.
     *
     * @return the node address
     */
    public String getAddress() {
        return address;
    }

    /**
     * Sets the address of the query node.
     *
     * @param address the node address
     */
    public void setAddress(String address) {
        this.address = address;
    }

    /**
     * Returns the hostname of the query node.
     *
     * @return the node hostname
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * Sets the hostname of the query node.
     *
     * @param hostname the node hostname
     */
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    @Override
    public String toString() {
        return "NodeInfo{" +
                "nodeId=" + nodeId +
                ", address='" + address + '\'' +
                ", hostname='" + hostname + '\'' +
                '}';
    }

    /**
     * Creates a new {@code NodeInfo} builder.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link NodeInfo}.
     */
    public static class Builder {
        private Long nodeId;
        private String address;
        private String hostname;

        private Builder() {
        }

        /**
         * Sets the ID of the query node.
         *
         * @param nodeId the node ID
         * @return this builder
         */
        public Builder nodeId(Long nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        /**
         * Sets the address of the query node.
         *
         * @param address the node address
         * @return this builder
         */
        public Builder address(String address) {
            this.address = address;
            return this;
        }

        /**
         * Sets the hostname of the query node.
         *
         * @param hostname the node hostname
         * @return this builder
         */
        public Builder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        /**
         * Builds a {@link NodeInfo} with the configured properties.
         *
         * @return the built {@code NodeInfo}
         */
        public NodeInfo build() {
            return new NodeInfo(this);
        }
    }
}
