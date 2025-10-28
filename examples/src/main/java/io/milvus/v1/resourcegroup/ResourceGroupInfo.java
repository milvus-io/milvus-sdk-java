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

package io.milvus.v1.resourcegroup;

import io.milvus.common.resourcegroup.ResourceGroupConfig;

import java.util.HashSet;
import java.util.Set;

public class ResourceGroupInfo {
    private String resourceGroupName;
    private ResourceGroupConfig resourceGroupConfig;
    private Set<String> fullDatabases; // databases belong to this resource group completely.
    private Set<String> partialDatabases; // databases belong to this resource group partially, some collection is in
    // other resource group.
    private Set<NodeInfo> nodes; // actual query node in this resource group.

    private ResourceGroupInfo(Builder builder) {
        this.resourceGroupName = builder.resourceGroupName;
        this.resourceGroupConfig = builder.resourceGroupConfig;
        this.fullDatabases = builder.fullDatabases;
        if (this.fullDatabases == null) {
            this.fullDatabases = new HashSet<String>();
        }
        this.partialDatabases = builder.partialDatabases;
        if (this.partialDatabases == null) {
            this.partialDatabases = new HashSet<String>();
        }
        this.nodes = builder.nodes;
        if (this.nodes == null) {
            this.nodes = new HashSet<NodeInfo>();
        }
    }

    public String getResourceGroupName() {
        return resourceGroupName;
    }

    public ResourceGroupConfig getResourceGroupConfig() {
        return resourceGroupConfig;
    }

    public Set<String> getFullDatabases() {
        return fullDatabases;
    }

    public Set<String> getPartialDatabases() {
        return partialDatabases;
    }

    public Set<NodeInfo> getNodes() {
        return nodes;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String resourceGroupName;
        private ResourceGroupConfig resourceGroupConfig;
        private Set<String> fullDatabases;
        private Set<String> partialDatabases;
        private Set<NodeInfo> nodes; // actual query node in this resource group.

        public Builder withResourceGroupName(String resourceGroupName) {
            this.resourceGroupName = resourceGroupName;
            return this;
        }

        public Builder addFullDatabases(String databaseName) {
            if (this.fullDatabases == null) {
                this.fullDatabases = new HashSet<String>();
            }
            this.fullDatabases.add(databaseName);
            return this;
        }

        public Builder addPartialDatabases(String databaseName) {
            if (this.partialDatabases == null) {
                this.partialDatabases = new HashSet<String>();
            }
            this.partialDatabases.add(databaseName);
            return this;
        }

        public Builder addAvailableNode(NodeInfo node) {
            if (this.nodes == null) {
                this.nodes = new HashSet<NodeInfo>();
            }
            this.nodes.add(node);
            return this;
        }

        public Builder withConfig(ResourceGroupConfig resourceGroupConfig) {
            this.resourceGroupConfig = resourceGroupConfig;
            return this;
        }

        public ResourceGroupInfo build() {
            return new ResourceGroupInfo(this);
        }
    }

    /**
     * Check if this resource group is the default resource group.
     *
     * @return true if this resource group is the default resource group.
     */
    public boolean isDefaultResourceGroup() {
        return this.resourceGroupName == ResourceGroupManagement.DEFAULT_RG;
    }

    /**
     * Check if this resource group is the recycle resource group.
     *
     * @return true if this resource group is the recycle resource group.
     */
    public boolean isRecycleResourceGroup() {
        return this.resourceGroupName == ResourceGroupManagement.RECYCLE_RG;
    }
}
