package io.milvus.resourcegroup;

import java.util.HashSet;
import java.util.Set;

import io.milvus.common.resourcegroup.ResourceGroupConfig;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class ResourceGroupInfo {
    private String resourceGroupName;
    private ResourceGroupConfig resourceGroupConfig;
    private Set<String> fullDatabases; // databases belong to this resource group completely.
    private Set<String> partialDatabases; // databases belong to this resource group partially, some collection is in
                                          // other resource group.
    private Set<NodeInfo> nodes; // actual query node in this resource group.

    private ResourceGroupInfo(@NonNull Builder builder) {
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

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String resourceGroupName;
        private ResourceGroupConfig resourceGroupConfig;
        private Set<String> fullDatabases;
        private Set<String> partialDatabases;
        private Set<NodeInfo> nodes; // actual query node in this resource group.

        public Builder withResourceGroupName(@NonNull String resourceGroupName) {
            this.resourceGroupName = resourceGroupName;
            return this;
        }

        public Builder addFullDatabases(@NonNull String databaseName) {
            if (this.fullDatabases == null) {
                this.fullDatabases = new HashSet<String>();
            }
            this.fullDatabases.add(databaseName);
            return this;
        }

        public Builder addPartialDatabases(@NonNull String databaseName) {
            if (this.partialDatabases == null) {
                this.partialDatabases = new HashSet<String>();
            }
            this.partialDatabases.add(databaseName);
            return this;
        }

        public Builder addAvailableNode(@NonNull NodeInfo node) {
            if (this.nodes == null) {
                this.nodes = new HashSet<NodeInfo>();
            }
            this.nodes.add(node);
            return this;
        }

        public Builder withConfig(@NonNull ResourceGroupConfig resourceGroupConfig) {
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
