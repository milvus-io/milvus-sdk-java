package io.milvus.v2.service.resourcegroup.request;

import io.milvus.common.resourcegroup.ResourceGroupConfig;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;
import java.util.Map;

public class UpdateResourceGroupsReq {
    private Map<String, ResourceGroupConfig> resourceGroups;

    private UpdateResourceGroupsReq(Builder builder) {
        this.resourceGroups = builder.resourceGroups;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, ResourceGroupConfig> getResourceGroups() {
        return resourceGroups;
    }

    public void setResourceGroups(Map<String, ResourceGroupConfig> resourceGroups) {
        this.resourceGroups = resourceGroups;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        UpdateResourceGroupsReq that = (UpdateResourceGroupsReq) obj;
        return new EqualsBuilder()
                .append(resourceGroups, that.resourceGroups)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(resourceGroups)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "UpdateResourceGroupsReq{" +
                "resourceGroups=" + resourceGroups +
                '}';
    }

    public static class Builder {
        private Map<String, ResourceGroupConfig> resourceGroups = new HashMap<>();

        public Builder resourceGroups(Map<String, ResourceGroupConfig> resourceGroups) {
            this.resourceGroups = resourceGroups;
            return this;
        }

        public UpdateResourceGroupsReq build() {
            return new UpdateResourceGroupsReq(this);
        }
    }
}
