package io.milvus.v2.service.resourcegroup.request;

import io.milvus.common.resourcegroup.ResourceGroupConfig;

import java.util.HashMap;
import java.util.Map;

public class UpdateResourceGroupsReq {
    private Map<String, ResourceGroupConfig> resourceGroups;

    private UpdateResourceGroupsReq(UpdateResourceGroupsReqBuilder builder) {
        this.resourceGroups = builder.resourceGroups;
    }

    public static UpdateResourceGroupsReqBuilder builder() {
        return new UpdateResourceGroupsReqBuilder();
    }

    public Map<String, ResourceGroupConfig> getResourceGroups() {
        return resourceGroups;
    }

    public void setResourceGroups(Map<String, ResourceGroupConfig> resourceGroups) {
        this.resourceGroups = resourceGroups;
    }

    @Override
    public String toString() {
        return "UpdateResourceGroupsReq{" +
                "resourceGroups=" + resourceGroups +
                '}';
    }

    public static class UpdateResourceGroupsReqBuilder {
        private Map<String, ResourceGroupConfig> resourceGroups = new HashMap<>();

        public UpdateResourceGroupsReqBuilder resourceGroups(Map<String, ResourceGroupConfig> resourceGroups) {
            this.resourceGroups = resourceGroups;
            return this;
        }

        public UpdateResourceGroupsReq build() {
            return new UpdateResourceGroupsReq(this);
        }
    }
}
