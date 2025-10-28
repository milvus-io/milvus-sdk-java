package io.milvus.v2.service.resourcegroup.request;

import io.milvus.common.resourcegroup.ResourceGroupConfig;

public class CreateResourceGroupReq {
    private String groupName;
    private ResourceGroupConfig config;

    private CreateResourceGroupReq(CreateResourceGroupReqBuilder builder) {
        this.groupName = builder.groupName;
        this.config = builder.config;
    }

    public static CreateResourceGroupReqBuilder builder() {
        return new CreateResourceGroupReqBuilder();
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public ResourceGroupConfig getConfig() {
        return config;
    }

    public void setConfig(ResourceGroupConfig config) {
        this.config = config;
    }

    @Override
    public String toString() {
        return "CreateResourceGroupReq{" +
                "groupName='" + groupName + '\'' +
                ", config=" + config +
                '}';
    }

    public static class CreateResourceGroupReqBuilder {
        private String groupName;
        private ResourceGroupConfig config;

        public CreateResourceGroupReqBuilder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public CreateResourceGroupReqBuilder config(ResourceGroupConfig config) {
            this.config = config;
            return this;
        }

        public CreateResourceGroupReq build() {
            return new CreateResourceGroupReq(this);
        }
    }
}
