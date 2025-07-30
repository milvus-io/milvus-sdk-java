package io.milvus.v2.service.resourcegroup.request;

import io.milvus.common.resourcegroup.ResourceGroupConfig;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class CreateResourceGroupReq {
    private String groupName;
    private ResourceGroupConfig config;

    private CreateResourceGroupReq(Builder builder) {
        this.groupName = builder.groupName;
        this.config = builder.config;
    }

    public static Builder builder() {
        return new Builder();
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
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        CreateResourceGroupReq that = (CreateResourceGroupReq) obj;
        return new EqualsBuilder()
                .append(groupName, that.groupName)
                .append(config, that.config)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(groupName)
                .append(config)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "CreateResourceGroupReq{" +
                "groupName='" + groupName + '\'' +
                ", config=" + config +
                '}';
    }

    public static class Builder {
        private String groupName;
        private ResourceGroupConfig config;

        public Builder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public Builder config(ResourceGroupConfig config) {
            this.config = config;
            return this;
        }

        public CreateResourceGroupReq build() {
            return new CreateResourceGroupReq(this);
        }
    }
}
