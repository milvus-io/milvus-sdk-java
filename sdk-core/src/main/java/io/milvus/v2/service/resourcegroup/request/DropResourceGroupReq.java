package io.milvus.v2.service.resourcegroup.request;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class DropResourceGroupReq {
    private String groupName;

    private DropResourceGroupReq(Builder builder) {
        this.groupName = builder.groupName;
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DropResourceGroupReq that = (DropResourceGroupReq) obj;
        return new EqualsBuilder()
                .append(groupName, that.groupName)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(groupName)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "DropResourceGroupReq{" +
                "groupName='" + groupName + '\'' +
                '}';
    }

    public static class Builder {
        private String groupName;

        public Builder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public DropResourceGroupReq build() {
            return new DropResourceGroupReq(this);
        }
    }
}
