package io.milvus.v2.service.resourcegroup.response;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.List;

public class ListResourceGroupsResp {
    private List<String> groupNames;

    private ListResourceGroupsResp(Builder builder) {
        this.groupNames = builder.groupNames;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<String> getGroupNames() {
        return groupNames;
    }

    public void setGroupNames(List<String> groupNames) {
        this.groupNames = groupNames;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ListResourceGroupsResp that = (ListResourceGroupsResp) obj;
        return new EqualsBuilder()
                .append(groupNames, that.groupNames)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(groupNames)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "ListResourceGroupsResp{" +
                "groupNames=" + groupNames +
                '}';
    }

    public static class Builder {
        private List<String> groupNames = new ArrayList<>();

        public Builder groupNames(List<String> groupNames) {
            this.groupNames = groupNames;
            return this;
        }

        public ListResourceGroupsResp build() {
            return new ListResourceGroupsResp(this);
        }
    }
}
