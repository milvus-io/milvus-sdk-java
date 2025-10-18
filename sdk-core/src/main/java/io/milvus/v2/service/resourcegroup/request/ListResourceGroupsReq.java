package io.milvus.v2.service.resourcegroup.request;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class ListResourceGroupsReq {

    private ListResourceGroupsReq(Builder builder) {
        // No fields to initialize
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        return new EqualsBuilder().isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).toHashCode();
    }

    @Override
    public String toString() {
        return "ListResourceGroupsReq{}";
    }

    public static class Builder {

        public ListResourceGroupsReq build() {
            return new ListResourceGroupsReq(this);
        }
    }
}
