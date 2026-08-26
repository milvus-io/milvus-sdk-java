package io.milvus.v2.service.resourcegroup.response;

import java.util.ArrayList;
import java.util.List;

/**
 * Response returned by the {@code listResourceGroups} API.
 */
public class ListResourceGroupsResp {
    private List<String> groupNames;

    private ListResourceGroupsResp(ListResourceGroupsRespBuilder builder) {
        this.groupNames = builder.groupNames;
    }

    /**
     * Creates a new builder for {@code ListResourceGroupsResp}.
     *
     * @return the builder
     */
    public static ListResourceGroupsRespBuilder builder() {
        return new ListResourceGroupsRespBuilder();
    }

    /**
     * Returns the names of all resource groups.
     *
     * @return the list of resource group names
     */
    public List<String> getGroupNames() {
        return groupNames;
    }

    /**
     * Sets the names of all resource groups.
     *
     * @param groupNames the list of resource group names
     */
    public void setGroupNames(List<String> groupNames) {
        this.groupNames = groupNames;
    }

    @Override
    public String toString() {
        return "ListResourceGroupsResp{" +
                "groupNames=" + groupNames +
                '}';
    }

    public static class ListResourceGroupsRespBuilder {
        private List<String> groupNames = new ArrayList<>();

        /**
         * Sets the names of all resource groups.
         *
         * @param groupNames the list of resource group names
         * @return this builder
         */
        public ListResourceGroupsRespBuilder groupNames(List<String> groupNames) {
            this.groupNames = groupNames;
            return this;
        }

        /**
         * Builds the {@code ListResourceGroupsResp}.
         *
         * @return the built response
         */
        public ListResourceGroupsResp build() {
            return new ListResourceGroupsResp(this);
        }
    }
}
