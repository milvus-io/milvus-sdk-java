package io.milvus.v2.service.resourcegroup.response;

import java.util.ArrayList;
import java.util.List;

public class ListResourceGroupsResp {
    private List<String> groupNames;

    private ListResourceGroupsResp(ListResourceGroupsRespBuilder builder) {
        this.groupNames = builder.groupNames;
    }

    public static ListResourceGroupsRespBuilder builder() {
        return new ListResourceGroupsRespBuilder();
    }

    public List<String> getGroupNames() {
        return groupNames;
    }

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

        public ListResourceGroupsRespBuilder groupNames(List<String> groupNames) {
            this.groupNames = groupNames;
            return this;
        }

        public ListResourceGroupsResp build() {
            return new ListResourceGroupsResp(this);
        }
    }
}
