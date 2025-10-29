package io.milvus.v2.service.resourcegroup.request;

public class ListResourceGroupsReq {

    private ListResourceGroupsReq(ListResourceGroupsReqBuilder builder) {
        // No fields to initialize
    }

    public static ListResourceGroupsReqBuilder builder() {
        return new ListResourceGroupsReqBuilder();
    }

    @Override
    public String toString() {
        return "ListResourceGroupsReq{}";
    }

    public static class ListResourceGroupsReqBuilder {

        public ListResourceGroupsReq build() {
            return new ListResourceGroupsReq(this);
        }
    }
}
