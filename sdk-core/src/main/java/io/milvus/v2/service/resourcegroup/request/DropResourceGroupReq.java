package io.milvus.v2.service.resourcegroup.request;

public class DropResourceGroupReq {
    private String groupName;

    private DropResourceGroupReq(DropResourceGroupReqBuilder builder) {
        this.groupName = builder.groupName;
    }

    public static DropResourceGroupReqBuilder builder() {
        return new DropResourceGroupReqBuilder();
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    @Override
    public String toString() {
        return "DropResourceGroupReq{" +
                "groupName='" + groupName + '\'' +
                '}';
    }

    public static class DropResourceGroupReqBuilder {
        private String groupName;

        public DropResourceGroupReqBuilder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public DropResourceGroupReq build() {
            return new DropResourceGroupReq(this);
        }
    }
}
