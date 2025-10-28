package io.milvus.v2.service.resourcegroup.request;

public class DescribeResourceGroupReq {
    private String groupName;

    private DescribeResourceGroupReq(DescribeResourceGroupReqBuilder builder) {
        this.groupName = builder.groupName;
    }

    public static DescribeResourceGroupReqBuilder builder() {
        return new DescribeResourceGroupReqBuilder();
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    @Override
    public String toString() {
        return "DescribeResourceGroupReq{" +
                "groupName='" + groupName + '\'' +
                '}';
    }

    public static class DescribeResourceGroupReqBuilder {
        private String groupName;

        public DescribeResourceGroupReqBuilder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public DescribeResourceGroupReq build() {
            return new DescribeResourceGroupReq(this);
        }
    }
}
