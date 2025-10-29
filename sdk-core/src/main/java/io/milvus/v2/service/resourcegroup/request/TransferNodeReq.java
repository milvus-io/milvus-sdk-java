package io.milvus.v2.service.resourcegroup.request;

public class TransferNodeReq {
    private String sourceGroupName;
    private String targetGroupName;
    private Integer numOfNodes;

    private TransferNodeReq(TransferNodeReqBuilder builder) {
        this.sourceGroupName = builder.sourceGroupName;
        this.targetGroupName = builder.targetGroupName;
        this.numOfNodes = builder.numOfNodes;
    }

    public static TransferNodeReqBuilder builder() {
        return new TransferNodeReqBuilder();
    }

    public String getSourceGroupName() {
        return sourceGroupName;
    }

    public void setSourceGroupName(String sourceGroupName) {
        this.sourceGroupName = sourceGroupName;
    }

    public String getTargetGroupName() {
        return targetGroupName;
    }

    public void setTargetGroupName(String targetGroupName) {
        this.targetGroupName = targetGroupName;
    }

    public Integer getNumOfNodes() {
        return numOfNodes;
    }

    public void setNumOfNodes(Integer numOfNodes) {
        this.numOfNodes = numOfNodes;
    }

    @Override
    public String toString() {
        return "TransferNodeReq{" +
                "sourceGroupName='" + sourceGroupName + '\'' +
                ", targetGroupName='" + targetGroupName + '\'' +
                ", numOfNodes=" + numOfNodes +
                '}';
    }

    public static class TransferNodeReqBuilder {
        private String sourceGroupName;
        private String targetGroupName;
        private Integer numOfNodes;

        public TransferNodeReqBuilder sourceGroupName(String sourceGroupName) {
            this.sourceGroupName = sourceGroupName;
            return this;
        }

        public TransferNodeReqBuilder targetGroupName(String targetGroupName) {
            this.targetGroupName = targetGroupName;
            return this;
        }

        public TransferNodeReqBuilder numOfNodes(Integer numOfNodes) {
            this.numOfNodes = numOfNodes;
            return this;
        }

        public TransferNodeReq build() {
            return new TransferNodeReq(this);
        }
    }
}
