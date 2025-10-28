package io.milvus.v2.service.resourcegroup.request;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class TransferNodeReq {
    private String sourceGroupName;
    private String targetGroupName;
    private Integer numOfNodes;

    private TransferNodeReq(Builder builder) {
        this.sourceGroupName = builder.sourceGroupName;
        this.targetGroupName = builder.targetGroupName;
        this.numOfNodes = builder.numOfNodes;
    }

    public static Builder builder() {
        return new Builder();
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
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TransferNodeReq that = (TransferNodeReq) obj;
        return new EqualsBuilder()
                .append(sourceGroupName, that.sourceGroupName)
                .append(targetGroupName, that.targetGroupName)
                .append(numOfNodes, that.numOfNodes)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(sourceGroupName)
                .append(targetGroupName)
                .append(numOfNodes)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "TransferNodeReq{" +
                "sourceGroupName='" + sourceGroupName + '\'' +
                ", targetGroupName='" + targetGroupName + '\'' +
                ", numOfNodes=" + numOfNodes +
                '}';
    }

    public static class Builder {
        private String sourceGroupName;
        private String targetGroupName;
        private Integer numOfNodes;

        public Builder sourceGroupName(String sourceGroupName) {
            this.sourceGroupName = sourceGroupName;
            return this;
        }

        public Builder targetGroupName(String targetGroupName) {
            this.targetGroupName = targetGroupName;
            return this;
        }

        public Builder numOfNodes(Integer numOfNodes) {
            this.numOfNodes = numOfNodes;
            return this;
        }

        public TransferNodeReq build() {
            return new TransferNodeReq(this);
        }
    }
}
