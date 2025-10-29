package io.milvus.v2.service.resourcegroup.request;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class TransferReplicaReq {
    private String sourceGroupName;
    private String targetGroupName;
    private String collectionName;
    private String databaseName;
    private Long numberOfReplicas;

    private TransferReplicaReq(Builder builder) {
        this.sourceGroupName = builder.sourceGroupName;
        this.targetGroupName = builder.targetGroupName;
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.numberOfReplicas = builder.numberOfReplicas;
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

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public Long getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public void setNumberOfReplicas(Long numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TransferReplicaReq that = (TransferReplicaReq) obj;
        return new EqualsBuilder()
                .append(sourceGroupName, that.sourceGroupName)
                .append(targetGroupName, that.targetGroupName)
                .append(collectionName, that.collectionName)
                .append(databaseName, that.databaseName)
                .append(numberOfReplicas, that.numberOfReplicas)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(sourceGroupName)
                .append(targetGroupName)
                .append(collectionName)
                .append(databaseName)
                .append(numberOfReplicas)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "TransferReplicaReq{" +
                "sourceGroupName='" + sourceGroupName + '\'' +
                ", targetGroupName='" + targetGroupName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", numberOfReplicas=" + numberOfReplicas +
                '}';
    }

    public static class Builder {
        private String sourceGroupName;
        private String targetGroupName;
        private String collectionName;
        private String databaseName;
        private Long numberOfReplicas;

        public Builder sourceGroupName(String sourceGroupName) {
            this.sourceGroupName = sourceGroupName;
            return this;
        }

        public Builder targetGroupName(String targetGroupName) {
            this.targetGroupName = targetGroupName;
            return this;
        }

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder numberOfReplicas(Long numberOfReplicas) {
            this.numberOfReplicas = numberOfReplicas;
            return this;
        }

        public TransferReplicaReq build() {
            return new TransferReplicaReq(this);
        }
    }
}
