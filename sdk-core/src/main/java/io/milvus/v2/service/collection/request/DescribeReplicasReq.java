package io.milvus.v2.service.collection.request;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class DescribeReplicasReq {
    private String collectionName;
    private String databaseName;

    private DescribeReplicasReq(Builder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DescribeReplicasReq that = (DescribeReplicasReq) obj;
        return new EqualsBuilder()
                .append(collectionName, that.collectionName)
                .append(databaseName, that.databaseName)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(collectionName)
                .append(databaseName)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "DescribeReplicasReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String collectionName;
        private String databaseName;

        private Builder() {}

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public DescribeReplicasReq build() {
            return new DescribeReplicasReq(this);
        }
    }
}
