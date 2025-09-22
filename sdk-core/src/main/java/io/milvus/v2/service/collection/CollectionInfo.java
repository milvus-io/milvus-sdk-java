package io.milvus.v2.service.collection;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class CollectionInfo {
    private String collectionName;
    private Integer shardNum;

    // Private constructor for builder
    private CollectionInfo(Builder builder) {
        this.collectionName = builder.collectionName;
        this.shardNum = builder.shardNum;
    }

    // Static method to create builder
    public static Builder builder() {
        return new Builder();
    }

    // Getter methods
    public String getCollectionName() {
        return collectionName;
    }

    public Integer getShardNum() {
        return shardNum;
    }

    // Setter methods
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public void setShardNum(Integer shardNum) {
        this.shardNum = shardNum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CollectionInfo that = (CollectionInfo) o;

        return new EqualsBuilder()
                .append(collectionName, that.collectionName)
                .append(shardNum, that.shardNum)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(collectionName)
                .append(shardNum)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "CollectionInfo{" +
                "collectionName='" + collectionName + '\'' +
                ", shardNum=" + shardNum +
                '}';
    }

    // Builder class
    public static class Builder {
        private String collectionName;
        private Integer shardNum;

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder shardNum(Integer shardNum) {
            this.shardNum = shardNum;
            return this;
        }

        public CollectionInfo build() {
            return new CollectionInfo(this);
        }
    }
}
