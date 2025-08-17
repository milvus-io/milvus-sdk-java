package io.milvus.v2.service.utility.request;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class GetPersistentSegmentInfoReq {
    private String collectionName;

    private GetPersistentSegmentInfoReq(Builder builder) {
        this.collectionName = builder.collectionName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        GetPersistentSegmentInfoReq that = (GetPersistentSegmentInfoReq) obj;
        return new EqualsBuilder()
                .append(collectionName, that.collectionName)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(collectionName)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "GetPersistentSegmentInfoReq{" +
                "collectionName='" + collectionName + '\'' +
                '}';
    }

    public static class Builder {
        private String collectionName;

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public GetPersistentSegmentInfoReq build() {
            return new GetPersistentSegmentInfoReq(this);
        }
    }
}
