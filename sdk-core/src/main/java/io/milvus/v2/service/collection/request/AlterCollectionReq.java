package io.milvus.v2.service.collection.request;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;
import java.util.Map;

@Deprecated
public class AlterCollectionReq {
    private String collectionName;
    private String databaseName;
    private final Map<String, String> properties = new HashMap<>();

    private AlterCollectionReq(Builder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        if (builder.properties != null) {
            this.properties.putAll(builder.properties);
        }
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

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AlterCollectionReq that = (AlterCollectionReq) obj;
        return new EqualsBuilder()
                .append(collectionName, that.collectionName)
                .append(databaseName, that.databaseName)
                .append(properties, that.properties)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(collectionName)
                .append(databaseName)
                .append(properties)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "AlterCollectionReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", properties=" + properties +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String collectionName;
        private String databaseName;
        private Map<String, String> properties = new HashMap<>();

        private Builder() {}

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder property(String key, String value) {
            if (this.properties == null) {
                this.properties = new HashMap<>();
            }
            this.properties.put(key, value);
            return this;
        }

        public AlterCollectionReq build() {
            return new AlterCollectionReq(this);
        }
    }
}
