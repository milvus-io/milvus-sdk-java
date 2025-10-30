package io.milvus.v2.service.collection.request;

import java.util.HashMap;
import java.util.Map;

@Deprecated
public class AlterCollectionReq {
    private String collectionName;
    private String databaseName;
    private final Map<String, String> properties = new HashMap<>();

    private AlterCollectionReq(AlterCollectionReqBuilder builder) {
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
    public String toString() {
        return "AlterCollectionReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", properties=" + properties +
                '}';
    }

    public static AlterCollectionReqBuilder builder() {
        return new AlterCollectionReqBuilder();
    }

    public static class AlterCollectionReqBuilder {
        private String collectionName;
        private String databaseName;
        private Map<String, String> properties = new HashMap<>();

        private AlterCollectionReqBuilder() {
        }

        public AlterCollectionReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public AlterCollectionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public AlterCollectionReqBuilder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public AlterCollectionReqBuilder property(String key, String value) {
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
