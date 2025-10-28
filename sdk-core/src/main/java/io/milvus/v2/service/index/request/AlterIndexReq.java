package io.milvus.v2.service.index.request;

import java.util.HashMap;
import java.util.Map;

@Deprecated
public class AlterIndexReq {
    private String collectionName;
    private String databaseName;
    private String indexName;
    private Map<String, String> properties;

    private AlterIndexReq(AlterIndexReqBuilder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.indexName = builder.indexName;
        this.properties = builder.properties;
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

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "AlterIndexReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", indexName='" + indexName + '\'' +
                ", properties=" + properties +
                '}';
    }

    public static AlterIndexReqBuilder builder() {
        return new AlterIndexReqBuilder();
    }

    public static class AlterIndexReqBuilder {
        private String collectionName;
        private String databaseName;
        private String indexName;
        private Map<String, String> properties = new HashMap<>();

        private AlterIndexReqBuilder() {
        }

        public AlterIndexReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public AlterIndexReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public AlterIndexReqBuilder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public AlterIndexReqBuilder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public AlterIndexReq build() {
            return new AlterIndexReq(this);
        }
    }
}
