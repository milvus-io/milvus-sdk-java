package io.milvus.v2.service.index.request;

import java.util.HashMap;
import java.util.Map;

/**
 * Request parameters for the {@code alterIndex} API.
 *
 * @deprecated use {@link AlterIndexPropertiesReq} with the {@code alterIndexProperties} API instead
 */
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

    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the index name.
     *
     * @return the index name
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Sets the index name.
     *
     * @param indexName the index name
     */
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    /**
     * Returns the index properties to be altered.
     *
     * @return the index properties
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Sets the index properties to be altered.
     *
     * @param properties the index properties
     */
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

    /**
     * Creates a new builder for {@code AlterIndexReq}.
     *
     * @return the builder
     */
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

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public AlterIndexReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public AlterIndexReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the index name.
         *
         * @param indexName the index name
         * @return this builder
         */
        public AlterIndexReqBuilder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        /**
         * Sets the index properties to be altered.
         *
         * @param properties the index properties
         * @return this builder
         */
        public AlterIndexReqBuilder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        /**
         * Builds the {@code AlterIndexReq}.
         *
         * @return the built request
         */
        public AlterIndexReq build() {
            return new AlterIndexReq(this);
        }
    }
}
