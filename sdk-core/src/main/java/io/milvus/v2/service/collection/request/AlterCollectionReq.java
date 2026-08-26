package io.milvus.v2.service.collection.request;

import java.util.HashMap;
import java.util.Map;

/**
 * Deprecated request parameters for the {@code alterCollection} API.
 * Use {@link AlterCollectionPropertiesReq} instead.
 *
 * @deprecated since SDK v2.5.3, replaced by {@code alterCollectionProperties} to keep
 *             consistency with other SDKs.
 */
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
     * Returns the properties to alter on the collection.
     *
     * @return the collection properties
     */
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

    /**
     * Creates a new builder for {@link AlterCollectionReq}.
     *
     * @return the builder
     */
    public static AlterCollectionReqBuilder builder() {
        return new AlterCollectionReqBuilder();
    }

    public static class AlterCollectionReqBuilder {
        private String collectionName;
        private String databaseName;
        private Map<String, String> properties = new HashMap<>();

        private AlterCollectionReqBuilder() {
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public AlterCollectionReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public AlterCollectionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the properties to alter on the collection.
         *
         * @param properties the collection properties
         * @return this builder
         */
        public AlterCollectionReqBuilder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        /**
         * Adds a single property to alter on the collection.
         *
         * @param key the property key
         * @param value the property value
         * @return this builder
         */
        public AlterCollectionReqBuilder property(String key, String value) {
            if (this.properties == null) {
                this.properties = new HashMap<>();
            }
            this.properties.put(key, value);
            return this;
        }

        /**
         * Builds an {@link AlterCollectionReq} with the configured parameters.
         *
         * @return the request
         */
        public AlterCollectionReq build() {
            return new AlterCollectionReq(this);
        }
    }
}
