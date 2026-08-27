/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.v2.service.index.request;

import java.util.HashMap;
import java.util.Map;

/**
 * Request parameters for the {@code alterIndexProperties} API.
 */
public class AlterIndexPropertiesReq {
    private String collectionName;
    private String databaseName;
    private String indexName;
    private Map<String, String> properties;

    private AlterIndexPropertiesReq(AlterIndexPropertiesReqBuilder builder) {
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
        return "AlterIndexPropertiesReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", indexName='" + indexName + '\'' +
                ", properties=" + properties +
                '}';
    }

    /**
     * Creates a new builder for {@code AlterIndexPropertiesReq}.
     *
     * @return the builder
     */
    public static AlterIndexPropertiesReqBuilder builder() {
        return new AlterIndexPropertiesReqBuilder();
    }

    public static class AlterIndexPropertiesReqBuilder {
        private String collectionName;
        private String databaseName;
        private String indexName;
        private Map<String, String> properties = new HashMap<>();

        private AlterIndexPropertiesReqBuilder() {
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public AlterIndexPropertiesReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public AlterIndexPropertiesReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the index name.
         *
         * @param indexName the index name
         * @return this builder
         */
        public AlterIndexPropertiesReqBuilder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        /**
         * Sets the index properties to be altered.
         *
         * @param properties the index properties
         * @return this builder
         */
        public AlterIndexPropertiesReqBuilder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        /**
         * Adds a single index property to be altered.
         *
         * @param key the property key
         * @param value the property value
         * @return this builder
         */
        public AlterIndexPropertiesReqBuilder property(String key, String value) {
            if (this.properties == null) {
                this.properties = new HashMap<>();
            }
            this.properties.put(key, value);
            return this;
        }

        /**
         * Builds the {@code AlterIndexPropertiesReq}.
         *
         * @return the built request
         */
        public AlterIndexPropertiesReq build() {
            return new AlterIndexPropertiesReq(this);
        }
    }
}
