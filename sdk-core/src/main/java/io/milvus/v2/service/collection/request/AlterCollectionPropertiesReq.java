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

package io.milvus.v2.service.collection.request;

import java.util.HashMap;
import java.util.Map;

/**
 * Request parameters for the {@code alterCollectionProperties} API.
 */
public class AlterCollectionPropertiesReq {
    private String collectionName;
    private String databaseName;
    private final Map<String, String> properties = new HashMap<>();

    private AlterCollectionPropertiesReq(AlterCollectionPropertiesReqBuilder builder) {
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
        return "AlterCollectionPropertiesReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", properties=" + properties +
                '}';
    }

    /**
     * Creates a new builder for {@link AlterCollectionPropertiesReq}.
     *
     * @return the builder
     */
    public static AlterCollectionPropertiesReqBuilder builder() {
        return new AlterCollectionPropertiesReqBuilder();
    }

    public static class AlterCollectionPropertiesReqBuilder {
        private String collectionName;
        private String databaseName;
        private Map<String, String> properties = new HashMap<>();

        private AlterCollectionPropertiesReqBuilder() {
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public AlterCollectionPropertiesReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public AlterCollectionPropertiesReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the properties to alter on the collection.
         *
         * @param properties the collection properties
         * @return this builder
         */
        public AlterCollectionPropertiesReqBuilder properties(Map<String, String> properties) {
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
        public AlterCollectionPropertiesReqBuilder property(String key, String value) {
            if (this.properties == null) {
                this.properties = new HashMap<>();
            }
            this.properties.put(key, value);
            return this;
        }

        /**
         * Builds an {@link AlterCollectionPropertiesReq} with the configured parameters.
         *
         * @return the request
         */
        public AlterCollectionPropertiesReq build() {
            return new AlterCollectionPropertiesReq(this);
        }
    }
}
