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

import java.util.ArrayList;
import java.util.List;

/**
 * Request parameters for the {@code dropCollectionProperties} API.
 */
public class DropCollectionPropertiesReq {
    private String collectionName;
    private String databaseName;
    private List<String> propertyKeys = new ArrayList<>();

    private DropCollectionPropertiesReq(DropCollectionPropertiesReqBuilder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.propertyKeys = builder.propertyKeys;
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
     * Returns the keys of the properties to drop from the collection.
     *
     * @return the property keys
     */
    public List<String> getPropertyKeys() {
        return propertyKeys;
    }

    /**
     * Sets the keys of the properties to drop from the collection.
     *
     * @param propertyKeys the property keys
     */
    public void setPropertyKeys(List<String> propertyKeys) {
        this.propertyKeys = propertyKeys;
    }

    @Override
    public String toString() {
        return "DropCollectionPropertiesReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", propertyKeys=" + propertyKeys +
                '}';
    }

    /**
     * Creates a new builder for {@link DropCollectionPropertiesReq}.
     *
     * @return the builder
     */
    public static DropCollectionPropertiesReqBuilder builder() {
        return new DropCollectionPropertiesReqBuilder();
    }

    public static class DropCollectionPropertiesReqBuilder {
        private String collectionName;
        private String databaseName;
        private List<String> propertyKeys = new ArrayList<>();

        private DropCollectionPropertiesReqBuilder() {
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public DropCollectionPropertiesReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public DropCollectionPropertiesReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the keys of the properties to drop from the collection.
         *
         * @param propertyKeys the property keys
         * @return this builder
         */
        public DropCollectionPropertiesReqBuilder propertyKeys(List<String> propertyKeys) {
            this.propertyKeys = propertyKeys;
            return this;
        }

        /**
         * Builds a {@link DropCollectionPropertiesReq} with the configured parameters.
         *
         * @return the request
         */
        public DropCollectionPropertiesReq build() {
            return new DropCollectionPropertiesReq(this);
        }
    }
}
