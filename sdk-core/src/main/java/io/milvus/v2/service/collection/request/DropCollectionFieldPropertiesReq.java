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
 * Request parameters for the {@code dropCollectionFieldProperties} API.
 */
public class DropCollectionFieldPropertiesReq {
    private String collectionName;
    private String databaseName;
    private String fieldName;
    private List<String> propertyKeys = new ArrayList<>();

    private DropCollectionFieldPropertiesReq(DropCollectionFieldPropertiesReqBuilder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.fieldName = builder.fieldName;
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
     * Returns the name of the field whose properties are to be dropped.
     *
     * @return the field name
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Sets the name of the field whose properties are to be dropped.
     *
     * @param fieldName the field name
     */
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * Returns the keys of the properties to drop from the field.
     *
     * @return the property keys
     */
    public List<String> getPropertyKeys() {
        return propertyKeys;
    }

    /**
     * Sets the keys of the properties to drop from the field.
     *
     * @param propertyKeys the property keys
     */
    public void setPropertyKeys(List<String> propertyKeys) {
        this.propertyKeys = propertyKeys;
    }

    @Override
    public String toString() {
        return "DropCollectionFieldPropertiesReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", propertyKeys=" + propertyKeys +
                '}';
    }

    /**
     * Creates a new builder for {@link DropCollectionFieldPropertiesReq}.
     *
     * @return the builder
     */
    public static DropCollectionFieldPropertiesReqBuilder builder() {
        return new DropCollectionFieldPropertiesReqBuilder();
    }

    public static class DropCollectionFieldPropertiesReqBuilder {
        private String collectionName;
        private String databaseName;
        private String fieldName;
        private List<String> propertyKeys = new ArrayList<>();

        private DropCollectionFieldPropertiesReqBuilder() {
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public DropCollectionFieldPropertiesReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public DropCollectionFieldPropertiesReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the name of the field whose properties are to be dropped.
         *
         * @param fieldName the field name
         * @return this builder
         */
        public DropCollectionFieldPropertiesReqBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Sets the keys of the properties to drop from the field.
         *
         * @param propertyKeys the property keys
         * @return this builder
         */
        public DropCollectionFieldPropertiesReqBuilder propertyKeys(List<String> propertyKeys) {
            this.propertyKeys = propertyKeys;
            return this;
        }

        /**
         * Builds a {@link DropCollectionFieldPropertiesReq} with the configured parameters.
         *
         * @return the request
         */
        public DropCollectionFieldPropertiesReq build() {
            return new DropCollectionFieldPropertiesReq(this);
        }
    }
}
