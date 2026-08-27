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

/**
 * Request parameters for the {@code dropCollectionField} API.
 */
public class DropCollectionFieldReq {
    private String collectionName;
    private String databaseName;
    private String fieldName;
    private Long fieldId;

    private DropCollectionFieldReq(DropCollectionFieldReqBuilder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.fieldName = builder.fieldName;
        this.fieldId = builder.fieldId;
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
     * Returns the name of the field to drop.
     *
     * @return the field name
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Sets the name of the field to drop.
     *
     * @param fieldName the field name
     */
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * Returns the ID of the field to drop.
     *
     * @return the field ID
     */
    public Long getFieldId() {
        return fieldId;
    }

    /**
     * Sets the ID of the field to drop.
     *
     * @param fieldId the field ID
     */
    public void setFieldId(Long fieldId) {
        this.fieldId = fieldId;
    }

    @Override
    public String toString() {
        return "DropCollectionFieldReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", fieldId=" + fieldId +
                '}';
    }

    /**
     * Creates a new builder for {@link DropCollectionFieldReq}.
     *
     * @return the builder
     */
    public static DropCollectionFieldReqBuilder builder() {
        return new DropCollectionFieldReqBuilder();
    }

    public static class DropCollectionFieldReqBuilder {
        private String collectionName = "";
        private String databaseName = "";
        private String fieldName = "";
        private Long fieldId;

        private DropCollectionFieldReqBuilder() {
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public DropCollectionFieldReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public DropCollectionFieldReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the name of the field to drop.
         *
         * @param fieldName the field name
         * @return this builder
         */
        public DropCollectionFieldReqBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Sets the ID of the field to drop.
         *
         * @param fieldId the field ID
         * @return this builder
         */
        public DropCollectionFieldReqBuilder fieldId(Long fieldId) {
            this.fieldId = fieldId;
            return this;
        }

        /**
         * Builds a {@link DropCollectionFieldReq} with the configured parameters.
         *
         * @return the request
         */
        public DropCollectionFieldReq build() {
            return new DropCollectionFieldReq(this);
        }
    }
}
