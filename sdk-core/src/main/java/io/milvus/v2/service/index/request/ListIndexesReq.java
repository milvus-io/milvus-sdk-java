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

/**
 * Request parameters for the {@code listIndexes} API.
 */
public class ListIndexesReq {
    private String databaseName;
    private String collectionName;
    private String fieldName;

    private ListIndexesReq(ListIndexesReqBuilder builder) {
        if (builder.collectionName == null) {
            throw new IllegalArgumentException("Collection name cannot be null");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.fieldName = builder.fieldName;
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
     * @throws IllegalArgumentException if the collection name is null
     */
    public void setCollectionName(String collectionName) {
        if (collectionName == null) {
            throw new IllegalArgumentException("Collection name cannot be null");
        }
        this.collectionName = collectionName;
    }

    /**
     * Returns the field name.
     *
     * @return the field name
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Sets the field name.
     *
     * @param fieldName the field name
     */
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public String toString() {
        return "ListIndexesReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", fieldName='" + fieldName + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@code ListIndexesReq}.
     *
     * @return the builder
     */
    public static ListIndexesReqBuilder builder() {
        return new ListIndexesReqBuilder();
    }

    public static class ListIndexesReqBuilder {
        private String databaseName;
        private String collectionName;
        private String fieldName;

        private ListIndexesReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public ListIndexesReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         * @throws IllegalArgumentException if the collection name is null
         */
        public ListIndexesReqBuilder collectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("Collection name cannot be null");
            }
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the field name.
         *
         * @param fieldName the field name
         * @return this builder
         */
        public ListIndexesReqBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Builds the {@code ListIndexesReq}.
         *
         * @return the built request
         */
        public ListIndexesReq build() {
            return new ListIndexesReq(this);
        }
    }
}
