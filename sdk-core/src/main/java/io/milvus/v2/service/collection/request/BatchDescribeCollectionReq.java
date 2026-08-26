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

import java.util.List;

/**
 * Request parameters for the {@code batchDescribeCollection} API.
 */
public class BatchDescribeCollectionReq {
    private String databaseName;
    private List<String> collectionNames;
    private List<Long> collectionIds;

    // Private constructor for builder
    private BatchDescribeCollectionReq(BatchDescribeCollectionReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionNames = builder.collectionNames;
        this.collectionIds = builder.collectionIds;
    }

    // Static method to create builder
    /**
     * Creates a new builder for {@link BatchDescribeCollectionReq}.
     *
     * @return the builder
     */
    public static BatchDescribeCollectionReqBuilder builder() {
        return new BatchDescribeCollectionReqBuilder();
    }

    // Getter methods
    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Returns the names of the collections to describe.
     *
     * @return the collection names
     */
    public List<String> getCollectionNames() {
        return collectionNames;
    }

    /**
     * Returns the IDs of the collections to describe.
     *
     * @return the collection IDs
     */
    public List<Long> getCollectionIds() {
        return collectionIds;
    }

    // Setter methods
    /**
     * Sets the database name.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Sets the names of the collections to describe.
     *
     * @param collectionNames the collection names
     */
    public void setCollectionNames(List<String> collectionNames) {
        this.collectionNames = collectionNames;
    }

    /**
     * Sets the IDs of the collections to describe.
     *
     * @param collectionIds the collection IDs
     */
    public void setCollectionIds(List<Long> collectionIds) {
        this.collectionIds = collectionIds;
    }

    @Override
    public String toString() {
        return "BatchDescribeCollectionReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionNames=" + collectionNames +
                ", collectionIds=" + collectionIds +
                '}';
    }

    // Builder class
    public static class BatchDescribeCollectionReqBuilder {
        private String databaseName;
        private List<String> collectionNames;
        private List<Long> collectionIds;

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public BatchDescribeCollectionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the names of the collections to describe.
         *
         * @param collectionNames the collection names
         * @return this builder
         */
        public BatchDescribeCollectionReqBuilder collectionNames(List<String> collectionNames) {
            this.collectionNames = collectionNames;
            return this;
        }

        /**
         * Sets the IDs of the collections to describe.
         *
         * @param collectionIds the collection IDs
         * @return this builder
         */
        public BatchDescribeCollectionReqBuilder collectionIds(List<Long> collectionIds) {
            this.collectionIds = collectionIds;
            return this;
        }

        /**
         * Builds a {@link BatchDescribeCollectionReq} with the configured parameters.
         *
         * @return the request
         */
        public BatchDescribeCollectionReq build() {
            return new BatchDescribeCollectionReq(this);
        }
    }
}
