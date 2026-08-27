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
 * Request parameters for the {@code renameCollection} API.
 */
public class RenameCollectionReq {
    private String databaseName;
    private String collectionName;
    private String newCollectionName;
    private String targetDbName;

    private RenameCollectionReq(RenameCollectionReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.newCollectionName = builder.newCollectionName;
        this.targetDbName = builder.targetDbName;
    }

    /**
     * Returns the database name of the collection to rename.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name of the collection to rename.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the current name of the collection to rename.
     *
     * @return the current collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the current name of the collection to rename.
     *
     * @param collectionName the current collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the new name of the collection.
     *
     * @return the new collection name
     */
    public String getNewCollectionName() {
        return newCollectionName;
    }

    /**
     * Sets the new name of the collection.
     *
     * @param newCollectionName the new collection name
     */
    public void setNewCollectionName(String newCollectionName) {
        this.newCollectionName = newCollectionName;
    }

    /**
     * Returns the target database name to move the collection to.
     *
     * @return the target database name
     */
    public String getTargetDbName() {
        return targetDbName;
    }

    /**
     * Sets the target database name to move the collection to.
     *
     * @param targetDbName the target database name
     */
    public void setTargetDbName(String targetDbName) {
        this.targetDbName = targetDbName;
    }

    @Override
    public String toString() {
        return "RenameCollectionReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", newCollectionName='" + newCollectionName + '\'' +
                ", targetDbName='" + targetDbName + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link RenameCollectionReq}.
     *
     * @return the builder
     */
    public static RenameCollectionReqBuilder builder() {
        return new RenameCollectionReqBuilder();
    }

    public static class RenameCollectionReqBuilder {
        private String databaseName;
        private String collectionName;
        private String newCollectionName;
        private String targetDbName;

        private RenameCollectionReqBuilder() {
        }

        /**
         * Sets the database name of the collection to rename.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public RenameCollectionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the current name of the collection to rename.
         *
         * @param collectionName the current collection name
         * @return this builder
         */
        public RenameCollectionReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the new name of the collection.
         *
         * @param newCollectionName the new collection name
         * @return this builder
         */
        public RenameCollectionReqBuilder newCollectionName(String newCollectionName) {
            this.newCollectionName = newCollectionName;
            return this;
        }

        /**
         * Sets the target database name to move the collection to.
         *
         * @param targetDbName the target database name
         * @return this builder
         */
        public RenameCollectionReqBuilder targetDbName(String targetDbName) {
            this.targetDbName = targetDbName;
            return this;
        }

        /**
         * Builds a {@link RenameCollectionReq} with the configured parameters.
         *
         * @return the request
         */
        public RenameCollectionReq build() {
            return new RenameCollectionReq(this);
        }
    }
}
