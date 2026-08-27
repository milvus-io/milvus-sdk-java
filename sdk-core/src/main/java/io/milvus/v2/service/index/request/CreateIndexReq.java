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

import io.milvus.v2.common.IndexParam;

import java.util.List;

/**
 * Request parameters for the {@code createIndex} API.
 */
public class CreateIndexReq {
    private String databaseName;
    private String collectionName;
    private List<IndexParam> indexParams;
    private Boolean sync = Boolean.TRUE; // wait the index to complete
    private Long timeout = 60000L; // timeout value for waiting the index to complete

    private CreateIndexReq(CreateIndexReqBuilder builder) {
        if (builder.collectionName == null) {
            throw new IllegalArgumentException("Collection name cannot be null");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.indexParams = builder.indexParams;
        this.sync = builder.sync;
        this.timeout = builder.timeout;
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
     * Returns the index parameters to be created.
     *
     * @return the list of index parameters
     */
    public List<IndexParam> getIndexParams() {
        return indexParams;
    }

    /**
     * Sets the index parameters to be created.
     *
     * @param indexParams the list of index parameters
     */
    public void setIndexParams(List<IndexParam> indexParams) {
        this.indexParams = indexParams;
    }

    /**
     * Returns whether the call waits until the index is built.
     *
     * @return {@code true} to wait for the index to complete
     */
    public Boolean getSync() {
        return sync;
    }

    /**
     * Sets whether the call waits until the index is built.
     *
     * @param sync {@code true} to wait for the index to complete
     */
    public void setSync(Boolean sync) {
        this.sync = sync;
    }

    /**
     * Returns the timeout in milliseconds for waiting the index to complete.
     *
     * @return the timeout value in milliseconds
     */
    public Long getTimeout() {
        return timeout;
    }

    /**
     * Sets the timeout in milliseconds for waiting the index to complete.
     *
     * @param timeout the timeout value in milliseconds
     */
    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    @Override
    public String toString() {
        return "CreateIndexReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", indexParams=" + indexParams +
                ", sync=" + sync +
                ", timeout=" + timeout +
                '}';
    }

    /**
     * Creates a new builder for {@code CreateIndexReq}.
     *
     * @return the builder
     */
    public static CreateIndexReqBuilder builder() {
        return new CreateIndexReqBuilder();
    }

    public static class CreateIndexReqBuilder {
        private String databaseName;
        private String collectionName;
        private List<IndexParam> indexParams;
        private Boolean sync = Boolean.TRUE; // wait the index to complete
        private Long timeout = 60000L; // timeout value for waiting the index to complete

        private CreateIndexReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public CreateIndexReqBuilder databaseName(String databaseName) {
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
        public CreateIndexReqBuilder collectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("Collection name cannot be null");
            }
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the index parameters to be created.
         *
         * @param indexParams the list of index parameters
         * @return this builder
         */
        public CreateIndexReqBuilder indexParams(List<IndexParam> indexParams) {
            this.indexParams = indexParams;
            return this;
        }

        /**
         * Sets whether the call waits until the index is built.
         *
         * @param sync {@code true} to wait for the index to complete
         * @return this builder
         */
        public CreateIndexReqBuilder sync(Boolean sync) {
            this.sync = sync;
            return this;
        }

        /**
         * Sets the timeout in milliseconds for waiting the index to complete.
         *
         * @param timeout the timeout value in milliseconds
         * @return this builder
         */
        public CreateIndexReqBuilder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Builds the {@code CreateIndexReq}.
         *
         * @return the built request
         */
        public CreateIndexReq build() {
            return new CreateIndexReq(this);
        }
    }
}
