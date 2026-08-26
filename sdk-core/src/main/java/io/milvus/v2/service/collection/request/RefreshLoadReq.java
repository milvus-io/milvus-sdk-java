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
 * Request parameters for the {@code refreshLoad} API, which refreshes the memory
 * replicas of a loaded collection so that the latest data becomes searchable.
 */
public class RefreshLoadReq {
    private String databaseName;
    private String collectionName;
    private Boolean async = Boolean.TRUE;
    private Boolean sync = Boolean.TRUE; // wait the collection to be fully loaded. "async" is deprecated, use "sync" instead
    private Long timeout = 60000L; // timeout value for waiting the collection to be fully loaded

    private RefreshLoadReq(RefreshLoadReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.async = builder.async;
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
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns whether the refresh operation is asynchronous.
     *
     * @return {@code true} if the operation is asynchronous
     * @deprecated use {@link #getSync()} instead
     */
    public Boolean getAsync() {
        return async;
    }

    /**
     * Sets whether the refresh operation is asynchronous.
     *
     * @param async {@code true} to run the operation asynchronously
     * @deprecated use {@link #setSync(Boolean)} instead
     */
    public void setAsync(Boolean async) {
        this.async = async;
        this.sync = !async;
    }

    /**
     * Returns whether to wait until the collection is fully loaded.
     *
     * @return {@code true} if the operation waits for the collection to be loaded
     */
    public Boolean getSync() {
        return sync;
    }

    /**
     * Sets whether to wait until the collection is fully loaded.
     *
     * @param sync {@code true} to wait for the collection to be loaded
     */
    public void setSync(Boolean sync) {
        this.sync = sync;
        this.async = !sync;
    }

    /**
     * Returns the timeout value for waiting the collection to be fully loaded, in milliseconds.
     *
     * @return the timeout in milliseconds
     */
    public Long getTimeout() {
        return timeout;
    }

    /**
     * Sets the timeout value for waiting the collection to be fully loaded, in milliseconds.
     *
     * @param timeout the timeout in milliseconds
     */
    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    @Override
    public String toString() {
        return "RefreshLoadReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", async=" + async +
                ", sync=" + sync +
                ", timeout=" + timeout +
                '}';
    }

    /**
     * Creates a new builder for {@link RefreshLoadReq}.
     *
     * @return the builder
     */
    public static RefreshLoadReqBuilder builder() {
        return new RefreshLoadReqBuilder();
    }

    public static class RefreshLoadReqBuilder {
        private String databaseName;
        private String collectionName;
        private Boolean async = Boolean.TRUE;
        private Boolean sync = Boolean.TRUE;
        private Long timeout = 60000L;

        private RefreshLoadReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public RefreshLoadReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public RefreshLoadReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets whether the refresh operation is asynchronous.
         *
         * @param async {@code true} to run the operation asynchronously
         * @return this builder
         * @deprecated use {@link #sync(Boolean)} instead
         */
        public RefreshLoadReqBuilder async(Boolean async) {
            this.async = async;
            this.sync = !async;
            return this;
        }

        /**
         * Sets whether to wait until the collection is fully loaded.
         *
         * @param sync {@code true} to wait for the collection to be loaded
         * @return this builder
         */
        public RefreshLoadReqBuilder sync(Boolean sync) {
            this.sync = sync;
            this.async = !sync;
            return this;
        }

        /**
         * Sets the timeout value for waiting the collection to be fully loaded, in milliseconds.
         *
         * @param timeout the timeout in milliseconds
         * @return this builder
         */
        public RefreshLoadReqBuilder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Builds a {@link RefreshLoadReq} with the configured parameters.
         *
         * @return the request
         */
        public RefreshLoadReq build() {
            return new RefreshLoadReq(this);
        }
    }
}
