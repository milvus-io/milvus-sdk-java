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
 * Request parameters for the {@code releaseCollection} API.
 */
public class ReleaseCollectionReq {
    private String databaseName;
    private String collectionName;
    @Deprecated
    private Boolean async = Boolean.TRUE;
    private Long timeout = 60000L;

    private ReleaseCollectionReq(ReleaseCollectionReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.async = builder.async;
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
     * Returns whether the release operation is asynchronous.
     *
     * @return {@code true} if the operation is asynchronous
     * @deprecated the async flag is no longer used; control the wait with the timeout instead
     */
    @Deprecated
    public Boolean getAsync() {
        return async;
    }

    /**
     * Sets whether the release operation is asynchronous.
     *
     * @param async {@code true} to run the operation asynchronously
     * @deprecated the async flag is no longer used; control the wait with the timeout instead
     */
    @Deprecated
    public void setAsync(Boolean async) {
        this.async = async;
    }

    /**
     * Returns the timeout for the release operation, in milliseconds.
     *
     * @return the timeout in milliseconds
     */
    public Long getTimeout() {
        return timeout;
    }

    /**
     * Sets the timeout for the release operation, in milliseconds.
     *
     * @param timeout the timeout in milliseconds
     */
    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    @Override
    public String toString() {
        return "ReleaseCollectionReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", async=" + async +
                ", timeout=" + timeout +
                '}';
    }

    /**
     * Creates a new builder for {@link ReleaseCollectionReq}.
     *
     * @return the builder
     */
    public static ReleaseCollectionReqBuilder builder() {
        return new ReleaseCollectionReqBuilder();
    }

    public static class ReleaseCollectionReqBuilder {
        private String databaseName;
        private String collectionName;
        private Boolean async = Boolean.TRUE;
        private Long timeout = 60000L;

        private ReleaseCollectionReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public ReleaseCollectionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public ReleaseCollectionReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets whether the release operation is asynchronous.
         *
         * @param async {@code true} to run the operation asynchronously
         * @return this builder
         * @deprecated the async flag is no longer used; control the wait with the timeout instead
         */
        @Deprecated
        public ReleaseCollectionReqBuilder async(Boolean async) {
            this.async = async;
            return this;
        }

        /**
         * Sets the timeout for the release operation, in milliseconds.
         *
         * @param timeout the timeout in milliseconds
         * @return this builder
         */
        public ReleaseCollectionReqBuilder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Builds a {@link ReleaseCollectionReq} with the configured parameters.
         *
         * @return the request
         */
        public ReleaseCollectionReq build() {
            return new ReleaseCollectionReq(this);
        }
    }
}
