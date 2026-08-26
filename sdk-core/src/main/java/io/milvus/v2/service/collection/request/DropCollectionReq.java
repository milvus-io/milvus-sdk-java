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
 * Request parameters for the {@code dropCollection} API.
 */
public class DropCollectionReq {
    private String databaseName;
    private String collectionName;
    @Deprecated
    private Boolean async;
    private Long timeout;

    private DropCollectionReq(DropCollectionReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.async = builder.async != null ? builder.async : Boolean.TRUE;
        this.timeout = builder.timeout != null ? builder.timeout : 60000L;
    }

    /**
     * Creates a new builder for {@link DropCollectionReq}.
     *
     * @return the builder
     */
    public static DropCollectionReqBuilder builder() {
        return new DropCollectionReqBuilder();
    }

    // Getters
    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
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
     * Returns whether the drop operation is asynchronous.
     *
     * @return {@code true} if the operation is asynchronous
     * @deprecated use the timeout to control the wait behavior instead
     */
    @Deprecated
    public Boolean getAsync() {
        return async;
    }

    /**
     * Returns the timeout for the drop operation, in milliseconds.
     *
     * @return the timeout in milliseconds
     */
    public Long getTimeout() {
        return timeout;
    }

    // Setters
    /**
     * Sets the database name.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
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
     * Sets whether the drop operation is asynchronous.
     *
     * @param async {@code true} to run the operation asynchronously
     * @deprecated use the timeout to control the wait behavior instead
     */
    @Deprecated
    public void setAsync(Boolean async) {
        this.async = async;
    }

    /**
     * Sets the timeout for the drop operation, in milliseconds.
     *
     * @param timeout the timeout in milliseconds
     */
    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    @Override
    public String toString() {
        return "DropCollectionReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", async=" + async +
                ", timeout=" + timeout +
                '}';
    }

    public static class DropCollectionReqBuilder {
        private String databaseName;
        private String collectionName;
        private Boolean async;
        private Long timeout;

        private DropCollectionReqBuilder() {
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public DropCollectionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public DropCollectionReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets whether the drop operation is asynchronous.
         *
         * @param async {@code true} to run the operation asynchronously
         * @return this builder
         * @deprecated use the timeout to control the wait behavior instead
         */
        @Deprecated
        public DropCollectionReqBuilder async(Boolean async) {
            this.async = async;
            return this;
        }

        /**
         * Sets the timeout for the drop operation, in milliseconds.
         *
         * @param timeout the timeout in milliseconds
         * @return this builder
         */
        public DropCollectionReqBuilder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Builds a {@link DropCollectionReq} with the configured parameters.
         *
         * @return the request
         */
        public DropCollectionReq build() {
            return new DropCollectionReq(this);
        }
    }
}
