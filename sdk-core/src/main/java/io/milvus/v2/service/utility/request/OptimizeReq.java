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

package io.milvus.v2.service.utility.request;

/**
 * Request parameters for the {@code optimize} API.
 */
public class OptimizeReq {
    private String databaseName;
    private String collectionName;
    private String targetSize; // e.g. "512MB", "1GB", null for server default
    private boolean async = false; // false = block until done (default), true = return task immediately
    private Long timeout; // ms, null = no timeout

    private OptimizeReq(OptimizeReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.targetSize = builder.targetSize;
        this.async = builder.async;
        this.timeout = builder.timeout;
    }

    public static OptimizeReqBuilder builder() {
        return new OptimizeReqBuilder();
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
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Returns the target segment size for the optimization.
     *
     * @return the target segment size, e.g. {@code "512MB"} or {@code "1GB"}, or {@code null} for the server default
     */
    public String getTargetSize() {
        return targetSize;
    }

    /**
     * Returns whether the optimization is executed asynchronously.
     *
     * @return {@code true} to return the task immediately, {@code false} to block until done
     */
    public boolean isAsync() {
        return async;
    }

    /**
     * Returns the timeout (in milliseconds) for the optimization.
     *
     * @return the timeout in milliseconds, or {@code null} for no timeout
     */
    public Long getTimeout() {
        return timeout;
    }

    @Override
    public String toString() {
        return "OptimizeReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", targetSize='" + targetSize + '\'' +
                ", async=" + async +
                ", timeout=" + timeout +
                '}';
    }

    public static class OptimizeReqBuilder {
        private String databaseName;
        private String collectionName;
        private String targetSize;
        private boolean async = false;
        private Long timeout;

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public OptimizeReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public OptimizeReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the target segment size for the optimization.
         *
         * @param targetSize the target segment size, e.g. {@code "512MB"} or {@code "1GB"}, or {@code null} for the server default
         * @return this builder
         */
        public OptimizeReqBuilder targetSize(String targetSize) {
            this.targetSize = targetSize;
            return this;
        }

        /**
         * Sets whether the optimization is executed asynchronously.
         *
         * @param async {@code true} to return the task immediately, {@code false} to block until done
         * @return this builder
         */
        public OptimizeReqBuilder async(boolean async) {
            this.async = async;
            return this;
        }

        /**
         * Sets the timeout (in milliseconds) for the optimization.
         *
         * @param timeout the timeout in milliseconds, or {@code null} for no timeout
         * @return this builder
         */
        public OptimizeReqBuilder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Builds the {@code OptimizeReq}.
         *
         * @return the constructed {@code OptimizeReq}
         */
        public OptimizeReq build() {
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("collectionName cannot be null or empty");
            }
            return new OptimizeReq(this);
        }
    }
}
