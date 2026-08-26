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
 * Request parameters for the {@code flushAll} API.
 */
public class FlushAllReq {
    private String databaseName;
    private Long waitFlushedTimeoutMs;

    private FlushAllReq(FlushAllReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.waitFlushedTimeoutMs = builder.waitFlushedTimeoutMs;
    }

    public static FlushAllReqBuilder builder() {
        return new FlushAllReqBuilder();
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
     * Returns the timeout (in milliseconds) to wait for the flush to complete.
     *
     * @return the timeout in milliseconds
     */
    public Long getWaitFlushedTimeoutMs() {
        return waitFlushedTimeoutMs;
    }

    /**
     * Sets the timeout (in milliseconds) to wait for the flush to complete.
     *
     * @param waitFlushedTimeoutMs the timeout in milliseconds
     */
    public void setWaitFlushedTimeoutMs(Long waitFlushedTimeoutMs) {
        this.waitFlushedTimeoutMs = waitFlushedTimeoutMs;
    }

    @Override
    public String toString() {
        return "FlushAllReq{" +
                "databaseName='" + databaseName + '\'' +
                ", waitFlushedTimeoutMs=" + waitFlushedTimeoutMs +
                '}';
    }

    public static class FlushAllReqBuilder {
        private String databaseName;
        private Long waitFlushedTimeoutMs = 0L;

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public FlushAllReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the timeout (in milliseconds) to wait for the flush to complete.
         *
         * @param waitFlushedTimeoutMs the timeout in milliseconds
         * @return this builder
         */
        public FlushAllReqBuilder waitFlushedTimeoutMs(Long waitFlushedTimeoutMs) {
            this.waitFlushedTimeoutMs = waitFlushedTimeoutMs;
            return this;
        }

        /**
         * Builds the {@code FlushAllReq}.
         *
         * @return the constructed {@code FlushAllReq}
         */
        public FlushAllReq build() {
            return new FlushAllReq(this);
        }
    }
}
