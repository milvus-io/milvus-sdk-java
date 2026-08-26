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

import java.util.ArrayList;
import java.util.List;

/**
 * Request parameters for the {@code flush} API.
 */
public class FlushReq {
    private String databaseName;
    private List<String> collectionNames;
    private Long waitFlushedTimeoutMs; // 0 - waiting util flush task is done

    private FlushReq(FlushReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionNames = builder.collectionNames;
        this.waitFlushedTimeoutMs = builder.waitFlushedTimeoutMs;
    }

    public static FlushReqBuilder builder() {
        return new FlushReqBuilder();
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
     * Returns the names of the collections to flush.
     *
     * @return the collection names
     */
    public List<String> getCollectionNames() {
        return collectionNames;
    }

    /**
     * Sets the names of the collections to flush.
     *
     * @param collectionNames the collection names
     */
    public void setCollectionNames(List<String> collectionNames) {
        this.collectionNames = collectionNames;
    }

    /**
     * Returns the timeout (in milliseconds) to wait for the flush to complete.
     *
     * @return the timeout in milliseconds, {@code 0} waits until the flush task is done
     */
    public Long getWaitFlushedTimeoutMs() {
        return waitFlushedTimeoutMs;
    }

    /**
     * Sets the timeout (in milliseconds) to wait for the flush to complete.
     *
     * @param waitFlushedTimeoutMs the timeout in milliseconds, {@code 0} waits until the flush task is done
     */
    public void setWaitFlushedTimeoutMs(Long waitFlushedTimeoutMs) {
        this.waitFlushedTimeoutMs = waitFlushedTimeoutMs;
    }

    @Override
    public String toString() {
        return "FlushReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionNames=" + collectionNames +
                ", waitFlushedTimeoutMs=" + waitFlushedTimeoutMs +
                '}';
    }

    public static class FlushReqBuilder {
        private String databaseName;
        private List<String> collectionNames = new ArrayList<>();
        private Long waitFlushedTimeoutMs = 0L; // 0 - waiting util flush task is done

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public FlushReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the names of the collections to flush.
         *
         * @param collectionNames the collection names
         * @return this builder
         */
        public FlushReqBuilder collectionNames(List<String> collectionNames) {
            this.collectionNames = collectionNames;
            return this;
        }

        /**
         * Sets the timeout (in milliseconds) to wait for the flush to complete.
         *
         * @param waitFlushedTimeoutMs the timeout in milliseconds, {@code 0} waits until the flush task is done
         * @return this builder
         */
        public FlushReqBuilder waitFlushedTimeoutMs(Long waitFlushedTimeoutMs) {
            this.waitFlushedTimeoutMs = waitFlushedTimeoutMs;
            return this;
        }

        /**
         * Builds the {@code FlushReq}.
         *
         * @return the constructed {@code FlushReq}
         */
        public FlushReq build() {
            return new FlushReq(this);
        }
    }
}
