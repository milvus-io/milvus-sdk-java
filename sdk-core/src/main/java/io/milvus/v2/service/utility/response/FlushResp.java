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

package io.milvus.v2.service.utility.response;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Response returned by the {@code flush} API.
 */
public class FlushResp {
    private String databaseName;
    private Map<String, List<Long>> collectionSegmentIDs;
    private Map<String, Long> collectionFlushTs;

    private FlushResp(FlushRespBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionSegmentIDs = builder.collectionSegmentIDs;
        this.collectionFlushTs = builder.collectionFlushTs;
    }

    public static FlushRespBuilder builder() {
        return new FlushRespBuilder();
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
     * Returns the IDs of the segments flushed for each collection.
     *
     * @return a map of collection name to flushed segment IDs
     */
    public Map<String, List<Long>> getCollectionSegmentIDs() {
        return collectionSegmentIDs;
    }

    /**
     * Sets the IDs of the segments flushed for each collection.
     *
     * @param collectionSegmentIDs a map of collection name to flushed segment IDs
     */
    public void setCollectionSegmentIDs(Map<String, List<Long>> collectionSegmentIDs) {
        this.collectionSegmentIDs = collectionSegmentIDs;
    }

    /**
     * Returns the flush timestamps for each collection.
     *
     * @return a map of collection name to flush timestamp
     */
    public Map<String, Long> getCollectionFlushTs() {
        return collectionFlushTs;
    }

    /**
     * Sets the flush timestamps for each collection.
     *
     * @param collectionFlushTs a map of collection name to flush timestamp
     */
    public void setCollectionFlushTs(Map<String, Long> collectionFlushTs) {
        this.collectionFlushTs = collectionFlushTs;
    }

    @Override
    public String toString() {
        return "FlushResp{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionSegmentIDs=" + collectionSegmentIDs +
                ", collectionFlushTs=" + collectionFlushTs +
                '}';
    }

    public static class FlushRespBuilder {
        private String databaseName = "";
        private Map<String, List<Long>> collectionSegmentIDs = new HashMap<>();
        private Map<String, Long> collectionFlushTs = new HashMap<>();

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public FlushRespBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the IDs of the segments flushed for each collection.
         *
         * @param collectionSegmentIDs a map of collection name to flushed segment IDs
         * @return this builder
         */
        public FlushRespBuilder collectionSegmentIDs(Map<String, List<Long>> collectionSegmentIDs) {
            this.collectionSegmentIDs = collectionSegmentIDs;
            return this;
        }

        /**
         * Sets the flush timestamps for each collection.
         *
         * @param collectionFlushTs a map of collection name to flush timestamp
         * @return this builder
         */
        public FlushRespBuilder collectionFlushTs(Map<String, Long> collectionFlushTs) {
            this.collectionFlushTs = collectionFlushTs;
            return this;
        }

        /**
         * Builds the {@code FlushResp}.
         *
         * @return the constructed {@code FlushResp}
         */
        public FlushResp build() {
            return new FlushResp(this);
        }
    }
}
