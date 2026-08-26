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

package io.milvus.v2.service.snapshot.request;

/**
 * Request parameters for the {@code pinSnapshotData} API.
 * Pinning snapshot data protects the underlying data from garbage collection for a limited time.
 */
public class PinSnapshotDataReq {
    private String snapshotName;
    private String databaseName;
    private String collectionName;
    private Long ttlSeconds;

    private PinSnapshotDataReq(PinSnapshotDataReqBuilder builder) {
        this.snapshotName = builder.snapshotName;
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.ttlSeconds = builder.ttlSeconds;
    }

    public static PinSnapshotDataReqBuilder builder() {
        return new PinSnapshotDataReqBuilder();
    }

    /**
     * Returns the snapshot name.
     *
     * @return the snapshot name
     */
    public String getSnapshotName() {
        return snapshotName;
    }

    /**
     * Sets the snapshot name.
     *
     * @param snapshotName the snapshot name
     */
    public void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
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
     * Returns the time-to-live of the pin in seconds.
     *
     * @return the pin TTL in seconds
     */
    public Long getTtlSeconds() {
        return ttlSeconds;
    }

    /**
     * Sets the time-to-live of the pin in seconds.
     *
     * @param ttlSeconds the pin TTL in seconds
     */
    public void setTtlSeconds(Long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }

    @Override
    public String toString() {
        return "PinSnapshotDataReq{" +
                "snapshotName='" + snapshotName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", ttlSeconds=" + ttlSeconds +
                '}';
    }

    public static class PinSnapshotDataReqBuilder {
        private String snapshotName;
        private String databaseName = "";
        private String collectionName;
        private Long ttlSeconds = 0L;

        /**
         * Sets the snapshot name.
         *
         * @param snapshotName the snapshot name
         * @return this builder
         */
        public PinSnapshotDataReqBuilder snapshotName(String snapshotName) {
            this.snapshotName = snapshotName;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public PinSnapshotDataReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public PinSnapshotDataReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the time-to-live of the pin in seconds.
         *
         * @param ttlSeconds the pin TTL in seconds
         * @return this builder
         */
        public PinSnapshotDataReqBuilder ttlSeconds(Long ttlSeconds) {
            this.ttlSeconds = ttlSeconds;
            return this;
        }

        /**
         * Builds a {@link PinSnapshotDataReq}.
         *
         * @return the built request
         */
        public PinSnapshotDataReq build() {
            return new PinSnapshotDataReq(this);
        }
    }
}
