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

    public String getSnapshotName() {
        return snapshotName;
    }

    public void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public Long getTtlSeconds() {
        return ttlSeconds;
    }

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

        public PinSnapshotDataReqBuilder snapshotName(String snapshotName) {
            this.snapshotName = snapshotName;
            return this;
        }

        public PinSnapshotDataReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public PinSnapshotDataReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public PinSnapshotDataReqBuilder ttlSeconds(Long ttlSeconds) {
            this.ttlSeconds = ttlSeconds;
            return this;
        }

        public PinSnapshotDataReq build() {
            return new PinSnapshotDataReq(this);
        }
    }
}
