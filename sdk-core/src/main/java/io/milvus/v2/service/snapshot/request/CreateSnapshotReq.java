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

public class CreateSnapshotReq {
    private String databaseName;
    private String collectionName;
    private String snapshotName;
    private String description;
    private Long compactionProtectionSeconds;

    private CreateSnapshotReq(CreateSnapshotReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.snapshotName = builder.snapshotName;
        this.description = builder.description;
        this.compactionProtectionSeconds = builder.compactionProtectionSeconds;
    }

    public static CreateSnapshotReqBuilder builder() {
        return new CreateSnapshotReqBuilder();
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

    public String getSnapshotName() {
        return snapshotName;
    }

    public void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Long getCompactionProtectionSeconds() {
        return compactionProtectionSeconds;
    }

    public void setCompactionProtectionSeconds(Long compactionProtectionSeconds) {
        this.compactionProtectionSeconds = compactionProtectionSeconds;
    }

    @Override
    public String toString() {
        return "CreateSnapshotReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", snapshotName='" + snapshotName + '\'' +
                ", description='" + description + '\'' +
                ", compactionProtectionSeconds=" + compactionProtectionSeconds +
                '}';
    }

    public static class CreateSnapshotReqBuilder {
        private String databaseName = "";
        private String collectionName;
        private String snapshotName;
        private String description = "";
        private Long compactionProtectionSeconds = 0L;

        public CreateSnapshotReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public CreateSnapshotReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public CreateSnapshotReqBuilder snapshotName(String snapshotName) {
            this.snapshotName = snapshotName;
            return this;
        }

        public CreateSnapshotReqBuilder description(String description) {
            this.description = description;
            return this;
        }

        public CreateSnapshotReqBuilder compactionProtectionSeconds(Long compactionProtectionSeconds) {
            this.compactionProtectionSeconds = compactionProtectionSeconds;
            return this;
        }

        public CreateSnapshotReq build() {
            return new CreateSnapshotReq(this);
        }
    }
}
