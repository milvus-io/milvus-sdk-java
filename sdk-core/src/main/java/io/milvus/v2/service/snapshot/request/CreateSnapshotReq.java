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
 * Request parameters for the {@code createSnapshot} API.
 */
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

    /**
     * Creates a new builder for {@code CreateSnapshotReq}.
     *
     * @return the builder
     */
    public static CreateSnapshotReqBuilder builder() {
        return new CreateSnapshotReqBuilder();
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
     * Returns the name of the snapshot to be created.
     *
     * @return the snapshot name
     */
    public String getSnapshotName() {
        return snapshotName;
    }

    /**
     * Sets the name of the snapshot to be created.
     *
     * @param snapshotName the snapshot name
     */
    public void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }

    /**
     * Returns the description of the snapshot.
     *
     * @return the snapshot description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets the description of the snapshot.
     *
     * @param description the snapshot description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Returns the duration in seconds for which the snapshot data is protected from compaction.
     *
     * @return the compaction protection duration in seconds
     */
    public Long getCompactionProtectionSeconds() {
        return compactionProtectionSeconds;
    }

    /**
     * Sets the duration in seconds for which the snapshot data is protected from compaction.
     *
     * @param compactionProtectionSeconds the compaction protection duration in seconds
     */
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

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public CreateSnapshotReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public CreateSnapshotReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the name of the snapshot to be created.
         *
         * @param snapshotName the snapshot name
         * @return this builder
         */
        public CreateSnapshotReqBuilder snapshotName(String snapshotName) {
            this.snapshotName = snapshotName;
            return this;
        }

        /**
         * Sets the description of the snapshot.
         *
         * @param description the snapshot description
         * @return this builder
         */
        public CreateSnapshotReqBuilder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the duration in seconds for which the snapshot data is protected from compaction.
         *
         * @param compactionProtectionSeconds the compaction protection duration in seconds
         * @return this builder
         */
        public CreateSnapshotReqBuilder compactionProtectionSeconds(Long compactionProtectionSeconds) {
            this.compactionProtectionSeconds = compactionProtectionSeconds;
            return this;
        }

        /**
         * Builds the {@code CreateSnapshotReq}.
         *
         * @return the built request
         */
        public CreateSnapshotReq build() {
            return new CreateSnapshotReq(this);
        }
    }
}
