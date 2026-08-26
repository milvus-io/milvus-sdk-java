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
 * Request parameters for the {@code restoreSnapshot} API.
 */
public class RestoreSnapshotReq {
    private String snapshotName;
    private String sourceCollectionName;
    private String targetCollectionName;
    private String sourceDbName;
    private String targetDbName;

    private RestoreSnapshotReq(RestoreSnapshotReqBuilder builder) {
        this.snapshotName = builder.snapshotName;
        this.sourceCollectionName = builder.sourceCollectionName;
        this.targetCollectionName = builder.targetCollectionName;
        this.sourceDbName = builder.sourceDbName;
        this.targetDbName = builder.targetDbName;
    }

    public static RestoreSnapshotReqBuilder builder() {
        return new RestoreSnapshotReqBuilder();
    }

    /**
     * Returns the snapshot name to restore from.
     *
     * @return the snapshot name
     */
    public String getSnapshotName() {
        return snapshotName;
    }

    /**
     * Sets the snapshot name to restore from.
     *
     * @param snapshotName the snapshot name
     */
    public void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }

    /**
     * Returns the source collection name.
     *
     * @return the source collection name
     */
    public String getSourceCollectionName() {
        return sourceCollectionName;
    }

    /**
     * Sets the source collection name.
     *
     * @param sourceCollectionName the source collection name
     */
    public void setSourceCollectionName(String sourceCollectionName) {
        this.sourceCollectionName = sourceCollectionName;
    }

    /**
     * Returns the target collection name.
     *
     * @return the target collection name
     */
    public String getTargetCollectionName() {
        return targetCollectionName;
    }

    /**
     * Sets the target collection name.
     *
     * @param targetCollectionName the target collection name
     */
    public void setTargetCollectionName(String targetCollectionName) {
        this.targetCollectionName = targetCollectionName;
    }

    /**
     * Returns the source database name.
     *
     * @return the source database name
     */
    public String getSourceDbName() {
        return sourceDbName;
    }

    /**
     * Sets the source database name.
     *
     * @param sourceDbName the source database name
     */
    public void setSourceDbName(String sourceDbName) {
        this.sourceDbName = sourceDbName;
    }

    /**
     * Returns the target database name.
     *
     * @return the target database name
     */
    public String getTargetDbName() {
        return targetDbName;
    }

    /**
     * Sets the target database name.
     *
     * @param targetDbName the target database name
     */
    public void setTargetDbName(String targetDbName) {
        this.targetDbName = targetDbName;
    }

    @Override
    public String toString() {
        return "RestoreSnapshotReq{" +
                "snapshotName='" + snapshotName + '\'' +
                ", sourceCollectionName='" + sourceCollectionName + '\'' +
                ", targetCollectionName='" + targetCollectionName + '\'' +
                ", sourceDbName='" + sourceDbName + '\'' +
                ", targetDbName='" + targetDbName + '\'' +
                '}';
    }

    public static class RestoreSnapshotReqBuilder {
        private String snapshotName;
        private String sourceCollectionName;
        private String targetCollectionName;
        private String sourceDbName = "";
        private String targetDbName = "";

        /**
         * Sets the snapshot name to restore from.
         *
         * @param snapshotName the snapshot name
         * @return this builder
         */
        public RestoreSnapshotReqBuilder snapshotName(String snapshotName) {
            this.snapshotName = snapshotName;
            return this;
        }

        /**
         * Sets the source collection name.
         *
         * @param sourceCollectionName the source collection name
         * @return this builder
         */
        public RestoreSnapshotReqBuilder sourceCollectionName(String sourceCollectionName) {
            this.sourceCollectionName = sourceCollectionName;
            return this;
        }

        /**
         * Sets the target collection name.
         *
         * @param targetCollectionName the target collection name
         * @return this builder
         */
        public RestoreSnapshotReqBuilder targetCollectionName(String targetCollectionName) {
            this.targetCollectionName = targetCollectionName;
            return this;
        }

        /**
         * Sets the source database name.
         *
         * @param sourceDbName the source database name
         * @return this builder
         */
        public RestoreSnapshotReqBuilder sourceDbName(String sourceDbName) {
            this.sourceDbName = sourceDbName;
            return this;
        }

        /**
         * Sets the target database name.
         *
         * @param targetDbName the target database name
         * @return this builder
         */
        public RestoreSnapshotReqBuilder targetDbName(String targetDbName) {
            this.targetDbName = targetDbName;
            return this;
        }

        /**
         * Builds a {@link RestoreSnapshotReq}.
         *
         * @return the built request
         */
        public RestoreSnapshotReq build() {
            return new RestoreSnapshotReq(this);
        }
    }
}
