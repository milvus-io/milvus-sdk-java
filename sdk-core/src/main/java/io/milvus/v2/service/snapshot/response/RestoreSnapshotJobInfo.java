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

package io.milvus.v2.service.snapshot.response;

/**
 * Holds the state and progress of a restore snapshot job.
 */
public class RestoreSnapshotJobInfo {
    public static final String STATE_NONE = "RestoreSnapshotNone";
    public static final String STATE_PENDING = "RestoreSnapshotPending";
    public static final String STATE_EXECUTING = "RestoreSnapshotExecuting";
    public static final String STATE_COMPLETED = "RestoreSnapshotCompleted";
    public static final String STATE_FAILED = "RestoreSnapshotFailed";

    private Long jobId;
    private String snapshotName;
    private String dbName;
    private String collectionName;
    private String state;
    private Integer progress;
    private String reason;
    private Long startTime;
    private Long timeCost;

    private RestoreSnapshotJobInfo(RestoreSnapshotJobInfoBuilder builder) {
        this.jobId = builder.jobId;
        this.snapshotName = builder.snapshotName;
        this.dbName = builder.dbName;
        this.collectionName = builder.collectionName;
        this.state = builder.state;
        this.progress = builder.progress;
        this.reason = builder.reason;
        this.startTime = builder.startTime;
        this.timeCost = builder.timeCost;
    }

    public static RestoreSnapshotJobInfoBuilder builder() {
        return new RestoreSnapshotJobInfoBuilder();
    }

    /**
     * Returns the restore snapshot job ID.
     *
     * @return the job ID
     */
    public Long getJobId() {
        return jobId;
    }

    /**
     * Sets the restore snapshot job ID.
     *
     * @param jobId the job ID
     */
    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    /**
     * Returns the name of the snapshot being restored.
     *
     * @return the snapshot name
     */
    public String getSnapshotName() {
        return snapshotName;
    }

    /**
     * Sets the name of the snapshot being restored.
     *
     * @param snapshotName the snapshot name
     */
    public void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }

    /**
     * Returns the name of the target database.
     *
     * @return the target database name
     */
    public String getDbName() {
        return dbName;
    }

    /**
     * Sets the name of the target database.
     *
     * @param dbName the target database name
     */
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    /**
     * Returns the name of the target collection.
     *
     * @return the target collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the name of the target collection.
     *
     * @param collectionName the target collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the current state of the restore job.
     *
     * @return one of the {@code STATE_*} constants
     */
    public String getState() {
        return state;
    }

    /**
     * Sets the current state of the restore job.
     *
     * @param state one of the {@code STATE_*} constants
     */
    public void setState(String state) {
        this.state = state;
    }

    /**
     * Returns the progress of the restore job as a percentage.
     *
     * @return the progress percentage
     */
    public Integer getProgress() {
        return progress;
    }

    /**
     * Sets the progress of the restore job as a percentage.
     *
     * @param progress the progress percentage
     */
    public void setProgress(Integer progress) {
        this.progress = progress;
    }

    /**
     * Returns the failure reason when the job has failed.
     *
     * @return the failure reason
     */
    public String getReason() {
        return reason;
    }

    /**
     * Sets the failure reason when the job has failed.
     *
     * @param reason the failure reason
     */
    public void setReason(String reason) {
        this.reason = reason;
    }

    /**
     * Returns the start time of the restore job.
     *
     * @return the start time
     */
    public Long getStartTime() {
        return startTime;
    }

    /**
     * Sets the start time of the restore job.
     *
     * @param startTime the start time
     */
    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    /**
     * Returns the time cost of the restore job.
     *
     * @return the time cost
     */
    public Long getTimeCost() {
        return timeCost;
    }

    /**
     * Sets the time cost of the restore job.
     *
     * @param timeCost the time cost
     */
    public void setTimeCost(Long timeCost) {
        this.timeCost = timeCost;
    }

    @Override
    public String toString() {
        return "RestoreSnapshotJobInfo{" +
                "jobId=" + jobId +
                ", snapshotName='" + snapshotName + '\'' +
                ", dbName='" + dbName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", state='" + state + '\'' +
                ", progress=" + progress +
                ", reason='" + reason + '\'' +
                ", startTime=" + startTime +
                ", timeCost=" + timeCost +
                '}';
    }

    public static class RestoreSnapshotJobInfoBuilder {
        private Long jobId;
        private String snapshotName;
        private String dbName;
        private String collectionName;
        private String state;
        private Integer progress;
        private String reason;
        private Long startTime;
        private Long timeCost;

        /**
         * Sets the restore snapshot job ID.
         *
         * @param jobId the job ID
         * @return this builder
         */
        public RestoreSnapshotJobInfoBuilder jobId(Long jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Sets the name of the snapshot being restored.
         *
         * @param snapshotName the snapshot name
         * @return this builder
         */
        public RestoreSnapshotJobInfoBuilder snapshotName(String snapshotName) {
            this.snapshotName = snapshotName;
            return this;
        }

        /**
         * Sets the name of the target database.
         *
         * @param dbName the target database name
         * @return this builder
         */
        public RestoreSnapshotJobInfoBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        /**
         * Sets the name of the target collection.
         *
         * @param collectionName the target collection name
         * @return this builder
         */
        public RestoreSnapshotJobInfoBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the current state of the restore job.
         *
         * @param state one of the {@code STATE_*} constants
         * @return this builder
         */
        public RestoreSnapshotJobInfoBuilder state(String state) {
            this.state = state;
            return this;
        }

        /**
         * Sets the progress of the restore job as a percentage.
         *
         * @param progress the progress percentage
         * @return this builder
         */
        public RestoreSnapshotJobInfoBuilder progress(Integer progress) {
            this.progress = progress;
            return this;
        }

        /**
         * Sets the failure reason when the job has failed.
         *
         * @param reason the failure reason
         * @return this builder
         */
        public RestoreSnapshotJobInfoBuilder reason(String reason) {
            this.reason = reason;
            return this;
        }

        /**
         * Sets the start time of the restore job.
         *
         * @param startTime the start time
         * @return this builder
         */
        public RestoreSnapshotJobInfoBuilder startTime(Long startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Sets the time cost of the restore job.
         *
         * @param timeCost the time cost
         * @return this builder
         */
        public RestoreSnapshotJobInfoBuilder timeCost(Long timeCost) {
            this.timeCost = timeCost;
            return this;
        }

        /**
         * Builds a {@link RestoreSnapshotJobInfo}.
         *
         * @return the built job info
         */
        public RestoreSnapshotJobInfo build() {
            return new RestoreSnapshotJobInfo(this);
        }
    }
}
