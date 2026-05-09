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

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Integer getProgress() {
        return progress;
    }

    public void setProgress(Integer progress) {
        this.progress = progress;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getTimeCost() {
        return timeCost;
    }

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

        public RestoreSnapshotJobInfoBuilder jobId(Long jobId) {
            this.jobId = jobId;
            return this;
        }

        public RestoreSnapshotJobInfoBuilder snapshotName(String snapshotName) {
            this.snapshotName = snapshotName;
            return this;
        }

        public RestoreSnapshotJobInfoBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public RestoreSnapshotJobInfoBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public RestoreSnapshotJobInfoBuilder state(String state) {
            this.state = state;
            return this;
        }

        public RestoreSnapshotJobInfoBuilder progress(Integer progress) {
            this.progress = progress;
            return this;
        }

        public RestoreSnapshotJobInfoBuilder reason(String reason) {
            this.reason = reason;
            return this;
        }

        public RestoreSnapshotJobInfoBuilder startTime(Long startTime) {
            this.startTime = startTime;
            return this;
        }

        public RestoreSnapshotJobInfoBuilder timeCost(Long timeCost) {
            this.timeCost = timeCost;
            return this;
        }

        public RestoreSnapshotJobInfo build() {
            return new RestoreSnapshotJobInfo(this);
        }
    }
}
