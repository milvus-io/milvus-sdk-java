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

public class RefreshExternalCollectionJobInfo {
    private final long jobId;
    private final String collectionName;
    private final String state;
    private final int progress;
    private final String reason;
    private final String externalSource;
    private final long startTime;
    private final long endTime;

    private RefreshExternalCollectionJobInfo(RefreshExternalCollectionJobInfoBuilder builder) {
        this.jobId = builder.jobId;
        this.collectionName = builder.collectionName;
        this.state = builder.state;
        this.progress = builder.progress;
        this.reason = builder.reason;
        this.externalSource = builder.externalSource;
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
    }

    public static RefreshExternalCollectionJobInfoBuilder builder() {
        return new RefreshExternalCollectionJobInfoBuilder();
    }

    public long getJobId() {
        return jobId;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getState() {
        return state;
    }

    public int getProgress() {
        return progress;
    }

    public String getReason() {
        return reason;
    }

    public String getExternalSource() {
        return externalSource;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    @Override
    public String toString() {
        return "RefreshExternalCollectionJobInfo{" +
                "jobId=" + jobId +
                ", collectionName='" + collectionName + '\'' +
                ", state='" + state + '\'' +
                ", progress=" + progress +
                ", reason='" + reason + '\'' +
                ", externalSource='" + externalSource + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }

    public static class RefreshExternalCollectionJobInfoBuilder {
        private long jobId;
        private String collectionName;
        private String state;
        private int progress;
        private String reason;
        private String externalSource;
        private long startTime;
        private long endTime;

        public RefreshExternalCollectionJobInfoBuilder jobId(long jobId) {
            this.jobId = jobId;
            return this;
        }

        public RefreshExternalCollectionJobInfoBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public RefreshExternalCollectionJobInfoBuilder state(String state) {
            this.state = state;
            return this;
        }

        public RefreshExternalCollectionJobInfoBuilder progress(int progress) {
            this.progress = progress;
            return this;
        }

        public RefreshExternalCollectionJobInfoBuilder reason(String reason) {
            this.reason = reason;
            return this;
        }

        public RefreshExternalCollectionJobInfoBuilder externalSource(String externalSource) {
            this.externalSource = externalSource;
            return this;
        }

        public RefreshExternalCollectionJobInfoBuilder startTime(long startTime) {
            this.startTime = startTime;
            return this;
        }

        public RefreshExternalCollectionJobInfoBuilder endTime(long endTime) {
            this.endTime = endTime;
            return this;
        }

        public RefreshExternalCollectionJobInfo build() {
            return new RefreshExternalCollectionJobInfo(this);
        }
    }
}
