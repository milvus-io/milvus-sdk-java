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

/**
 * Information of a refresh-external-collection job, as returned by the
 * {@code listRefreshExternalCollectionJobs} and {@code getRefreshExternalCollectionProgress} APIs.
 */
public class RefreshExternalCollectionJobInfo {
    private final long jobId;
    private final String collectionName;
    private final String state;
    private final int progress;
    private final String reason;
    private final String externalSource;
    private final String externalSpec;
    private final long startTime;
    private final long endTime;

    private RefreshExternalCollectionJobInfo(RefreshExternalCollectionJobInfoBuilder builder) {
        this.jobId = builder.jobId;
        this.collectionName = builder.collectionName;
        this.state = builder.state;
        this.progress = builder.progress;
        this.reason = builder.reason;
        this.externalSource = builder.externalSource;
        this.externalSpec = builder.externalSpec;
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
    }

    /**
     * Creates a new {@code RefreshExternalCollectionJobInfo} builder.
     *
     * @return the builder
     */
    public static RefreshExternalCollectionJobInfoBuilder builder() {
        return new RefreshExternalCollectionJobInfoBuilder();
    }

    /**
     * Returns the ID of the refresh job.
     *
     * @return the job ID
     */
    public long getJobId() {
        return jobId;
    }

    /**
     * Returns the name of the collection being refreshed.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Returns the state of the refresh job.
     *
     * @return the job state
     */
    public String getState() {
        return state;
    }

    /**
     * Returns the progress of the refresh job, in the range {@code [0, 100]}.
     *
     * @return the job progress
     */
    public int getProgress() {
        return progress;
    }

    /**
     * Returns the reason for the current job state, if any.
     *
     * @return the reason
     */
    public String getReason() {
        return reason;
    }

    /**
     * Returns the external data source used for the refresh job.
     *
     * @return the external source
     */
    public String getExternalSource() {
        return externalSource;
    }

    /**
     * Returns the specification of the external data source.
     *
     * @return the external specification
     */
    public String getExternalSpec() {
        return externalSpec;
    }

    /**
     * Returns the start time of the refresh job, in milliseconds.
     *
     * @return the start time in milliseconds
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns the end time of the refresh job, in milliseconds.
     *
     * @return the end time in milliseconds
     */
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
                ", externalSpec='" + externalSpec + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }

    /**
     * Builder for {@link RefreshExternalCollectionJobInfo}.
     */
    public static class RefreshExternalCollectionJobInfoBuilder {
        private long jobId;
        private String collectionName;
        private String state;
        private int progress;
        private String reason;
        private String externalSource;
        private String externalSpec;
        private long startTime;
        private long endTime;

        /**
         * Sets the ID of the refresh job.
         *
         * @param jobId the job ID
         * @return this builder
         */
        public RefreshExternalCollectionJobInfoBuilder jobId(long jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Sets the name of the collection being refreshed.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public RefreshExternalCollectionJobInfoBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the state of the refresh job.
         *
         * @param state the job state
         * @return this builder
         */
        public RefreshExternalCollectionJobInfoBuilder state(String state) {
            this.state = state;
            return this;
        }

        /**
         * Sets the progress of the refresh job.
         *
         * @param progress the job progress
         * @return this builder
         */
        public RefreshExternalCollectionJobInfoBuilder progress(int progress) {
            this.progress = progress;
            return this;
        }

        /**
         * Sets the reason for the current job state.
         *
         * @param reason the reason
         * @return this builder
         */
        public RefreshExternalCollectionJobInfoBuilder reason(String reason) {
            this.reason = reason;
            return this;
        }

        /**
         * Sets the external data source used for the refresh job.
         *
         * @param externalSource the external source
         * @return this builder
         */
        public RefreshExternalCollectionJobInfoBuilder externalSource(String externalSource) {
            this.externalSource = externalSource;
            return this;
        }

        /**
         * Sets the specification of the external data source.
         *
         * @param externalSpec the external specification
         * @return this builder
         */
        public RefreshExternalCollectionJobInfoBuilder externalSpec(String externalSpec) {
            this.externalSpec = externalSpec;
            return this;
        }

        /**
         * Sets the start time of the refresh job, in milliseconds.
         *
         * @param startTime the start time in milliseconds
         * @return this builder
         */
        public RefreshExternalCollectionJobInfoBuilder startTime(long startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Sets the end time of the refresh job, in milliseconds.
         *
         * @param endTime the end time in milliseconds
         * @return this builder
         */
        public RefreshExternalCollectionJobInfoBuilder endTime(long endTime) {
            this.endTime = endTime;
            return this;
        }

        /**
         * Builds the {@link RefreshExternalCollectionJobInfo}.
         *
         * @return the job information
         */
        public RefreshExternalCollectionJobInfo build() {
            return new RefreshExternalCollectionJobInfo(this);
        }
    }
}
