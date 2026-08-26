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
 * Response returned by the {@code restoreSnapshot} API.
 */
public class RestoreSnapshotResp {
    private Long jobId;

    private RestoreSnapshotResp(RestoreSnapshotRespBuilder builder) {
        this.jobId = builder.jobId;
    }

    public static RestoreSnapshotRespBuilder builder() {
        return new RestoreSnapshotRespBuilder();
    }

    /**
     * Returns the ID of the restore snapshot job.
     *
     * @return the restore snapshot job ID
     */
    public Long getJobId() {
        return jobId;
    }

    /**
     * Sets the ID of the restore snapshot job.
     *
     * @param jobId the restore snapshot job ID
     */
    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    @Override
    public String toString() {
        return "RestoreSnapshotResp{" +
                "jobId=" + jobId +
                '}';
    }

    public static class RestoreSnapshotRespBuilder {
        private Long jobId;

        /**
         * Sets the ID of the restore snapshot job.
         *
         * @param jobId the restore snapshot job ID
         * @return this builder
         */
        public RestoreSnapshotRespBuilder jobId(Long jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Builds a {@link RestoreSnapshotResp}.
         *
         * @return the built response
         */
        public RestoreSnapshotResp build() {
            return new RestoreSnapshotResp(this);
        }
    }
}
