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

public class GetRestoreSnapshotStateReq {
    private Long jobId;

    private GetRestoreSnapshotStateReq(GetRestoreSnapshotStateReqBuilder builder) {
        this.jobId = builder.jobId;
    }

    public static GetRestoreSnapshotStateReqBuilder builder() {
        return new GetRestoreSnapshotStateReqBuilder();
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    @Override
    public String toString() {
        return "GetRestoreSnapshotStateReq{" +
                "jobId=" + jobId +
                '}';
    }

    public static class GetRestoreSnapshotStateReqBuilder {
        private Long jobId;

        public GetRestoreSnapshotStateReqBuilder jobId(Long jobId) {
            this.jobId = jobId;
            return this;
        }

        public GetRestoreSnapshotStateReq build() {
            return new GetRestoreSnapshotStateReq(this);
        }
    }
}
