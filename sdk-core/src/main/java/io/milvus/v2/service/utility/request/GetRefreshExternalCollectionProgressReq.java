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

package io.milvus.v2.service.utility.request;

/**
 * Request parameters for the {@code getRefreshExternalCollectionProgress} API.
 */
public class GetRefreshExternalCollectionProgressReq {
    private final long jobId;

    private GetRefreshExternalCollectionProgressReq(GetRefreshExternalCollectionProgressReqBuilder builder) {
        this.jobId = builder.jobId;
    }

    public static GetRefreshExternalCollectionProgressReqBuilder builder() {
        return new GetRefreshExternalCollectionProgressReqBuilder();
    }

    /**
     * Returns the ID of the refresh external collection job.
     *
     * @return the refresh job ID
     */
    public long getJobId() {
        return jobId;
    }

    @Override
    public String toString() {
        return "GetRefreshExternalCollectionProgressReq{" +
                "jobId=" + jobId +
                '}';
    }

    public static class GetRefreshExternalCollectionProgressReqBuilder {
        private long jobId;

        /**
         * Sets the ID of the refresh external collection job.
         *
         * @param jobId the refresh job ID
         * @return this builder
         */
        public GetRefreshExternalCollectionProgressReqBuilder jobId(long jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Builds the {@code GetRefreshExternalCollectionProgressReq}.
         *
         * @return the constructed {@code GetRefreshExternalCollectionProgressReq}
         */
        public GetRefreshExternalCollectionProgressReq build() {
            return new GetRefreshExternalCollectionProgressReq(this);
        }
    }
}
