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

public class RefreshExternalCollectionResp {
    private final long jobId;

    private RefreshExternalCollectionResp(RefreshExternalCollectionRespBuilder builder) {
        this.jobId = builder.jobId;
    }

    public static RefreshExternalCollectionRespBuilder builder() {
        return new RefreshExternalCollectionRespBuilder();
    }

    public long getJobId() {
        return jobId;
    }

    @Override
    public String toString() {
        return "RefreshExternalCollectionResp{" +
                "jobId=" + jobId +
                '}';
    }

    public static class RefreshExternalCollectionRespBuilder {
        private long jobId;

        public RefreshExternalCollectionRespBuilder jobId(long jobId) {
            this.jobId = jobId;
            return this;
        }

        public RefreshExternalCollectionResp build() {
            return new RefreshExternalCollectionResp(this);
        }
    }
}
