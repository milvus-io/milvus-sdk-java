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

package io.milvus.bulkwriter.request.describe;

public class CloudDescribeImportRequest extends BaseDescribeImportRequest {
    private static final long serialVersionUID = -6479634844757426430L;
    private String clusterId;
    private String jobId;

    public CloudDescribeImportRequest() {
    }

    public CloudDescribeImportRequest(String clusterId, String jobId) {
        this.clusterId = clusterId;
        this.jobId = jobId;
    }

    protected CloudDescribeImportRequest(CloudDescribeImportRequestBuilder builder) {
        super(builder);
        this.clusterId = builder.clusterId;
        this.jobId = builder.jobId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    @Override
    public String toString() {
        return "CloudDescribeImportRequest{" +
                "clusterId='" + clusterId + '\'' +
                ", jobId='" + jobId + '\'' +
                '}';
    }

    public static CloudDescribeImportRequestBuilder builder() {
        return new CloudDescribeImportRequestBuilder();
    }

    public static class CloudDescribeImportRequestBuilder extends BaseDescribeImportRequestBuilder<CloudDescribeImportRequestBuilder> {
        private String clusterId;
        private String jobId;

        private CloudDescribeImportRequestBuilder() {
            this.clusterId = "";
            this.jobId = "";
        }

        public CloudDescribeImportRequestBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public CloudDescribeImportRequestBuilder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public CloudDescribeImportRequest build() {
            return new CloudDescribeImportRequest(this);
        }
    }
}
