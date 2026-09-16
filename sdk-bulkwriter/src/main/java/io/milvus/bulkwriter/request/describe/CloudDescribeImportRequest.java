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

/**
 * Request for describing an import job on a Zilliz cloud instance.
 *
 * <p>The job is identified by clusterId (or projectId and regionId for project database
 * deployments) together with the jobId.</p>
 */


public class CloudDescribeImportRequest extends BaseDescribeImportRequest {
    private static final long serialVersionUID = -6479634844757426430L;
    private String clusterId;

    /**
     * For project database deployments: use projectId and regionId instead of clusterId.
     */
    private String projectId;

    /**
     * For project database deployments: use projectId and regionId instead of clusterId.
     */
    private String regionId;
    private String jobId;
    /**
     * Creates a new CloudDescribeImportRequest.
     */


    public CloudDescribeImportRequest() {
    }
    /**
     * Creates a new CloudDescribeImportRequest.
     *
     * @param clusterId the clusterId
     * @param jobId the jobId
     */


    public CloudDescribeImportRequest(String clusterId, String jobId) {
        this.clusterId = clusterId;
        this.jobId = jobId;
    }
    /**
     * Creates a new CloudDescribeImportRequest.
     *
     * @param clusterId the clusterId
     * @param projectId the projectId
     * @param regionId the regionId
     * @param jobId the jobId
     */


    public CloudDescribeImportRequest(String clusterId, String projectId, String regionId, String jobId) {
        this.clusterId = clusterId;
        this.projectId = projectId;
        this.regionId = regionId;
        this.jobId = jobId;
    }

    protected CloudDescribeImportRequest(CloudDescribeImportRequestBuilder builder) {
        super(builder);
        this.clusterId = builder.clusterId;
        this.projectId = builder.projectId;
        this.regionId = builder.regionId;
        this.jobId = builder.jobId;
    }
    /**
     * Returns the clusterId.
     *
     * @return the clusterId
     */


    public String getClusterId() {
        return clusterId;
    }
    /**
     * Sets the clusterId.
     *
     * @param clusterId the clusterId
     */


    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }
    /**
     * Returns the projectId.
     *
     * @return the projectId
     */


    public String getProjectId() {
        return projectId;
    }
    /**
     * Sets the projectId.
     *
     * @param projectId the projectId
     */


    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }
    /**
     * Returns the regionId.
     *
     * @return the regionId
     */


    public String getRegionId() {
        return regionId;
    }
    /**
     * Sets the regionId.
     *
     * @param regionId the regionId
     */


    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }
    /**
     * Returns the jobId.
     *
     * @return the jobId
     */


    public String getJobId() {
        return jobId;
    }
    /**
     * Sets the jobId.
     *
     * @param jobId the jobId
     */


    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    @Override
    public String toString() {
        return "CloudDescribeImportRequest{" +
                "clusterId='" + clusterId + '\'' +
                ", projectId='" + projectId + '\'' +
                ", regionId='" + regionId + '\'' +
                ", jobId='" + jobId + '\'' +
                '}';
    }
    /**
     * Creates a new builder.
     *
     * @return the builder
     */


    public static CloudDescribeImportRequestBuilder builder() {
        return new CloudDescribeImportRequestBuilder();
    }

    /**
     * Builder for {@link CloudDescribeImportRequest} class.
     */


    public static class CloudDescribeImportRequestBuilder extends BaseDescribeImportRequestBuilder<CloudDescribeImportRequestBuilder> {
        private String clusterId;
        private String projectId;
        private String regionId;
        private String jobId;

        private CloudDescribeImportRequestBuilder() {
            this.clusterId = "";
            this.projectId = "";
            this.regionId = "";
            this.jobId = "";
        }
        /**
         * Sets the clusterId.
         *
         * @param clusterId the clusterId
         * @return this builder
         */


        public CloudDescribeImportRequestBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }
        /**
         * Sets the projectId.
         *
         * @param projectId the projectId
         * @return this builder
         */


        public CloudDescribeImportRequestBuilder projectId(String projectId) {
            this.projectId = projectId;
            return this;
        }
        /**
         * Sets the regionId.
         *
         * @param regionId the regionId
         * @return this builder
         */


        public CloudDescribeImportRequestBuilder regionId(String regionId) {
            this.regionId = regionId;
            return this;
        }
        /**
         * Sets the jobId.
         *
         * @param jobId the jobId
         * @return this builder
         */


        public CloudDescribeImportRequestBuilder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }
        /**
         * Builds the CloudDescribeImportRequest.
         *
         * @return the built CloudDescribeImportRequest
         */


        public CloudDescribeImportRequest build() {
            return new CloudDescribeImportRequest(this);
        }
    }
}
