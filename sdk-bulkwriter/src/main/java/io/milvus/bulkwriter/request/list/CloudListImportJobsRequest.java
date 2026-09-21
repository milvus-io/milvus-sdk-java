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

package io.milvus.bulkwriter.request.list;

/**
 * Zilliz Cloud only. Request for listing import jobs on a Zilliz cloud instance.
 *
 * <p>The jobs are scoped by clusterId (or projectId and regionId for project database
 * deployments) and support pagination via pageSize and currentPage.</p>
 *
 * @zillizCloudOnly
 */


public class CloudListImportJobsRequest extends BaseListImportJobsRequest {
    private static final long serialVersionUID = -3380786382584854649L;
    private String clusterId;

    /**
     * For project database deployments: use projectId and regionId instead of clusterId.
     */
    private String projectId;

    /**
     * For project database deployments: use projectId and regionId instead of clusterId.
     */
    private String regionId;
    private Integer pageSize;
    private Integer currentPage;

    protected CloudListImportJobsRequest() {
    }

    protected CloudListImportJobsRequest(String clusterId, Integer pageSize, Integer currentPage) {
        this.clusterId = clusterId;
        this.pageSize = pageSize;
        this.currentPage = currentPage;
    }

    protected CloudListImportJobsRequest(CloudListImportJobsRequestBuilder builder) {
        super(builder);
        this.clusterId = builder.clusterId;
        this.projectId = builder.projectId;
        this.regionId = builder.regionId;
        this.pageSize = builder.pageSize;
        this.currentPage = builder.currentPage;
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
     * Returns the pageSize.
     *
     * @return the pageSize
     */


    public Integer getPageSize() {
        return pageSize;
    }
    /**
     * Sets the pageSize.
     *
     * @param pageSize the pageSize
     */


    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }
    /**
     * Returns the currentPage.
     *
     * @return the currentPage
     */


    public Integer getCurrentPage() {
        return currentPage;
    }
    /**
     * Sets the currentPage.
     *
     * @param currentPage the currentPage
     */


    public void setCurrentPage(Integer currentPage) {
        this.currentPage = currentPage;
    }

    @Override
    public String toString() {
        return "CloudListImportJobsRequest{" +
                "clusterId='" + clusterId + '\'' +
                ", projectId='" + projectId + '\'' +
                ", regionId='" + regionId + '\'' +
                ", pageSize=" + pageSize +
                ", currentPage=" + currentPage +
                '}';
    }
    /**
     * Creates a new builder.
     *
     * @return the builder
     */


    public static CloudListImportJobsRequestBuilder builder() {
        return new CloudListImportJobsRequestBuilder();
    }

    /**
     * Builder for {@link CloudListImportJobsRequest} class.
     */


    public static class CloudListImportJobsRequestBuilder extends BaseListImportJobsRequestBuilder<CloudListImportJobsRequestBuilder> {
        private String clusterId;
        private String projectId;
        private String regionId;
        private Integer pageSize;
        private Integer currentPage;

        private CloudListImportJobsRequestBuilder() {
            this.clusterId = "";
            this.projectId = "";
            this.regionId = "";
            this.pageSize = 0;
            this.currentPage = 0;
        }
        /**
         * Sets the clusterId.
         *
         * @param clusterId the clusterId
         * @return this builder
         */


        public CloudListImportJobsRequestBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }
        /**
         * Sets the projectId.
         *
         * @param projectId the projectId
         * @return this builder
         */


        public CloudListImportJobsRequestBuilder projectId(String projectId) {
            this.projectId = projectId;
            return this;
        }
        /**
         * Sets the regionId.
         *
         * @param regionId the regionId
         * @return this builder
         */


        public CloudListImportJobsRequestBuilder regionId(String regionId) {
            this.regionId = regionId;
            return this;
        }
        /**
         * Sets the pageSize.
         *
         * @param pageSize the pageSize
         * @return this builder
         */


        public CloudListImportJobsRequestBuilder pageSize(Integer pageSize) {
            this.pageSize = pageSize;
            return this;
        }
        /**
         * Sets the currentPage.
         *
         * @param currentPage the currentPage
         * @return this builder
         */


        public CloudListImportJobsRequestBuilder currentPage(Integer currentPage) {
            this.currentPage = currentPage;
            return this;
        }
        /**
         * Builds the CloudListImportJobsRequest.
         *
         * @return the built CloudListImportJobsRequest
         */


        public CloudListImportJobsRequest build() {
            return new CloudListImportJobsRequest(this);
        }
    }
}
