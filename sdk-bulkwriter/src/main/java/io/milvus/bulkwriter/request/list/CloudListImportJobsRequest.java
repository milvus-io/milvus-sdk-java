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

public class CloudListImportJobsRequest extends BaseListImportJobsRequest {
    private static final long serialVersionUID = -3380786382584854649L;
    private String clusterId;
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
        this.pageSize = builder.pageSize;
        this.currentPage = builder.currentPage;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public Integer getCurrentPage() {
        return currentPage;
    }

    public void setCurrentPage(Integer currentPage) {
        this.currentPage = currentPage;
    }

    @Override
    public String toString() {
        return "CloudListImportJobsRequest{" +
                "clusterId='" + clusterId + '\'' +
                "pageSize=" + pageSize +
                "currentPage=" + currentPage +
                '}';
    }

    public static CloudListImportJobsRequestBuilder builder() {
        return new CloudListImportJobsRequestBuilder();
    }

    public static class CloudListImportJobsRequestBuilder extends BaseListImportJobsRequestBuilder<CloudListImportJobsRequestBuilder> {
        private String clusterId;
        private Integer pageSize;
        private Integer currentPage;

        private CloudListImportJobsRequestBuilder() {
            this.clusterId = "";
            this.pageSize = 0;
            this.currentPage = 0;
        }

        public CloudListImportJobsRequestBuilder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public CloudListImportJobsRequestBuilder pageSize(Integer pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public CloudListImportJobsRequestBuilder currentPage(Integer currentPage) {
            this.currentPage = currentPage;
            return this;
        }

        public CloudListImportJobsRequest build() {
            return new CloudListImportJobsRequest(this);
        }
    }
}
