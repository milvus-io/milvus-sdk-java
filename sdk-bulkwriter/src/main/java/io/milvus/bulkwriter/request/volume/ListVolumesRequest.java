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

package io.milvus.bulkwriter.request.volume;


public class ListVolumesRequest {
    private String projectId;
    private Integer pageSize;
    private Integer currentPage;

    public ListVolumesRequest() {
    }

    public ListVolumesRequest(String projectId, Integer pageSize, Integer currentPage) {
        this.projectId = projectId;
        this.pageSize = pageSize;
        this.currentPage = currentPage;
    }

    protected ListVolumesRequest(ListVolumesRequestBuilder builder) {
        this.projectId = builder.projectId;
        this.pageSize = builder.pageSize;
        this.currentPage = builder.currentPage;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
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
        return "ListVolumesRequest{" +
                "projectId='" + projectId + '\'' +
                ", pageSize=" + pageSize +
                ", currentPage=" + currentPage +
                '}';
    }

    public static ListVolumesRequestBuilder builder() {
        return new ListVolumesRequestBuilder();
    }

    public static class ListVolumesRequestBuilder {
        private String projectId;
        private Integer pageSize;
        private Integer currentPage;

        private ListVolumesRequestBuilder() {
            this.projectId = "";
            this.pageSize = 0;
            this.currentPage = 0;
        }

        public ListVolumesRequestBuilder projectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public ListVolumesRequestBuilder pageSize(Integer pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public ListVolumesRequestBuilder currentPage(Integer currentPage) {
            this.currentPage = currentPage;
            return this;
        }

        public ListVolumesRequest build() {
            return new ListVolumesRequest(this);
        }
    }
}
