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


/**
 * Request for paginated querying of the volume list under a specified project.
 *
 * <p>Supports an optional volume type filter with available values MANAGED or EXTERNAL.</p>
 */



public class ListVolumesRequest {
    private String projectId;
    private Integer pageSize;
    private Integer currentPage;
    // Volume type filter, available values: MANAGED or EXTERNAL.
    private String type;
    /**
     * Creates a new ListVolumesRequest.
     */


    public ListVolumesRequest() {
    }
    /**
     * Creates a new ListVolumesRequest.
     *
     * @param projectId the projectId
     * @param pageSize the pageSize
     * @param currentPage the currentPage
     */


    public ListVolumesRequest(String projectId, Integer pageSize, Integer currentPage) {
        this.projectId = projectId;
        this.pageSize = pageSize;
        this.currentPage = currentPage;
    }

    protected ListVolumesRequest(ListVolumesRequestBuilder builder) {
        this.projectId = builder.projectId;
        this.pageSize = builder.pageSize;
        this.currentPage = builder.currentPage;
        this.type = builder.type;
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
    /**
     * Returns the type.
     *
     * @return the type
     */


    public String getType() {
        return type;
    }
    /**
     * Sets the type.
     *
     * @param type the type
     */


    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "ListVolumesRequest{" +
                "projectId='" + projectId + '\'' +
                ", pageSize=" + pageSize +
                ", currentPage=" + currentPage +
                ", type='" + type + '\'' +
                '}';
    }
    /**
     * Creates a new builder.
     *
     * @return the builder
     */


    public static ListVolumesRequestBuilder builder() {
        return new ListVolumesRequestBuilder();
    }

    /**
     * Builder for {@link ListVolumesRequest} class.
     */


    public static class ListVolumesRequestBuilder {
        private String projectId;
        private Integer pageSize;
        private Integer currentPage;
        private String type;

        private ListVolumesRequestBuilder() {
            this.projectId = "";
            this.pageSize = 0;
            this.currentPage = 0;
        }
        /**
         * Sets the projectId.
         *
         * @param projectId the projectId
         * @return this builder
         */


        public ListVolumesRequestBuilder projectId(String projectId) {
            this.projectId = projectId;
            return this;
        }
        /**
         * Sets the pageSize.
         *
         * @param pageSize the pageSize
         * @return this builder
         */


        public ListVolumesRequestBuilder pageSize(Integer pageSize) {
            this.pageSize = pageSize;
            return this;
        }
        /**
         * Sets the currentPage.
         *
         * @param currentPage the currentPage
         * @return this builder
         */


        public ListVolumesRequestBuilder currentPage(Integer currentPage) {
            this.currentPage = currentPage;
            return this;
        }

        /**
         * Set volume type filter.
         * Available values: MANAGED or EXTERNAL.
         *
         * @param type the volume type filter; MANAGED or EXTERNAL
         * @return this builder
         */


        public ListVolumesRequestBuilder type(String type) {
            this.type = type;
            return this;
        }
        /**
         * Builds the ListVolumesRequest.
         *
         * @return the built ListVolumesRequest
         */


        public ListVolumesRequest build() {
            return new ListVolumesRequest(this);
        }
    }
}
