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

package io.milvus.bulkwriter.response.volume;

import java.util.ArrayList;
import java.util.List;

/**
 * A response containing a paginated list of cloud storage volumes.
 *
 * <p>It carries the total count of volumes, the pagination information, and the list of
 * {@link VolumeInfo} entries on the current page.</p>
 */
public class ListVolumesResponse {
    private Integer count;
    private Integer currentPage;
    private Integer pageSize;
    private List<VolumeInfo> volumes;

    /**
     * Constructs an empty {@code ListVolumesResponse}.
     */
    public ListVolumesResponse() {
    }

    /**
     * Constructs a {@code ListVolumesResponse} with the given pagination information and volumes.
     *
     * @param count       the total number of volumes
     * @param currentPage the current page number
     * @param pageSize    the number of volumes per page
     * @param volumes     the list of volumes on the current page
     */
    public ListVolumesResponse(Integer count, Integer currentPage, Integer pageSize, List<VolumeInfo> volumes) {
        this.count = count;
        this.currentPage = currentPage;
        this.pageSize = pageSize;
        this.volumes = volumes;
    }

    private ListVolumesResponse(ListVolumesResponseBuilder builder) {
        this.count = builder.count;
        this.currentPage = builder.currentPage;
        this.pageSize = builder.pageSize;
        this.volumes = builder.volumes;
    }

    /**
     * Returns the total number of volumes.
     *
     * @return the total number of volumes
     */
    public Integer getCount() {
        return count;
    }

    /**
     * Sets the total number of volumes.
     *
     * @param count the total number of volumes
     */
    public void setCount(Integer count) {
        this.count = count;
    }

    /**
     * Returns the current page number.
     *
     * @return the current page number
     */
    public Integer getCurrentPage() {
        return currentPage;
    }

    /**
     * Sets the current page number.
     *
     * @param currentPage the current page number
     */
    public void setCurrentPage(Integer currentPage) {
        this.currentPage = currentPage;
    }

    /**
     * Returns the number of volumes per page.
     *
     * @return the page size
     */
    public Integer getPageSize() {
        return pageSize;
    }

    /**
     * Sets the number of volumes per page.
     *
     * @param pageSize the page size
     */
    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * Returns the list of volumes on the current page.
     *
     * @return the list of volumes
     */
    public List<VolumeInfo> getVolumes() {
        return volumes;
    }

    /**
     * Sets the list of volumes on the current page.
     *
     * @param volumes the list of volumes
     */
    public void setVolumes(List<VolumeInfo> volumes) {
        this.volumes = volumes;
    }

    @Override
    public String toString() {
        return "ListVolumesResponse{" +
                ", count=" + count +
                ", currentPage=" + currentPage +
                ", pageSize=" + pageSize +
                '}';
    }

    /**
     * Returns a new builder for a {@link ListVolumesResponse}.
     *
     * @return a {@code ListVolumesResponse} builder
     */
    public static ListVolumesResponseBuilder builder() {
        return new ListVolumesResponseBuilder();
    }

    /**
     * Builder for {@link ListVolumesResponse}.
     */
    public static class ListVolumesResponseBuilder {
        private Integer count;
        private Integer currentPage;
        private Integer pageSize;
        private List<VolumeInfo> volumes;

        private ListVolumesResponseBuilder() {
            this.count = 0;
            this.currentPage = 0;
            this.pageSize = 0;
            this.volumes = new ArrayList<>();
        }

        /**
         * Sets the total number of volumes.
         *
         * @param count the total number of volumes
         * @return this builder
         */
        public ListVolumesResponseBuilder count(Integer count) {
            this.count = count;
            return this;
        }

        /**
         * Sets the current page number.
         *
         * @param currentPage the current page number
         * @return this builder
         */
        public ListVolumesResponseBuilder currentPage(Integer currentPage) {
            this.currentPage = currentPage;
            return this;
        }

        /**
         * Sets the number of volumes per page.
         *
         * @param pageSize the page size
         * @return this builder
         */
        public ListVolumesResponseBuilder pageSize(Integer pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        /**
         * Sets the list of volumes on the current page.
         *
         * @param volumes the list of volumes
         * @return this builder
         */
        public ListVolumesResponseBuilder volumes(List<VolumeInfo> volumes) {
            this.volumes = volumes;
            return this;
        }

        /**
         * Builds the {@link ListVolumesResponse} instance.
         *
         * @return the built {@code ListVolumesResponse}
         */
        public ListVolumesResponse build() {
            return new ListVolumesResponse(this);
        }
    }
}
