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

public class ListVolumesResponse {
    private Integer count;
    private Integer currentPage;
    private Integer pageSize;
    private List<VolumeInfo> volumes;

    public ListVolumesResponse() {
    }

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

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Integer getCurrentPage() {
        return currentPage;
    }

    public void setCurrentPage(Integer currentPage) {
        this.currentPage = currentPage;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public List<VolumeInfo> getVolumes() {
        return volumes;
    }

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

    public static ListVolumesResponseBuilder builder() {
        return new ListVolumesResponseBuilder();
    }

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

        public ListVolumesResponseBuilder count(Integer count) {
            this.count = count;
            return this;
        }

        public ListVolumesResponseBuilder currentPage(Integer currentPage) {
            this.currentPage = currentPage;
            return this;
        }

        public ListVolumesResponseBuilder pageSize(Integer pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public ListVolumesResponseBuilder volumes(List<VolumeInfo> volumes) {
            this.volumes = volumes;
            return this;
        }

        public ListVolumesResponse build() {
            return new ListVolumesResponse(this);
        }
    }
}
