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

package io.milvus.bulkwriter.response.stage;

import java.util.ArrayList;
import java.util.List;

public class ListStagesResponse {
    private Integer count;
    private Integer currentPage;
    private Integer pageSize;
    private List<StageInfo> stages;

    public ListStagesResponse() {
    }

    public ListStagesResponse(Integer count, Integer currentPage, Integer pageSize, List<StageInfo> stages) {
        this.count = count;
        this.currentPage = currentPage;
        this.pageSize = pageSize;
        this.stages = stages;
    }

    private ListStagesResponse(ListStagesResponseBuilder builder) {
        this.count = builder.count;
        this.currentPage = builder.currentPage;
        this.pageSize = builder.pageSize;
        this.stages = builder.stages;
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

    public List<StageInfo> getStages() {
        return stages;
    }

    public void setStages(List<StageInfo> stages) {
        this.stages = stages;
    }

    @Override
    public String toString() {
        return "ListStagesResponse{" +
                ", count=" + count +
                ", currentPage=" + currentPage +
                ", pageSize=" + pageSize +
                '}';
    }

    public static ListStagesResponseBuilder builder() {
        return new ListStagesResponseBuilder();
    }

    public static class ListStagesResponseBuilder {
        private Integer count;
        private Integer currentPage;
        private Integer pageSize;
        private List<StageInfo> stages;

        private ListStagesResponseBuilder() {
            this.count = 0;
            this.currentPage = 0;
            this.pageSize = 0;
            this.stages = new ArrayList<>();
        }

        public ListStagesResponseBuilder count(Integer count) {
            this.count = count;
            return this;
        }

        public ListStagesResponseBuilder currentPage(Integer currentPage) {
            this.currentPage = currentPage;
            return this;
        }

        public ListStagesResponseBuilder pageSize(Integer pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public ListStagesResponseBuilder stages(List<StageInfo> stages) {
            this.stages = stages;
            return this;
        }

        public ListStagesResponse build() {
            return new ListStagesResponse(this);
        }
    }
}
