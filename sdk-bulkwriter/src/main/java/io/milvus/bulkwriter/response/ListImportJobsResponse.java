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

package io.milvus.bulkwriter.response;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ListImportJobsResponse implements Serializable {
    private static final long serialVersionUID = -8400893490624599225L;
    private Integer count;
    private Integer currentPage;
    private Integer pageSize;
    private List<Record> records;

    public ListImportJobsResponse() {
    }

    public ListImportJobsResponse(Integer count, Integer currentPage, Integer pageSize, List<Record> records) {
        this.count = count;
        this.currentPage = currentPage;
        this.pageSize = pageSize;
        this.records = records;
    }

    private ListImportJobsResponse(ListImportJobsResponseBuilder builder) {
        this.count = builder.count;
        this.currentPage = builder.currentPage;
        this.pageSize = builder.pageSize;
        this.records = builder.records;
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

    public List<Record> getRecords() {
        return records;
    }

    public void setRecords(List<Record> records) {
        this.records = records;
    }

    @Override
    public String toString() {
        return "ListImportJobsResponse{" +
                ", count=" + count +
                ", currentPage=" + currentPage +
                ", pageSize=" + pageSize +
                '}';
    }

    public static ListImportJobsResponseBuilder builder() {
        return new ListImportJobsResponseBuilder();
    }

    public static class ListImportJobsResponseBuilder {
        private Integer count;
        private Integer currentPage;
        private Integer pageSize;
        private List<Record> records;

        private ListImportJobsResponseBuilder() {
            this.count = 0;
            this.currentPage = 0;
            this.pageSize = 0;
            this.records = new ArrayList<>();
        }

        public ListImportJobsResponseBuilder count(Integer count) {
            this.count = count;
            return this;
        }

        public ListImportJobsResponseBuilder currentPage(Integer currentPage) {
            this.currentPage = currentPage;
            return this;
        }

        public ListImportJobsResponseBuilder pageSize(Integer pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public ListImportJobsResponseBuilder records(List<Record> records) {
            this.records = records;
            return this;
        }

        public ListImportJobsResponse build() {
            return new ListImportJobsResponse(this);
        }
    }
}
