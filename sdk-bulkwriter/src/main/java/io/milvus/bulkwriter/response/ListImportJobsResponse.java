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

/**
 * A response containing a paginated list of bulk import jobs.
 *
 * <p>It carries the total count of matching import jobs, the pagination information,
 * and the list of job records on the current page.</p>
 */
public class ListImportJobsResponse implements Serializable {
    private static final long serialVersionUID = -8400893490624599225L;
    private Integer count;
    private Integer currentPage;
    private Integer pageSize;
    private List<Record> records;

    /**
     * Constructs an empty {@code ListImportJobsResponse}.
     */
    public ListImportJobsResponse() {
    }

    /**
     * Constructs a {@code ListImportJobsResponse} with the given pagination information and records.
     *
     * @param count       the total number of matching import jobs
     * @param currentPage the current page number
     * @param pageSize    the number of records per page
     * @param records     the list of import job records on the current page
     */
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

    /**
     * Returns the total number of matching import jobs.
     *
     * @return the total number of matching import jobs
     */
    public Integer getCount() {
        return count;
    }

    /**
     * Sets the total number of matching import jobs.
     *
     * @param count the total number of matching import jobs
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
     * Returns the number of records per page.
     *
     * @return the page size
     */
    public Integer getPageSize() {
        return pageSize;
    }

    /**
     * Sets the number of records per page.
     *
     * @param pageSize the page size
     */
    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * Returns the list of import job records on the current page.
     *
     * @return the list of import job records
     */
    public List<Record> getRecords() {
        return records;
    }

    /**
     * Sets the list of import job records on the current page.
     *
     * @param records the list of import job records
     */
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

    /**
     * Returns a new builder for a {@link ListImportJobsResponse}.
     *
     * @return a {@code ListImportJobsResponse} builder
     */
    public static ListImportJobsResponseBuilder builder() {
        return new ListImportJobsResponseBuilder();
    }

    /**
     * Builder for {@link ListImportJobsResponse}.
     */
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

        /**
         * Sets the total number of matching import jobs.
         *
         * @param count the total number of matching import jobs
         * @return this builder
         */
        public ListImportJobsResponseBuilder count(Integer count) {
            this.count = count;
            return this;
        }

        /**
         * Sets the current page number.
         *
         * @param currentPage the current page number
         * @return this builder
         */
        public ListImportJobsResponseBuilder currentPage(Integer currentPage) {
            this.currentPage = currentPage;
            return this;
        }

        /**
         * Sets the number of records per page.
         *
         * @param pageSize the page size
         * @return this builder
         */
        public ListImportJobsResponseBuilder pageSize(Integer pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        /**
         * Sets the list of import job records on the current page.
         *
         * @param records the list of import job records
         * @return this builder
         */
        public ListImportJobsResponseBuilder records(List<Record> records) {
            this.records = records;
            return this;
        }

        /**
         * Builds the {@link ListImportJobsResponse} instance.
         *
         * @return the built {@code ListImportJobsResponse}
         */
        public ListImportJobsResponse build() {
            return new ListImportJobsResponse(this);
        }
    }
}
