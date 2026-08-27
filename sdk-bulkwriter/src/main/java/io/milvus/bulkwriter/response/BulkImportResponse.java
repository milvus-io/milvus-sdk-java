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

/**
 * A response representing the result of submitting a bulk import job.
 *
 * <p>It carries the unique ID of the created import job, which can later be used to
 * describe, commit, or abort the import.</p>
 */
public class BulkImportResponse implements Serializable {
    private static final long serialVersionUID = -7162743560382861611L;
    private String jobId;

    /**
     * Constructs an empty {@code BulkImportResponse}.
     */
    public BulkImportResponse() {
    }

    /**
     * Constructs a {@code BulkImportResponse} with the given job ID.
     *
     * @param jobId the ID of the created bulk import job
     */
    public BulkImportResponse(String jobId) {
        this.jobId = jobId;
    }

    private BulkImportResponse(BulkImportResponseBuilder builder) {
        this.jobId = builder.jobId;
    }

    /**
     * Returns the ID of the created bulk import job.
     *
     * @return the import job ID
     */
    public String getJobId() {
        return jobId;
    }

    /**
     * Sets the ID of the created bulk import job.
     *
     * @param jobId the import job ID
     */
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    @Override
    public String toString() {
        return "BulkImportResponse{" +
                "jobId='" + jobId + '\'' +
                '}';
    }

    /**
     * Returns a new builder for a {@link BulkImportResponse}.
     *
     * @return a {@code BulkImportResponse} builder
     */
    public static BulkImportResponseBuilder builder() {
        return new BulkImportResponseBuilder();
    }

    /**
     * Builder for {@link BulkImportResponse}.
     */
    public static class BulkImportResponseBuilder {
        private String jobId;

        private BulkImportResponseBuilder() {
            this.jobId = "";
        }

        /**
         * Sets the ID of the created bulk import job.
         *
         * @param jobId the import job ID
         * @return this builder
         */
        public BulkImportResponseBuilder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Builds the {@link BulkImportResponse} instance.
         *
         * @return the built {@code BulkImportResponse}
         */
        public BulkImportResponse build() {
            return new BulkImportResponse(this);
        }
    }
}
