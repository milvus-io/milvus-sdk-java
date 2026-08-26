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

package io.milvus.bulkwriter.request.describe;

/**
 * Request parameters for describing a bulk import job, i.e. getting the import progress.
 *
 * <p>It identifies the import job to describe through its job ID, and inherits the API key
 * handling from {@link BaseDescribeImportRequest}.</p>
 */
public class MilvusDescribeImportRequest extends BaseDescribeImportRequest {
    private static final long serialVersionUID = 6123645882882199210L;
    private String jobId;

    /**
     * Constructs an empty {@code MilvusDescribeImportRequest}.
     */
    public MilvusDescribeImportRequest() {
    }

    /**
     * Constructs a {@code MilvusDescribeImportRequest} with the given job ID.
     *
     * @param jobId the ID of the import job to describe
     */
    public MilvusDescribeImportRequest(String jobId) {
        this.jobId = jobId;
    }

    protected MilvusDescribeImportRequest(MilvusDescribeImportRequestBuilder builder) {
        super(builder);
        this.jobId = builder.jobId;
    }

    /**
     * Returns the ID of the import job to describe.
     *
     * @return the import job ID
     */
    public String getJobId() {
        return jobId;
    }

    /**
     * Sets the ID of the import job to describe.
     *
     * @param jobId the import job ID
     */
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    @Override
    public String toString() {
        return "MilvusDescribeImportRequest{" +
                ", jobId='" + jobId + '\'' +
                '}';
    }

    /**
     * Returns a new builder for a {@link MilvusDescribeImportRequest}.
     *
     * @return a {@code MilvusDescribeImportRequest} builder
     */
    public static MilvusDescribeImportRequestBuilder builder() {
        return new MilvusDescribeImportRequestBuilder();
    }

    /**
     * Builder for {@link MilvusDescribeImportRequest}.
     */
    public static class MilvusDescribeImportRequestBuilder extends BaseDescribeImportRequestBuilder<MilvusDescribeImportRequestBuilder> {
        private String jobId;

        private MilvusDescribeImportRequestBuilder() {
            this.jobId = "";
        }

        /**
         * Sets the ID of the import job to describe.
         *
         * @param jobId the import job ID
         * @return this builder
         */
        public MilvusDescribeImportRequestBuilder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Builds the {@link MilvusDescribeImportRequest} instance.
         *
         * @return the built {@code MilvusDescribeImportRequest}
         */
        public MilvusDescribeImportRequest build() {
            return new MilvusDescribeImportRequest(this);
        }
    }
}
