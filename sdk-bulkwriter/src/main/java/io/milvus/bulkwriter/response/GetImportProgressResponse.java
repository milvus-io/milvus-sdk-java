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
 * A response containing the progress of a bulk import job.
 *
 * <p>It describes the import job through its job ID, target collection, imported file,
 * state, progress percentage, completion time, reason for failure, total imported rows,
 * and per-file {@link Detail} entries.</p>
 */
public class GetImportProgressResponse implements Serializable {
    private static final long serialVersionUID = -2302203037749197132L;
    private String jobId;
    private String collectionName;
    private String fileName;
    private Integer fileSize;
    private String state;
    private Integer progress;
    private String completeTime;
    private String reason;
    private Integer totalRows;
    private List<Detail> details;

    /**
     * Constructs an empty {@code GetImportProgressResponse}.
     */
    public GetImportProgressResponse() {
    }

    /**
     * Constructs a {@code GetImportProgressResponse} with the given import job information.
     *
     * @param jobId          the unique ID of the import job
     * @param collectionName the name of the collection targeted by the import job
     * @param fileName       the name of the file being imported
     * @param fileSize       the size of the file being imported in bytes
     * @param state          the current state of the import job
     * @param progress       the overall progress percentage of the import job
     * @param completeTime   the time when the import job completed
     * @param reason         the reason for failure, if the import job failed
     * @param totalRows      the total number of rows imported
     * @param details        the per-file import details
     */
    public GetImportProgressResponse(String jobId, String collectionName, String fileName, Integer fileSize,
                                     String state, Integer progress, String completeTime, String reason,
                                     Integer totalRows, List<Detail> details) {
        this.jobId = jobId;
        this.collectionName = collectionName;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.state = state;
        this.progress = progress;
        this.completeTime = completeTime;
        this.reason = reason;
        this.totalRows = totalRows;
        this.details = details;
    }

    private GetImportProgressResponse(GetImportProgressResponseBuilder builder) {
        this.jobId = builder.jobId;
        this.collectionName = builder.collectionName;
        this.fileName = builder.fileName;
        this.fileSize = builder.fileSize;
        this.state = builder.state;
        this.progress = builder.progress;
        this.completeTime = builder.completeTime;
        this.reason = builder.reason;
        this.totalRows = builder.totalRows;
        this.details = builder.details;
    }

    /**
     * Returns the unique ID of the import job.
     *
     * @return the import job ID
     */
    public String getJobId() {
        return jobId;
    }

    /**
     * Sets the unique ID of the import job.
     *
     * @param jobId the import job ID
     */
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    /**
     * Returns the name of the collection targeted by the import job.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the name of the collection targeted by the import job.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the name of the file being imported.
     *
     * @return the file name
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * Sets the name of the file being imported.
     *
     * @param fileName the file name
     */
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    /**
     * Returns the size of the file being imported in bytes.
     *
     * @return the file size in bytes
     */
    public Integer getFileSize() {
        return fileSize;
    }

    /**
     * Sets the size of the file being imported in bytes.
     *
     * @param fileSize the file size in bytes
     */
    public void setFileSize(Integer fileSize) {
        this.fileSize = fileSize;
    }

    /**
     * Returns the current state of the import job.
     *
     * @return the import job state
     */
    public String getState() {
        return state;
    }

    /**
     * Sets the current state of the import job.
     *
     * @param state the import job state
     */
    public void setState(String state) {
        this.state = state;
    }

    /**
     * Returns the overall progress percentage of the import job.
     *
     * @return the progress percentage
     */
    public Integer getProgress() {
        return progress;
    }

    /**
     * Sets the overall progress percentage of the import job.
     *
     * @param progress the progress percentage
     */
    public void setProgress(Integer progress) {
        this.progress = progress;
    }

    /**
     * Returns the time when the import job completed.
     *
     * @return the completion time
     */
    public String getCompleteTime() {
        return completeTime;
    }

    /**
     * Sets the time when the import job completed.
     *
     * @param completeTime the completion time
     */
    public void setCompleteTime(String completeTime) {
        this.completeTime = completeTime;
    }

    /**
     * Returns the reason for failure, if the import job failed.
     *
     * @return the failure reason
     */
    public String getReason() {
        return reason;
    }

    /**
     * Sets the reason for failure, if the import job failed.
     *
     * @param reason the failure reason
     */
    public void setReason(String reason) {
        this.reason = reason;
    }

    /**
     * Returns the total number of rows imported.
     *
     * @return the total number of imported rows
     */
    public Integer getTotalRows() {
        return totalRows;
    }

    /**
     * Sets the total number of rows imported.
     *
     * @param totalRows the total number of imported rows
     */
    public void setTotalRows(Integer totalRows) {
        this.totalRows = totalRows;
    }

    /**
     * Returns the per-file import details.
     *
     * @return the list of per-file import details
     */
    public List<Detail> getDetails() {
        return details;
    }

    /**
     * Sets the per-file import details.
     *
     * @param details the list of per-file import details
     */
    public void setDetails(List<Detail> details) {
        this.details = details;
    }

    @Override
    public String toString() {
        return "GetImportProgressResponse{" +
                "jobId='" + jobId + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", fileName='" + fileName + '\'' +
                ", fileSize=" + fileSize +
                ", state='" + state + '\'' +
                ", progress=" + progress +
                ", completeTime='" + completeTime + '\'' +
                ", reason='" + reason + '\'' +
                ", totalRows=" + totalRows +
                ", details=" + details +
                '}';
    }

    /**
     * Returns a new builder for a {@link GetImportProgressResponse}.
     *
     * @return a {@code Detail} builder
     */
    public static Detail.DetailBuilder builder() {
        return new Detail.DetailBuilder();
    }

    /**
     * Builder for {@link GetImportProgressResponse}.
     */
    public static class GetImportProgressResponseBuilder {
        private String jobId;
        private String collectionName;
        private String fileName;
        private Integer fileSize;
        private String state;
        private Integer progress;
        private String completeTime;
        private String reason;
        private Integer totalRows;
        private List<Detail> details;

        private GetImportProgressResponseBuilder() {
            this.jobId = "";
            this.collectionName = "";
            this.fileName = "";
            this.fileSize = 0;
            this.state = "";
            this.progress = 0;
            this.completeTime = "";
            this.reason = "";
            this.totalRows = 0;
            this.details = new ArrayList<>();
        }

        /**
         * Sets the unique ID of the import job.
         *
         * @param jobId the import job ID
         * @return this builder
         */
        public GetImportProgressResponseBuilder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Sets the name of the collection targeted by the import job.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public GetImportProgressResponseBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the name of the file being imported.
         *
         * @param fileName the file name
         * @return this builder
         */
        public GetImportProgressResponseBuilder fileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        /**
         * Sets the size of the file being imported in bytes.
         *
         * @param fileSize the file size in bytes
         * @return this builder
         */
        public GetImportProgressResponseBuilder fileSize(Integer fileSize) {
            this.fileSize = fileSize;
            return this;
        }

        /**
         * Sets the current state of the import job.
         *
         * @param state the import job state
         * @return this builder
         */
        public GetImportProgressResponseBuilder state(String state) {
            this.state = state;
            return this;
        }

        /**
         * Sets the overall progress percentage of the import job.
         *
         * @param progress the progress percentage
         * @return this builder
         */
        public GetImportProgressResponseBuilder progress(Integer progress) {
            this.progress = progress;
            return this;
        }

        /**
         * Sets the time when the import job completed.
         *
         * @param completeTime the completion time
         * @return this builder
         */
        public GetImportProgressResponseBuilder completeTime(String completeTime) {
            this.completeTime = completeTime;
            return this;
        }

        /**
         * Sets the reason for failure, if the import job failed.
         *
         * @param reason the failure reason
         * @return this builder
         */
        public GetImportProgressResponseBuilder reason(String reason) {
            this.reason = reason;
            return this;
        }

        /**
         * Sets the total number of rows imported.
         *
         * @param totalRows the total number of imported rows
         * @return this builder
         */
        public GetImportProgressResponseBuilder totalRows(Integer totalRows) {
            this.totalRows = totalRows;
            return this;
        }

        /**
         * Sets the per-file import details.
         *
         * @param details the list of per-file import details
         * @return this builder
         */
        public GetImportProgressResponseBuilder details(List<Detail> details) {
            this.details = details;
            return this;
        }

        /**
         * Builds the {@link GetImportProgressResponse} instance.
         *
         * @return the built {@code GetImportProgressResponse}
         */
        public GetImportProgressResponse build() {
            return new GetImportProgressResponse(this);
        }
    }

    /**
     * Per-file progress information of a bulk import job.
     */
    public static class Detail {
        private String fileName;
        private Integer fileSize;
        private String state;
        private Integer progress;
        private String completeTime;
        private String reason;

        /**
         * Constructs an empty {@code Detail}.
         */
        public Detail() {
        }

        /**
         * Constructs a {@code Detail} with the given per-file import information.
         *
         * @param fileName     the name of the file
         * @param fileSize     the size of the file in bytes
         * @param state        the import state of the file
         * @param progress     the import progress percentage of the file
         * @param completeTime the time when the file import completed
         * @param reason       the reason for failure, if the file import failed
         */
        public Detail(String fileName, Integer fileSize, String state, Integer progress, String completeTime, String reason) {
            this.fileName = fileName;
            this.fileSize = fileSize;
            this.state = state;
            this.progress = progress;
            this.completeTime = completeTime;
            this.reason = reason;
        }

        private Detail(DetailBuilder builder) {
            this.fileName = builder.fileName;
            this.fileSize = builder.fileSize;
            this.state = builder.state;
            this.progress = builder.progress;
            this.completeTime = builder.completeTime;
            this.reason = builder.reason;
        }

        /**
         * Returns the name of the file.
         *
         * @return the file name
         */
        public String getFileName() {
            return fileName;
        }

        /**
         * Sets the name of the file.
         *
         * @param fileName the file name
         */
        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        /**
         * Returns the size of the file in bytes.
         *
         * @return the file size in bytes
         */
        public Integer getFileSize() {
            return fileSize;
        }

        /**
         * Sets the size of the file in bytes.
         *
         * @param fileSize the file size in bytes
         */
        public void setFileSize(Integer fileSize) {
            this.fileSize = fileSize;
        }

        /**
         * Returns the import state of the file.
         *
         * @return the file import state
         */
        public String getState() {
            return state;
        }

        /**
         * Sets the import state of the file.
         *
         * @param state the file import state
         */
        public void setState(String state) {
            this.state = state;
        }

        /**
         * Returns the import progress percentage of the file.
         *
         * @return the file import progress percentage
         */
        public Integer getProgress() {
            return progress;
        }

        /**
         * Sets the import progress percentage of the file.
         *
         * @param progress the file import progress percentage
         */
        public void setProgress(Integer progress) {
            this.progress = progress;
        }

        /**
         * Returns the time when the file import completed.
         *
         * @return the completion time
         */
        public String getCompleteTime() {
            return completeTime;
        }

        /**
         * Sets the time when the file import completed.
         *
         * @param completeTime the completion time
         */
        public void setCompleteTime(String completeTime) {
            this.completeTime = completeTime;
        }

        /**
         * Returns the reason for failure, if the file import failed.
         *
         * @return the failure reason
         */
        public String getReason() {
            return reason;
        }

        /**
         * Sets the reason for failure, if the file import failed.
         *
         * @param reason the failure reason
         */
        public void setReason(String reason) {
            this.reason = reason;
        }

        @Override
        public String toString() {
            return "Detail{" +
                    "fileName='" + fileName + '\'' +
                    ", fileSize=" + fileSize +
                    ", state='" + state + '\'' +
                    ", progress=" + progress +
                    ", completeTime='" + completeTime + '\'' +
                    ", reason='" + reason + '\'' +
                    '}';
        }

        /**
         * Returns a new builder for a {@link Detail}.
         *
         * @return a {@code Detail} builder
         */
        public static DetailBuilder builder() {
            return new DetailBuilder();
        }

        /**
         * Builder for {@link Detail}.
         */
        public static class DetailBuilder {
            private String fileName;
            private Integer fileSize;
            private String state;
            private Integer progress;
            private String completeTime;
            private String reason;

            private DetailBuilder() {
                this.fileName = "";
                this.fileSize = 0;
                this.state = "";
                this.progress = 0;
                this.completeTime = "";
                this.reason = "";
            }

            /**
             * Sets the name of the file.
             *
             * @param fileName the file name
             * @return this builder
             */
            public DetailBuilder fileName(String fileName) {
                this.fileName = fileName;
                return this;
            }

            /**
             * Sets the size of the file in bytes.
             *
             * @param fileSize the file size in bytes
             * @return this builder
             */
            public DetailBuilder fileSize(Integer fileSize) {
                this.fileSize = fileSize;
                return this;
            }

            /**
             * Sets the import state of the file.
             *
             * @param state the file import state
             * @return this builder
             */
            public DetailBuilder state(String state) {
                this.state = state;
                return this;
            }

            /**
             * Sets the import progress percentage of the file.
             *
             * @param progress the file import progress percentage
             * @return this builder
             */
            public DetailBuilder progress(Integer progress) {
                this.progress = progress;
                return this;
            }

            /**
             * Sets the time when the file import completed.
             *
             * @param completeTime the completion time
             * @return this builder
             */
            public DetailBuilder completeTime(String completeTime) {
                this.completeTime = completeTime;
                return this;
            }

            /**
             * Sets the reason for failure, if the file import failed.
             *
             * @param reason the failure reason
             * @return this builder
             */
            public DetailBuilder reason(String reason) {
                this.reason = reason;
                return this;
            }

            /**
             * Builds the {@link Detail} instance.
             *
             * @return the built {@code Detail}
             */
            public Detail build() {
                return new Detail(this);
            }
        }
    }
}
