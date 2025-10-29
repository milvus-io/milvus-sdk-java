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

    public GetImportProgressResponse() {
    }

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

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Integer getFileSize() {
        return fileSize;
    }

    public void setFileSize(Integer fileSize) {
        this.fileSize = fileSize;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Integer getProgress() {
        return progress;
    }

    public void setProgress(Integer progress) {
        this.progress = progress;
    }

    public String getCompleteTime() {
        return completeTime;
    }

    public void setCompleteTime(String completeTime) {
        this.completeTime = completeTime;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public Integer getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(Integer totalRows) {
        this.totalRows = totalRows;
    }

    public List<Detail> getDetails() {
        return details;
    }

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

    public static Detail.DetailBuilder builder() {
        return new Detail.DetailBuilder();
    }

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

        public GetImportProgressResponseBuilder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public GetImportProgressResponseBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public GetImportProgressResponseBuilder fileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public GetImportProgressResponseBuilder fileSize(Integer fileSize) {
            this.fileSize = fileSize;
            return this;
        }

        public GetImportProgressResponseBuilder state(String state) {
            this.state = state;
            return this;
        }

        public GetImportProgressResponseBuilder progress(Integer progress) {
            this.progress = progress;
            return this;
        }

        public GetImportProgressResponseBuilder completeTime(String completeTime) {
            this.completeTime = completeTime;
            return this;
        }

        public GetImportProgressResponseBuilder reason(String reason) {
            this.reason = reason;
            return this;
        }

        public GetImportProgressResponseBuilder totalRows(Integer totalRows) {
            this.totalRows = totalRows;
            return this;
        }

        public GetImportProgressResponseBuilder details(List<Detail> details) {
            this.details = details;
            return this;
        }

        public GetImportProgressResponse build() {
            return new GetImportProgressResponse(this);
        }
    }

    public static class Detail {
        private String fileName;
        private Integer fileSize;
        private String state;
        private Integer progress;
        private String completeTime;
        private String reason;

        public Detail() {
        }

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

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public Integer getFileSize() {
            return fileSize;
        }

        public void setFileSize(Integer fileSize) {
            this.fileSize = fileSize;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public Integer getProgress() {
            return progress;
        }

        public void setProgress(Integer progress) {
            this.progress = progress;
        }

        public String getCompleteTime() {
            return completeTime;
        }

        public void setCompleteTime(String completeTime) {
            this.completeTime = completeTime;
        }

        public String getReason() {
            return reason;
        }

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

        public static DetailBuilder builder() {
            return new DetailBuilder();
        }

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

            public DetailBuilder fileName(String fileName) {
                this.fileName = fileName;
                return this;
            }

            public DetailBuilder fileSize(Integer fileSize) {
                this.fileSize = fileSize;
                return this;
            }

            public DetailBuilder state(String state) {
                this.state = state;
                return this;
            }

            public DetailBuilder progress(Integer progress) {
                this.progress = progress;
                return this;
            }

            public DetailBuilder completeTime(String completeTime) {
                this.completeTime = completeTime;
                return this;
            }

            public DetailBuilder reason(String reason) {
                this.reason = reason;
                return this;
            }

            public Detail build() {
                return new Detail(this);
            }
        }
    }
}
