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

import io.milvus.bulkwriter.model.UploadProgress;

public class UploadFilesRequest {
    /**
     * The full path of a local file or directory:
     * If it is a file, please include the file name, e.g., /Users/zilliz/data/1.parquet
     * If it is a directory, please end the path with a /, e.g., /Users/zilliz/data/
     */
    private String sourceFilePath;

    /**
     * The target volume directory path:
     * Leave it empty to upload to the root directory.
     * To upload to a specific folder, end the path with a /, e.g., data/
     */
    private String targetVolumePath;

    /**
     * The maximum number of files to upload concurrently.
     */
    private int uploadConcurrency = 5;

    /**
     * The maximum retry count for each file.
     */
    private int maxRetries = 5;

    /**
     * Retry interval in milliseconds.
     */
    private long retryIntervalMillis = 5000L;

    /**
     * Optional callback for upload progress snapshots.
     */
    private ProgressListener progressListener = null;

    /**
     * Multipart upload part size in bytes. Zero or negative means automatic.
     */
    private long partSizeBytes = 0L;

    public UploadFilesRequest() {
    }

    public UploadFilesRequest(String sourceFilePath, String targetVolumePath) {
        this.sourceFilePath = sourceFilePath;
        this.targetVolumePath = targetVolumePath;
    }

    protected UploadFilesRequest(UploadFilesRequestBuilder builder) {
        this.sourceFilePath = builder.sourceFilePath;
        this.targetVolumePath = builder.targetVolumePath;
        this.uploadConcurrency = builder.uploadConcurrency;
        this.maxRetries = builder.maxRetries;
        this.retryIntervalMillis = builder.retryIntervalMillis;
        this.progressListener = builder.progressListener;
        this.partSizeBytes = builder.partSizeBytes;
    }

    public String getSourceFilePath() {
        return sourceFilePath;
    }

    public void setSourceFilePath(String sourceFilePath) {
        this.sourceFilePath = sourceFilePath;
    }

    public String getTargetVolumePath() {
        return targetVolumePath;
    }

    public void setTargetVolumePath(String targetVolumePath) {
        this.targetVolumePath = targetVolumePath;
    }

    public int getUploadConcurrency() {
        return uploadConcurrency;
    }

    public void setUploadConcurrency(int uploadConcurrency) {
        this.uploadConcurrency = uploadConcurrency;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getRetryIntervalMillis() {
        return retryIntervalMillis;
    }

    public void setRetryIntervalMillis(long retryIntervalMillis) {
        this.retryIntervalMillis = retryIntervalMillis;
    }

    public ProgressListener getProgressListener() {
        return progressListener;
    }

    public void setProgressListener(ProgressListener progressListener) {
        this.progressListener = progressListener;
    }

    public long getPartSizeBytes() {
        return partSizeBytes;
    }

    public void setPartSizeBytes(long partSizeBytes) {
        this.partSizeBytes = partSizeBytes;
    }

    @Override
    public String toString() {
        return "UploadFilesRequest{" +
                "sourceFilePath='" + sourceFilePath + '\'' +
                ", targetVolumePath='" + targetVolumePath + '\'' +
                ", uploadConcurrency=" + uploadConcurrency +
                ", maxRetries=" + maxRetries +
                ", retryIntervalMillis=" + retryIntervalMillis +
                ", progressListener=" + (progressListener != null) +
                ", partSizeBytes=" + partSizeBytes +
                '}';
    }

    public static UploadFilesRequestBuilder builder() {
        return new UploadFilesRequestBuilder();
    }

    public static class UploadFilesRequestBuilder {
        private String sourceFilePath;
        private String targetVolumePath;
        private int uploadConcurrency;
        private int maxRetries;
        private long retryIntervalMillis;
        private ProgressListener progressListener;
        private long partSizeBytes;

        private UploadFilesRequestBuilder() {
            this.sourceFilePath = "";
            this.targetVolumePath = "";
            this.uploadConcurrency = 5;
            this.maxRetries = 5;
            this.retryIntervalMillis = 5000L;
            this.progressListener = null;
            this.partSizeBytes = 0L;
        }

        public UploadFilesRequestBuilder sourceFilePath(String sourceFilePath) {
            this.sourceFilePath = sourceFilePath;
            return this;
        }

        public UploadFilesRequestBuilder targetVolumePath(String targetVolumePath) {
            this.targetVolumePath = targetVolumePath;
            return this;
        }

        public UploadFilesRequestBuilder uploadConcurrency(int uploadConcurrency) {
            this.uploadConcurrency = uploadConcurrency;
            return this;
        }

        public UploadFilesRequestBuilder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public UploadFilesRequestBuilder retryIntervalMillis(long retryIntervalMillis) {
            this.retryIntervalMillis = retryIntervalMillis;
            return this;
        }

        public UploadFilesRequestBuilder progressListener(ProgressListener progressListener) {
            this.progressListener = progressListener;
            return this;
        }

        public UploadFilesRequestBuilder partSizeBytes(long partSizeBytes) {
            this.partSizeBytes = partSizeBytes;
            return this;
        }

        public UploadFilesRequest build() {
            return new UploadFilesRequest(this);
        }
    }

    @FunctionalInterface
    public interface ProgressListener {
        void onProgress(UploadProgress progress);
    }
}
