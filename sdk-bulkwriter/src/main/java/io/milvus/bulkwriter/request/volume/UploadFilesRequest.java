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

/**
 * Request for uploading a local file or directory to a target directory within a Volume.
 *
 * <p>Allows configuring upload concurrency, retry behavior, multipart part size, and an
 * optional progress listener.</p>
 */


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
    /**
     * Creates a new UploadFilesRequest.
     */


    public UploadFilesRequest() {
    }
    /**
     * Creates a new UploadFilesRequest.
     *
     * @param sourceFilePath the sourceFilePath
     * @param targetVolumePath the targetVolumePath
     */


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
    /**
     * Returns the sourceFilePath.
     *
     * @return the sourceFilePath
     */


    public String getSourceFilePath() {
        return sourceFilePath;
    }
    /**
     * Sets the sourceFilePath.
     *
     * @param sourceFilePath the sourceFilePath
     */


    public void setSourceFilePath(String sourceFilePath) {
        this.sourceFilePath = sourceFilePath;
    }
    /**
     * Returns the targetVolumePath.
     *
     * @return the targetVolumePath
     */


    public String getTargetVolumePath() {
        return targetVolumePath;
    }
    /**
     * Sets the targetVolumePath.
     *
     * @param targetVolumePath the targetVolumePath
     */


    public void setTargetVolumePath(String targetVolumePath) {
        this.targetVolumePath = targetVolumePath;
    }
    /**
     * Returns the uploadConcurrency.
     *
     * @return the uploadConcurrency
     */


    public int getUploadConcurrency() {
        return uploadConcurrency;
    }
    /**
     * Sets the uploadConcurrency.
     *
     * @param uploadConcurrency the uploadConcurrency
     */


    public void setUploadConcurrency(int uploadConcurrency) {
        this.uploadConcurrency = uploadConcurrency;
    }
    /**
     * Returns the maxRetries.
     *
     * @return the maxRetries
     */


    public int getMaxRetries() {
        return maxRetries;
    }
    /**
     * Sets the maxRetries.
     *
     * @param maxRetries the maxRetries
     */


    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
    /**
     * Returns the retryIntervalMillis.
     *
     * @return the retryIntervalMillis
     */


    public long getRetryIntervalMillis() {
        return retryIntervalMillis;
    }
    /**
     * Sets the retryIntervalMillis.
     *
     * @param retryIntervalMillis the retryIntervalMillis
     */


    public void setRetryIntervalMillis(long retryIntervalMillis) {
        this.retryIntervalMillis = retryIntervalMillis;
    }
    /**
     * Returns the progressListener.
     *
     * @return the progressListener
     */


    public ProgressListener getProgressListener() {
        return progressListener;
    }
    /**
     * Sets the progressListener.
     *
     * @param progressListener the progressListener
     */


    public void setProgressListener(ProgressListener progressListener) {
        this.progressListener = progressListener;
    }
    /**
     * Returns the partSizeBytes.
     *
     * @return the partSizeBytes
     */


    public long getPartSizeBytes() {
        return partSizeBytes;
    }
    /**
     * Sets the partSizeBytes.
     *
     * @param partSizeBytes the partSizeBytes
     */


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
    /**
     * Creates a new builder.
     *
     * @return the builder
     */


    public static UploadFilesRequestBuilder builder() {
        return new UploadFilesRequestBuilder();
    }

    /**
     * Builder for {@link UploadFilesRequest} class.
     */


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
        /**
         * Sets the sourceFilePath.
         *
         * @param sourceFilePath the sourceFilePath
         * @return this builder
         */


        public UploadFilesRequestBuilder sourceFilePath(String sourceFilePath) {
            this.sourceFilePath = sourceFilePath;
            return this;
        }
        /**
         * Sets the targetVolumePath.
         *
         * @param targetVolumePath the targetVolumePath
         * @return this builder
         */


        public UploadFilesRequestBuilder targetVolumePath(String targetVolumePath) {
            this.targetVolumePath = targetVolumePath;
            return this;
        }
        /**
         * Sets the uploadConcurrency.
         *
         * @param uploadConcurrency the uploadConcurrency
         * @return this builder
         */


        public UploadFilesRequestBuilder uploadConcurrency(int uploadConcurrency) {
            this.uploadConcurrency = uploadConcurrency;
            return this;
        }
        /**
         * Sets the maxRetries.
         *
         * @param maxRetries the maxRetries
         * @return this builder
         */


        public UploadFilesRequestBuilder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }
        /**
         * Sets the retryIntervalMillis.
         *
         * @param retryIntervalMillis the retryIntervalMillis
         * @return this builder
         */


        public UploadFilesRequestBuilder retryIntervalMillis(long retryIntervalMillis) {
            this.retryIntervalMillis = retryIntervalMillis;
            return this;
        }
        /**
         * Sets the progressListener.
         *
         * @param progressListener the progressListener
         * @return this builder
         */


        public UploadFilesRequestBuilder progressListener(ProgressListener progressListener) {
            this.progressListener = progressListener;
            return this;
        }
        /**
         * Sets the partSizeBytes.
         *
         * @param partSizeBytes the partSizeBytes
         * @return this builder
         */


        public UploadFilesRequestBuilder partSizeBytes(long partSizeBytes) {
            this.partSizeBytes = partSizeBytes;
            return this;
        }
        /**
         * Builds the UploadFilesRequest.
         *
         * @return the built UploadFilesRequest
         */


        public UploadFilesRequest build() {
            return new UploadFilesRequest(this);
        }
    }

    /**
     * Listener for upload progress callbacks.
     */
    @FunctionalInterface
    public interface ProgressListener {
        void onProgress(UploadProgress progress);
    }
}
