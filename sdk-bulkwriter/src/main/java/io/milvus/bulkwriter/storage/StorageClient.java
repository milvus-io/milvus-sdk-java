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

package io.milvus.bulkwriter.storage;


import java.io.File;

/**
 * Defines the contract for uploading and inspecting bulk-import data files in remote/cloud
 * storage.
 *
 * <p>Implementations abstract the underlying cloud-storage SDK (S3-compatible services such as
 * MinIO, or Azure Blob Storage), mapping data files to objects in a bucket and reporting object
 * sizes, bucket existence, and upload progress.
 */
public interface StorageClient {
    /**
     * Returns the size in bytes of the object stored in the bucket.
     *
     * @param bucketName the cloud-storage bucket name
     * @param objectKey the object key
     * @return the object size in bytes
     * @throws Exception if the object cannot be statted
     */
    Long getObjectEntity(String bucketName, String objectKey) throws Exception;

    /**
     * Checks whether the given bucket exists in the cloud storage.
     *
     * @param bucketName the cloud-storage bucket name
     * @return {@code true} if the bucket exists, {@code false} otherwise
     * @throws Exception if the bucket existence cannot be determined
     */
    boolean checkBucketExist(String bucketName) throws Exception;

    /**
     * Uploads a local data file as an object to the bucket without progress reporting.
     *
     * @param file the local data file to upload
     * @param bucketName the cloud-storage bucket name
     * @param objectKey the object key
     * @throws Exception if the upload fails
     */
    void putObject(File file, String bucketName, String objectKey) throws Exception;

    /**
     * Uploads a local data file as an object to the bucket, reporting progress.
     *
     * @param file the local data file to upload
     * @param bucketName the cloud-storage bucket name
     * @param objectKey the object key
     * @param progressListener the listener notified of upload progress
     * @throws Exception if the upload fails
     */
    default void putObject(File file, String bucketName, String objectKey,
                           UploadProgressListener progressListener) throws Exception {
        putObject(file, bucketName, objectKey);
    }

    /**
     * Uploads a local data file as an object to the bucket with progress reporting and an
     * optional explicit multipart part size.
     *
     * @param file the local data file to upload
     * @param bucketName the cloud-storage bucket name
     * @param objectKey the object key
     * @param progressListener the listener notified of upload progress
     * @param partSizeBytes the multipart part size in bytes, or {@code 0} to auto-calculate
     * @throws Exception if the upload fails
     */
    default void putObject(File file, String bucketName, String objectKey,
                           UploadProgressListener progressListener, long partSizeBytes) throws Exception {
        putObject(file, bucketName, objectKey, progressListener);
    }

    /**
     * Releases any resources held by the storage client.
     */
    default void close() {
    }

    /**
     * Callback interface for reporting the progress of a data-file upload.
     */
    @FunctionalInterface
    interface UploadProgressListener {
        /**
         * Called each time a batch of bytes is uploaded.
         *
         * @param bytes the number of bytes uploaded since the previous invocation
         */
        void onProgress(long bytes);
    }
}
