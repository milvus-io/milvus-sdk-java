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

package io.milvus.bulkwriter.model;

/**
 * A snapshot of the progress of a file upload to a cloud storage volume.
 *
 * <p>It reports the uploaded and total bytes, the number of completed files, the file
 * currently being uploaded, and the overall upload percentage.</p>
 */
public class UploadProgress {
    private final long uploadedBytes;
    private final long totalBytes;
    private final int completedFiles;
    private final long totalFiles;
    private final String currentFile;
    private final long currentFileUploadedBytes;
    private final long currentFileTotalBytes;
    private final double percent;

    /**
     * Constructs an {@code UploadProgress} with the given upload statistics.
     *
     * @param uploadedBytes            the total number of bytes uploaded so far
     * @param totalBytes               the total number of bytes to upload
     * @param completedFiles           the number of files fully uploaded
     * @param totalFiles               the total number of files to upload
     * @param currentFile              the name of the file currently being uploaded
     * @param currentFileUploadedBytes the number of bytes uploaded for the current file
     * @param currentFileTotalBytes    the total size of the current file in bytes
     * @param percent                  the overall upload percentage
     */
    public UploadProgress(long uploadedBytes, long totalBytes, int completedFiles, long totalFiles,
                          String currentFile, long currentFileUploadedBytes,
                          long currentFileTotalBytes, double percent) {
        this.uploadedBytes = uploadedBytes;
        this.totalBytes = totalBytes;
        this.completedFiles = completedFiles;
        this.totalFiles = totalFiles;
        this.currentFile = currentFile;
        this.currentFileUploadedBytes = currentFileUploadedBytes;
        this.currentFileTotalBytes = currentFileTotalBytes;
        this.percent = percent;
    }

    /**
     * Returns the total number of bytes uploaded so far.
     *
     * @return the number of uploaded bytes
     */
    public long getUploadedBytes() {
        return uploadedBytes;
    }

    /**
     * Returns the total number of bytes to upload.
     *
     * @return the total number of bytes to upload
     */
    public long getTotalBytes() {
        return totalBytes;
    }

    /**
     * Returns the number of files fully uploaded.
     *
     * @return the number of completed files
     */
    public int getCompletedFiles() {
        return completedFiles;
    }

    /**
     * Returns the total number of files to upload.
     *
     * @return the total number of files to upload
     */
    public long getTotalFiles() {
        return totalFiles;
    }

    /**
     * Returns the name of the file currently being uploaded.
     *
     * @return the current file name
     */
    public String getCurrentFile() {
        return currentFile;
    }

    /**
     * Returns the number of bytes uploaded for the current file.
     *
     * @return the number of uploaded bytes of the current file
     */
    public long getCurrentFileUploadedBytes() {
        return currentFileUploadedBytes;
    }

    /**
     * Returns the total size of the current file in bytes.
     *
     * @return the total size of the current file
     */
    public long getCurrentFileTotalBytes() {
        return currentFileTotalBytes;
    }

    /**
     * Returns the overall upload percentage.
     *
     * @return the upload percentage
     */
    public double getPercent() {
        return percent;
    }
}
