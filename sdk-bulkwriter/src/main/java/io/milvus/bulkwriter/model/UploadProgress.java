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

public class UploadProgress {
    private final long uploadedBytes;
    private final long totalBytes;
    private final int completedFiles;
    private final long totalFiles;
    private final String currentFile;
    private final long currentFileUploadedBytes;
    private final long currentFileTotalBytes;
    private final double percent;

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

    public long getUploadedBytes() {
        return uploadedBytes;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public int getCompletedFiles() {
        return completedFiles;
    }

    public long getTotalFiles() {
        return totalFiles;
    }

    public String getCurrentFile() {
        return currentFile;
    }

    public long getCurrentFileUploadedBytes() {
        return currentFileUploadedBytes;
    }

    public long getCurrentFileTotalBytes() {
        return currentFileTotalBytes;
    }

    public double getPercent() {
        return percent;
    }
}
