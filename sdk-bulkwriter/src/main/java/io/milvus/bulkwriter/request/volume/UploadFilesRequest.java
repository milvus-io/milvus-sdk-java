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

    public UploadFilesRequest() {
    }

    public UploadFilesRequest(String sourceFilePath, String targetVolumePath) {
        this.sourceFilePath = sourceFilePath;
        this.targetVolumePath = targetVolumePath;
    }

    protected UploadFilesRequest(UploadFilesRequestBuilder builder) {
        this.sourceFilePath = builder.sourceFilePath;
        this.targetVolumePath = builder.targetVolumePath;
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

    @Override
    public String toString() {
        return "UploadFilesRequest{" +
                "sourceFilePath='" + sourceFilePath + '\'' +
                ", targetVolumePath='" + targetVolumePath + '\'' +
                '}';
    }

    public static UploadFilesRequestBuilder builder() {
        return new UploadFilesRequestBuilder();
    }

    public static class UploadFilesRequestBuilder {
        private String sourceFilePath;
        private String targetVolumePath;

        private UploadFilesRequestBuilder() {
            this.sourceFilePath = "";
            this.targetVolumePath = "";
        }

        public UploadFilesRequestBuilder sourceFilePath(String sourceFilePath) {
            this.sourceFilePath = sourceFilePath;
            return this;
        }

        public UploadFilesRequestBuilder targetVolumePath(String targetVolumePath) {
            this.targetVolumePath = targetVolumePath;
            return this;
        }

        public UploadFilesRequest build() {
            return new UploadFilesRequest(this);
        }
    }
}
