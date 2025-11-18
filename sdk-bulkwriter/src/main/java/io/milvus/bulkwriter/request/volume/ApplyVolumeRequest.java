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

public class ApplyVolumeRequest extends BaseVolumeRequest {
    private String volumeName;
    private String path;

    protected ApplyVolumeRequest() {
    }

    protected ApplyVolumeRequest(String volumeName, String path) {
        this.volumeName = volumeName;
        this.path = path;
    }

    protected ApplyVolumeRequest(ApplyVolumeRequestBuilder builder) {
        super(builder);
        this.volumeName = builder.volumeName;
        this.path = builder.path;
    }

    public String getVolumeName() {
        return volumeName;
    }

    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return "ApplyVolumeRequest{" +
                "volumeName='" + volumeName + '\'' +
                ", path='" + path + '\'' +
                '}';
    }

    public static ApplyVolumeRequestBuilder builder() {
        return new ApplyVolumeRequestBuilder();
    }

    public static class ApplyVolumeRequestBuilder extends BaseVolumeRequestBuilder<ApplyVolumeRequestBuilder> {
        private String volumeName;
        private String path;

        private ApplyVolumeRequestBuilder() {
            this.volumeName = "";
            this.path = "";
        }

        public ApplyVolumeRequestBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        public ApplyVolumeRequestBuilder path(String path) {
            this.path = path;
            return this;
        }

        public ApplyVolumeRequest build() {
            return new ApplyVolumeRequest(this);
        }
    }
}
