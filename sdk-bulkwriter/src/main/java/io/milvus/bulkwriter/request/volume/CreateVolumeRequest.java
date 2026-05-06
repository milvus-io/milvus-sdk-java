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

public class CreateVolumeRequest {
    private String projectId;
    private String regionId;
    private String volumeName;
    // Volume type, available values: MANAGED or EXTERNAL. Defaults to MANAGED when not set.
    private String type;
    private String storageIntegrationId;
    // For EXTERNAL volume: if not set, defaults to the root directory of storage integration;
    // if set, the path must end with '/'.
    private String path;

    public CreateVolumeRequest() {
    }

    public CreateVolumeRequest(String projectId, String regionId, String volumeName) {
        this.projectId = projectId;
        this.regionId = regionId;
        this.volumeName = volumeName;
    }

    protected CreateVolumeRequest(CreateVolumeRequestBuilder builder) {
        this.projectId = builder.projectId;
        this.regionId = builder.regionId;
        this.volumeName = builder.volumeName;
        this.type = builder.type;
        this.storageIntegrationId = builder.storageIntegrationId;
        this.path = builder.path;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public String getVolumeName() {
        return volumeName;
    }

    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getStorageIntegrationId() {
        return storageIntegrationId;
    }

    public void setStorageIntegrationId(String storageIntegrationId) {
        this.storageIntegrationId = storageIntegrationId;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return "CreateVolumeRequest{" +
                "projectId='" + projectId + '\'' +
                ", regionId='" + regionId + '\'' +
                ", volumeName='" + volumeName + '\'' +
                ", type='" + type + '\'' +
                ", storageIntegrationId='" + storageIntegrationId + '\'' +
                ", path='" + path + '\'' +
                '}';
    }

    public static CreateVolumeRequestBuilder builder() {
        return new CreateVolumeRequestBuilder();
    }

    public static class CreateVolumeRequestBuilder {
        private String projectId;
        private String regionId;
        private String volumeName;
        private String type;
        private String storageIntegrationId;
        private String path;

        private CreateVolumeRequestBuilder() {
            this.projectId = "";
            this.regionId = "";
            this.volumeName = "";
        }

        public CreateVolumeRequestBuilder projectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public CreateVolumeRequestBuilder regionId(String regionId) {
            this.regionId = regionId;
            return this;
        }

        public CreateVolumeRequestBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        /**
         * Set volume type.
         * Available values: MANAGED or EXTERNAL. Defaults to MANAGED when not set.
         */
        public CreateVolumeRequestBuilder type(String type) {
            this.type = type;
            return this;
        }

        public CreateVolumeRequestBuilder storageIntegrationId(String storageIntegrationId) {
            this.storageIntegrationId = storageIntegrationId;
            return this;
        }

        /**
         * Set storage path for EXTERNAL volume.
         * If not set, defaults to the root directory of storage integration.
         * If set, the path must end with '/'.
         */
        public CreateVolumeRequestBuilder path(String path) {
            this.path = path;
            return this;
        }

        public CreateVolumeRequest build() {
            return new CreateVolumeRequest(this);
        }
    }
}
