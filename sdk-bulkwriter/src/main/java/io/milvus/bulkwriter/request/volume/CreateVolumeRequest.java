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

/**
 * Request for creating a volume under a specified project and region.
 *
 * <p>Supports MANAGED or EXTERNAL volume types; for EXTERNAL volumes a storage
 * integration and optional storage path can be specified.</p>
 */


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
    /**
     * Creates a new CreateVolumeRequest.
     */


    public CreateVolumeRequest() {
    }
    /**
     * Creates a new CreateVolumeRequest.
     *
     * @param projectId the projectId
     * @param regionId the regionId
     * @param volumeName the volumeName
     */


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
    /**
     * Returns the projectId.
     *
     * @return the projectId
     */


    public String getProjectId() {
        return projectId;
    }
    /**
     * Sets the projectId.
     *
     * @param projectId the projectId
     */


    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }
    /**
     * Returns the regionId.
     *
     * @return the regionId
     */


    public String getRegionId() {
        return regionId;
    }
    /**
     * Sets the regionId.
     *
     * @param regionId the regionId
     */


    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }
    /**
     * Returns the volumeName.
     *
     * @return the volumeName
     */


    public String getVolumeName() {
        return volumeName;
    }
    /**
     * Sets the volumeName.
     *
     * @param volumeName the volumeName
     */


    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }
    /**
     * Returns the type.
     *
     * @return the type
     */


    public String getType() {
        return type;
    }
    /**
     * Sets the type.
     *
     * @param type the type
     */


    public void setType(String type) {
        this.type = type;
    }
    /**
     * Returns the storageIntegrationId.
     *
     * @return the storageIntegrationId
     */


    public String getStorageIntegrationId() {
        return storageIntegrationId;
    }
    /**
     * Sets the storageIntegrationId.
     *
     * @param storageIntegrationId the storageIntegrationId
     */


    public void setStorageIntegrationId(String storageIntegrationId) {
        this.storageIntegrationId = storageIntegrationId;
    }
    /**
     * Returns the path.
     *
     * @return the path
     */


    public String getPath() {
        return path;
    }
    /**
     * Sets the path.
     *
     * @param path the path
     */


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
    /**
     * Creates a new builder.
     *
     * @return the builder
     */


    public static CreateVolumeRequestBuilder builder() {
        return new CreateVolumeRequestBuilder();
    }

    /**
     * Builder for {@link CreateVolumeRequest} class.
     */


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
        /**
         * Sets the projectId.
         *
         * @param projectId the projectId
         * @return this builder
         */


        public CreateVolumeRequestBuilder projectId(String projectId) {
            this.projectId = projectId;
            return this;
        }
        /**
         * Sets the regionId.
         *
         * @param regionId the regionId
         * @return this builder
         */


        public CreateVolumeRequestBuilder regionId(String regionId) {
            this.regionId = regionId;
            return this;
        }
        /**
         * Sets the volumeName.
         *
         * @param volumeName the volumeName
         * @return this builder
         */


        public CreateVolumeRequestBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        /**
         * Set volume type.
         * Available values: MANAGED or EXTERNAL. Defaults to MANAGED when not set.
         *
         * @param type the volume type; MANAGED or EXTERNAL
         * @return this builder
         */


        public CreateVolumeRequestBuilder type(String type) {
            this.type = type;
            return this;
        }
        /**
         * Sets the storageIntegrationId.
         *
         * @param storageIntegrationId the storageIntegrationId
         * @return this builder
         */


        public CreateVolumeRequestBuilder storageIntegrationId(String storageIntegrationId) {
            this.storageIntegrationId = storageIntegrationId;
            return this;
        }

        /**
         * Set storage path for EXTERNAL volume.
         * If not set, defaults to the root directory of storage integration.
         * If set, the path must end with '/'.
         *
         * @param path the storage path for EXTERNAL volume
         * @return this builder
         */


        public CreateVolumeRequestBuilder path(String path) {
            this.path = path;
            return this;
        }
        /**
         * Builds the CreateVolumeRequest.
         *
         * @return the built CreateVolumeRequest
         */


        public CreateVolumeRequest build() {
            return new CreateVolumeRequest(this);
        }
    }
}
