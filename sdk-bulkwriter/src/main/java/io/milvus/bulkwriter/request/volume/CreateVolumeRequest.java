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

    @Override
    public String toString() {
        return "CreateVolumeRequest{" +
                "projectId='" + projectId + '\'' +
                ", regionId='" + regionId + '\'' +
                ", volumeName='" + volumeName + '\'' +
                '}';
    }

    public static CreateVolumeRequestBuilder builder() {
        return new CreateVolumeRequestBuilder();
    }

    public static class CreateVolumeRequestBuilder {
        private String projectId;
        private String regionId;
        private String volumeName;

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

        public CreateVolumeRequest build() {
            return new CreateVolumeRequest(this);
        }
    }
}
