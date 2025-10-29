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

package io.milvus.bulkwriter.request.stage;

public class CreateStageRequest {
    private String projectId;
    private String regionId;
    private String stageName;

    public CreateStageRequest() {
    }

    public CreateStageRequest(String projectId, String regionId, String stageName) {
        this.projectId = projectId;
        this.regionId = regionId;
        this.stageName = stageName;
    }

    protected CreateStageRequest(CreateStageRequestBuilder builder) {
        this.projectId = builder.projectId;
        this.regionId = builder.regionId;
        this.stageName = builder.stageName;
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

    public String getStageName() {
        return stageName;
    }

    public void setStageName(String stageName) {
        this.stageName = stageName;
    }

    @Override
    public String toString() {
        return "CreateStageRequest{" +
                "projectId='" + projectId + '\'' +
                ", regionId='" + regionId + '\'' +
                ", stageName='" + stageName + '\'' +
                '}';
    }

    public static CreateStageRequestBuilder builder() {
        return new CreateStageRequestBuilder();
    }

    public static class CreateStageRequestBuilder {
        private String projectId;
        private String regionId;
        private String stageName;

        private CreateStageRequestBuilder() {
            this.projectId = "";
            this.regionId = "";
            this.stageName = "";
        }

        public CreateStageRequestBuilder projectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public CreateStageRequestBuilder regionId(String regionId) {
            this.regionId = regionId;
            return this;
        }

        public CreateStageRequestBuilder stageName(String stageName) {
            this.stageName = stageName;
            return this;
        }

        public CreateStageRequest build() {
            return new CreateStageRequest(this);
        }
    }
}
