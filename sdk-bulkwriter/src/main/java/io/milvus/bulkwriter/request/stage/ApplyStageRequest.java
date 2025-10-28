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

public class ApplyStageRequest extends BaseStageRequest {
    private String stageName;
    private String path;

    protected ApplyStageRequest() {
    }

    protected ApplyStageRequest(String stageName, String path) {
        this.stageName = stageName;
        this.path = path;
    }

    protected ApplyStageRequest(ApplyStageRequestBuilder builder) {
        super(builder);
        this.stageName = builder.stageName;
        this.path = builder.path;
    }

    public String getStageName() {
        return stageName;
    }

    public void setStageName(String stageName) {
        this.stageName = stageName;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return "ApplyStageRequest{" +
                "stageName='" + stageName + '\'' +
                ", path='" + path + '\'' +
                '}';
    }

    public static ApplyStageRequestBuilder builder() {
        return new ApplyStageRequestBuilder();
    }

    public static class ApplyStageRequestBuilder extends BaseStageRequestBuilder<ApplyStageRequestBuilder> {
        private String stageName;
        private String path;

        private ApplyStageRequestBuilder() {
            this.stageName = "";
            this.path = "";
        }

        public ApplyStageRequestBuilder stageName(String stageName) {
            this.stageName = stageName;
            return this;
        }

        public ApplyStageRequestBuilder path(String path) {
            this.path = path;
            return this;
        }

        public ApplyStageRequest build() {
            return new ApplyStageRequest(this);
        }
    }
}
