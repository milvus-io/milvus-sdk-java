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

public class DeleteStageRequest {
    private String stageName;

    public DeleteStageRequest() {
    }

    public DeleteStageRequest(String stageName) {
        this.stageName = stageName;
    }

    protected DeleteStageRequest(DeleteStageRequestBuilder builder) {
        this.stageName = builder.stageName;
    }

    public String getStageName() {
        return stageName;
    }

    public void setStageName(String stageName) {
        this.stageName = stageName;
    }

    @Override
    public String toString() {
        return "DeleteStageRequest{" +
                "stageName='" + stageName + '\'' +
                '}';
    }

    public static DeleteStageRequestBuilder builder() {
        return new DeleteStageRequestBuilder();
    }

    public static class DeleteStageRequestBuilder {
        private String stageName;

        private DeleteStageRequestBuilder() {
            this.stageName = "";
        }

        public DeleteStageRequestBuilder stageName(String stageName) {
            this.stageName = stageName;
            return this;
        }

        public DeleteStageRequest build() {
            return new DeleteStageRequest(this);
        }
    }
}
