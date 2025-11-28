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

public class DeleteVolumeRequest {
    private String volumeName;

    public DeleteVolumeRequest() {
    }

    public DeleteVolumeRequest(String volumeName) {
        this.volumeName = volumeName;
    }

    protected DeleteVolumeRequest(DeleteVolumeRequestBuilder builder) {
        this.volumeName = builder.volumeName;
    }

    public String getVolumeName() {
        return volumeName;
    }

    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }

    @Override
    public String toString() {
        return "DeleteVolumeRequest{" +
                "volumeName='" + volumeName + '\'' +
                '}';
    }

    public static DeleteVolumeRequestBuilder builder() {
        return new DeleteVolumeRequestBuilder();
    }

    public static class DeleteVolumeRequestBuilder {
        private String volumeName;

        private DeleteVolumeRequestBuilder() {
            this.volumeName = "";
        }

        public DeleteVolumeRequestBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        public DeleteVolumeRequest build() {
            return new DeleteVolumeRequest(this);
        }
    }
}
