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
 * Request parameters for deleting a cloud storage volume.
 *
 * <p>It identifies the volume to delete through its volume name.</p>
 */
public class DeleteVolumeRequest {
    private String volumeName;

    /**
     * Constructs an empty {@code DeleteVolumeRequest}.
     */
    public DeleteVolumeRequest() {
    }

    /**
     * Constructs a {@code DeleteVolumeRequest} with the given volume name.
     *
     * @param volumeName the name of the volume to delete
     */
    public DeleteVolumeRequest(String volumeName) {
        this.volumeName = volumeName;
    }

    protected DeleteVolumeRequest(DeleteVolumeRequestBuilder builder) {
        this.volumeName = builder.volumeName;
    }

    /**
     * Returns the name of the volume to delete.
     *
     * @return the volume name
     */
    public String getVolumeName() {
        return volumeName;
    }

    /**
     * Sets the name of the volume to delete.
     *
     * @param volumeName the volume name
     */
    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }

    @Override
    public String toString() {
        return "DeleteVolumeRequest{" +
                "volumeName='" + volumeName + '\'' +
                '}';
    }

    /**
     * Returns a new builder for a {@link DeleteVolumeRequest}.
     *
     * @return a {@code DeleteVolumeRequest} builder
     */
    public static DeleteVolumeRequestBuilder builder() {
        return new DeleteVolumeRequestBuilder();
    }

    /**
     * Builder for {@link DeleteVolumeRequest}.
     */
    public static class DeleteVolumeRequestBuilder {
        private String volumeName;

        private DeleteVolumeRequestBuilder() {
            this.volumeName = "";
        }

        /**
         * Sets the name of the volume to delete.
         *
         * @param volumeName the volume name
         * @return this builder
         */
        public DeleteVolumeRequestBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        /**
         * Builds the {@link DeleteVolumeRequest} instance.
         *
         * @return the built {@code DeleteVolumeRequest}
         */
        public DeleteVolumeRequest build() {
            return new DeleteVolumeRequest(this);
        }
    }
}
