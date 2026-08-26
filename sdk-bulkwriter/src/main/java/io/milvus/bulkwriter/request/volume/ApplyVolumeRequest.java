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
 * Request parameters for applying for a cloud storage volume used by the BulkWriter.
 *
 * <p>It specifies the volume to apply for and the path within it where files are uploaded,
 * and inherits the API key and options handling from {@link BaseVolumeRequest}.</p>
 */
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

    /**
     * Returns the name of the volume to apply for.
     *
     * @return the volume name
     */
    public String getVolumeName() {
        return volumeName;
    }

    /**
     * Sets the name of the volume to apply for.
     *
     * @param volumeName the volume name
     */
    public void setVolumeName(String volumeName) {
        this.volumeName = volumeName;
    }

    /**
     * Returns the path within the volume where files are uploaded.
     *
     * @return the upload path
     */
    public String getPath() {
        return path;
    }

    /**
     * Sets the path within the volume where files are uploaded.
     *
     * @param path the upload path
     */
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

    /**
     * Returns a new builder for an {@link ApplyVolumeRequest}.
     *
     * @return an {@code ApplyVolumeRequest} builder
     */
    public static ApplyVolumeRequestBuilder builder() {
        return new ApplyVolumeRequestBuilder();
    }

    /**
     * Builder for {@link ApplyVolumeRequest}.
     */
    public static class ApplyVolumeRequestBuilder extends BaseVolumeRequestBuilder<ApplyVolumeRequestBuilder> {
        private String volumeName;
        private String path;

        private ApplyVolumeRequestBuilder() {
            this.volumeName = "";
            this.path = "";
        }

        /**
         * Sets the name of the volume to apply for.
         *
         * @param volumeName the volume name
         * @return this builder
         */
        public ApplyVolumeRequestBuilder volumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        /**
         * Sets the path within the volume where files are uploaded.
         *
         * @param path the upload path
         * @return this builder
         */
        public ApplyVolumeRequestBuilder path(String path) {
            this.path = path;
            return this;
        }

        /**
         * Builds the {@link ApplyVolumeRequest} instance.
         *
         * @return the built {@code ApplyVolumeRequest}
         */
        public ApplyVolumeRequest build() {
            return new ApplyVolumeRequest(this);
        }
    }
}
