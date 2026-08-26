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

package io.milvus.v2.service.utility.response;

/**
 * A file resource that can be attached to a collection, as returned by the {@code listFileResources}
 * API.
 */
public class FileResourceInfo {
    private final String name;
    private final String path;

    private FileResourceInfo(FileResourceInfoBuilder builder) {
        this.name = builder.name;
        this.path = builder.path;
    }

    /**
     * Creates a new {@code FileResourceInfo} builder.
     *
     * @return the builder
     */
    public static FileResourceInfoBuilder builder() {
        return new FileResourceInfoBuilder();
    }

    /**
     * Returns the name of the file resource.
     *
     * @return the file resource name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the path of the file resource.
     *
     * @return the file resource path
     */
    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return "FileResourceInfo{" +
                "name='" + name + '\'' +
                ", path='" + path + '\'' +
                '}';
    }

    /**
     * Builder for {@link FileResourceInfo}.
     */
    public static class FileResourceInfoBuilder {
        private String name;
        private String path;

        /**
         * Sets the name of the file resource.
         *
         * @param name the file resource name
         * @return this builder
         */
        public FileResourceInfoBuilder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the path of the file resource.
         *
         * @param path the file resource path
         * @return this builder
         */
        public FileResourceInfoBuilder path(String path) {
            this.path = path;
            return this;
        }

        /**
         * Builds the {@link FileResourceInfo}.
         *
         * @return the file resource information
         */
        public FileResourceInfo build() {
            return new FileResourceInfo(this);
        }
    }
}
