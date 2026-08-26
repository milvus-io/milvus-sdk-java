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

package io.milvus.v2.service.utility.request;

/**
 * Request parameters for the {@code addFileResource} API.
 */
public class AddFileResourceReq {
    private final String name;
    private final String path;

    private AddFileResourceReq(AddFileResourceReqBuilder builder) {
        this.name = builder.name;
        this.path = builder.path;
    }

    public static AddFileResourceReqBuilder builder() {
        return new AddFileResourceReqBuilder();
    }

    /**
     * Returns the name of the file resource.
     *
     * @return the name of the file resource
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the path of the file resource.
     *
     * @return the path of the file resource
     */
    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return "AddFileResourceReq{" +
                "name='" + name + '\'' +
                ", path='" + path + '\'' +
                '}';
    }

    public static class AddFileResourceReqBuilder {
        private String name;
        private String path;

        /**
         * Sets the name of the file resource.
         *
         * @param name the name of the file resource
         * @return this builder
         */
        public AddFileResourceReqBuilder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the path of the file resource.
         *
         * @param path the path of the file resource
         * @return this builder
         */
        public AddFileResourceReqBuilder path(String path) {
            this.path = path;
            return this;
        }

        /**
         * Builds the {@code AddFileResourceReq}.
         *
         * @return the constructed {@code AddFileResourceReq}
         */
        public AddFileResourceReq build() {
            return new AddFileResourceReq(this);
        }
    }
}
