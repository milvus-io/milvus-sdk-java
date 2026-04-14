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

    public String getName() {
        return name;
    }

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

        public AddFileResourceReqBuilder name(String name) {
            this.name = name;
            return this;
        }

        public AddFileResourceReqBuilder path(String path) {
            this.path = path;
            return this;
        }

        public AddFileResourceReq build() {
            return new AddFileResourceReq(this);
        }
    }
}
