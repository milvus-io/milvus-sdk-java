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

public class RemoveFileResourceReq {
    private final String name;

    private RemoveFileResourceReq(RemoveFileResourceReqBuilder builder) {
        this.name = builder.name;
    }

    public static RemoveFileResourceReqBuilder builder() {
        return new RemoveFileResourceReqBuilder();
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "RemoveFileResourceReq{" +
                "name='" + name + '\'' +
                '}';
    }

    public static class RemoveFileResourceReqBuilder {
        private String name;

        public RemoveFileResourceReqBuilder name(String name) {
            this.name = name;
            return this;
        }

        public RemoveFileResourceReq build() {
            return new RemoveFileResourceReq(this);
        }
    }
}
