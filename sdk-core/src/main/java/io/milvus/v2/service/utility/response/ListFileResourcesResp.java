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

import java.util.List;

public class ListFileResourcesResp {
    private final List<FileResourceInfo> resources;

    private ListFileResourcesResp(ListFileResourcesRespBuilder builder) {
        this.resources = builder.resources;
    }

    public static ListFileResourcesRespBuilder builder() {
        return new ListFileResourcesRespBuilder();
    }

    public List<FileResourceInfo> getResources() {
        return resources;
    }

    @Override
    public String toString() {
        return "ListFileResourcesResp{" +
                "resources=" + resources +
                '}';
    }

    public static class ListFileResourcesRespBuilder {
        private List<FileResourceInfo> resources;

        public ListFileResourcesRespBuilder resources(List<FileResourceInfo> resources) {
            this.resources = resources;
            return this;
        }

        public ListFileResourcesResp build() {
            return new ListFileResourcesResp(this);
        }
    }
}
