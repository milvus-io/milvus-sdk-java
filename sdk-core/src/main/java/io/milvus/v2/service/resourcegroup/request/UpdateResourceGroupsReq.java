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

package io.milvus.v2.service.resourcegroup.request;

import io.milvus.common.resourcegroup.ResourceGroupConfig;

import java.util.HashMap;
import java.util.Map;

public class UpdateResourceGroupsReq {
    private Map<String, ResourceGroupConfig> resourceGroups;

    private UpdateResourceGroupsReq(UpdateResourceGroupsReqBuilder builder) {
        this.resourceGroups = builder.resourceGroups;
    }

    public static UpdateResourceGroupsReqBuilder builder() {
        return new UpdateResourceGroupsReqBuilder();
    }

    public Map<String, ResourceGroupConfig> getResourceGroups() {
        return resourceGroups;
    }

    public void setResourceGroups(Map<String, ResourceGroupConfig> resourceGroups) {
        this.resourceGroups = resourceGroups;
    }

    @Override
    public String toString() {
        return "UpdateResourceGroupsReq{" +
                "resourceGroups=" + resourceGroups +
                '}';
    }

    public static class UpdateResourceGroupsReqBuilder {
        private Map<String, ResourceGroupConfig> resourceGroups = new HashMap<>();

        public UpdateResourceGroupsReqBuilder resourceGroups(Map<String, ResourceGroupConfig> resourceGroups) {
            this.resourceGroups = resourceGroups;
            return this;
        }

        public UpdateResourceGroupsReq build() {
            return new UpdateResourceGroupsReq(this);
        }
    }
}
