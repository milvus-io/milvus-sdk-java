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

package io.milvus.unit.v2.service.resourcegroup;

import io.milvus.common.resourcegroup.ResourceGroupConfig;
import io.milvus.common.resourcegroup.ResourceGroupLimit;
import io.milvus.v2.service.resourcegroup.request.UpdateResourceGroupsReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class UpdateResourceGroupsReqTest {
    @Test
    void builderBuildsAllFields() {
        ResourceGroupConfig config = ResourceGroupConfig.newBuilder()
                .withRequests(new ResourceGroupLimit(1))
                .withLimits(new ResourceGroupLimit(2))
                .build();
        Map<String, ResourceGroupConfig> resourceGroups = new HashMap<>();
        resourceGroups.put("rg", config);

        UpdateResourceGroupsReq request = UpdateResourceGroupsReq.builder()
                .resourceGroups(resourceGroups)
                .build();

        assertEquals(resourceGroups, request.getResourceGroups());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        UpdateResourceGroupsReq request = UpdateResourceGroupsReq.builder().build();

        assertTrue(request.getResourceGroups().isEmpty());
    }

    @Test
    void setterUpdatesField() {
        UpdateResourceGroupsReq request = UpdateResourceGroupsReq.builder().build();

        request.setResourceGroups(new HashMap<>());

        assertTrue(request.getResourceGroups().isEmpty());
    }

    @Test
    void toStringContainsFields() {
        UpdateResourceGroupsReq request = UpdateResourceGroupsReq.builder()
                .resourceGroups(new HashMap<>())
                .build();

        assertEquals("UpdateResourceGroupsReq{resourceGroups={}}", request.toString());
    }
}
