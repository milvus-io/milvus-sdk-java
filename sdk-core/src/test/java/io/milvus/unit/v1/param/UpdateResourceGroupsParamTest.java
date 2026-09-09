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

package io.milvus.unit.v1.param;

import io.milvus.common.resourcegroup.ResourceGroupConfig;
import io.milvus.common.resourcegroup.ResourceGroupLimit;
import io.milvus.param.resourcegroup.UpdateResourceGroupsParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class UpdateResourceGroupsParamTest {
    @Test
    void buildAndGet() {
        ResourceGroupConfig config = ResourceGroupConfig.newBuilder().build();

        UpdateResourceGroupsParam param = UpdateResourceGroupsParam.newBuilder()
                .putResourceGroup("rg1", config)
                .build();

        Map<String, ResourceGroupConfig> resourceGroups = param.getResourceGroups();
        assertEquals(1, resourceGroups.size());
        assertEquals(config, resourceGroups.get("rg1"));
    }

    @Test
    void buildFailsWhenResourceGroupsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> UpdateResourceGroupsParam.newBuilder().build());
    }

    @Test
    void putResourceGroupRejectsNull() {
        assertThrows(IllegalArgumentException.class, () ->
                UpdateResourceGroupsParam.newBuilder().putResourceGroup(null, ResourceGroupConfig.newBuilder().build()));
        assertThrows(IllegalArgumentException.class, () ->
                UpdateResourceGroupsParam.newBuilder().putResourceGroup("rg1", null));
    }

    @Test
    void toGRPC() {
        ResourceGroupConfig config = ResourceGroupConfig.newBuilder()
                .withRequests(new ResourceGroupLimit(2))
                .withLimits(new ResourceGroupLimit(4))
                .build();

        UpdateResourceGroupsParam param = UpdateResourceGroupsParam.newBuilder()
                .putResourceGroup("rg1", config)
                .build();

        assertNotNull(param.toGRPC());
        assertEquals(Collections.singleton("rg1"), param.toGRPC().getResourceGroupsMap().keySet());
        assertNotNull(param.toString());
    }
}
