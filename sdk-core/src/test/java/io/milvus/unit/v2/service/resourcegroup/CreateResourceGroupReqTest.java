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
import io.milvus.v2.service.resourcegroup.request.CreateResourceGroupReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class CreateResourceGroupReqTest {
    private ResourceGroupConfig newConfig() {
        return ResourceGroupConfig.newBuilder()
                .withRequests(new ResourceGroupLimit(1))
                .withLimits(new ResourceGroupLimit(2))
                .build();
    }

    @Test
    void builderBuildsAllFields() {
        ResourceGroupConfig config = newConfig();

        CreateResourceGroupReq request = CreateResourceGroupReq.builder()
                .groupName("rg")
                .config(config)
                .build();

        assertEquals("rg", request.getGroupName());
        assertEquals(config, request.getConfig());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        CreateResourceGroupReq request = CreateResourceGroupReq.builder().build();

        assertNull(request.getGroupName());
        assertNull(request.getConfig());
    }

    @Test
    void settersUpdateFields() {
        CreateResourceGroupReq request = CreateResourceGroupReq.builder().build();
        ResourceGroupConfig config = newConfig();

        request.setGroupName("rg");
        request.setConfig(config);

        assertEquals("rg", request.getGroupName());
        assertEquals(config, request.getConfig());
    }

    @Test
    void toStringContainsFields() {
        CreateResourceGroupReq request = CreateResourceGroupReq.builder()
                .groupName("rg")
                .config(newConfig())
                .build();

        assertEquals("CreateResourceGroupReq{groupName='rg', config=" + request.getConfig() + "}",
                request.toString());
    }
}
