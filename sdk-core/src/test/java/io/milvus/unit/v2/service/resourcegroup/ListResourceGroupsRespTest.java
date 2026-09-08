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

import io.milvus.v2.service.resourcegroup.response.ListResourceGroupsResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ListResourceGroupsRespTest {
    @Test
    void builderBuildsAllFields() {
        List<String> groupNames = Arrays.asList("__default_resource_group", "rg");

        ListResourceGroupsResp response = ListResourceGroupsResp.builder()
                .groupNames(groupNames)
                .build();

        assertEquals(groupNames, response.getGroupNames());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        ListResourceGroupsResp response = ListResourceGroupsResp.builder().build();

        assertTrue(response.getGroupNames().isEmpty());
    }

    @Test
    void setterUpdatesField() {
        ListResourceGroupsResp response = ListResourceGroupsResp.builder().build();

        response.setGroupNames(Arrays.asList("rg"));

        assertEquals(Arrays.asList("rg"), response.getGroupNames());
    }

    @Test
    void toStringContainsFields() {
        ListResourceGroupsResp response = ListResourceGroupsResp.builder()
                .groupNames(Arrays.asList("rg"))
                .build();

        assertEquals("ListResourceGroupsResp{groupNames=[rg]}", response.toString());
    }
}
