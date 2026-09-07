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

package io.milvus.unit.v2.service.rbac;

import io.milvus.v2.service.rbac.PrivilegeGroup;
import io.milvus.v2.service.rbac.response.ListPrivilegeGroupsResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ListPrivilegeGroupsRespTest {
    @Test
    void builderBuildsAllFields() {
        PrivilegeGroup group = PrivilegeGroup.builder()
                .groupName("readonly")
                .privileges(Collections.singletonList("Search"))
                .build();
        List<PrivilegeGroup> groups = Collections.singletonList(group);

        ListPrivilegeGroupsResp response = ListPrivilegeGroupsResp.builder()
                .privilegeGroups(groups)
                .build();

        assertEquals(groups, response.getPrivilegeGroups());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        ListPrivilegeGroupsResp response = ListPrivilegeGroupsResp.builder().build();

        assertTrue(response.getPrivilegeGroups().isEmpty());
    }

    @Test
    void setterUpdatesField() {
        ListPrivilegeGroupsResp response = ListPrivilegeGroupsResp.builder().build();

        response.setPrivilegeGroups(Collections.emptyList());

        assertTrue(response.getPrivilegeGroups().isEmpty());
    }

    @Test
    void toStringContainsFields() {
        ListPrivilegeGroupsResp response = ListPrivilegeGroupsResp.builder()
                .privilegeGroups(Collections.singletonList(
                        PrivilegeGroup.builder().groupName("readonly").build()))
                .build();

        assertEquals("ListPrivilegeGroupsResp{privilegeGroups=[PrivilegeGroup{groupName='readonly', privileges=[]}]}",
                response.toString());
    }
}
