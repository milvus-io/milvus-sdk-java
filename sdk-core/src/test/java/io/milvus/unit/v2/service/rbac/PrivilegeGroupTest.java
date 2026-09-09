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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class PrivilegeGroupTest {
    @Test
    void builderBuildsAllFields() {
        List<String> privileges = Arrays.asList("Search", "Query");

        PrivilegeGroup group = PrivilegeGroup.builder()
                .groupName("readonly")
                .privileges(privileges)
                .build();

        assertEquals("readonly", group.getGroupName());
        assertEquals(privileges, group.getPrivileges());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        PrivilegeGroup group = PrivilegeGroup.builder().build();

        assertNull(group.getGroupName());
        assertTrue(group.getPrivileges().isEmpty());
    }

    @Test
    void settersUpdateFields() {
        PrivilegeGroup group = PrivilegeGroup.builder().build();

        group.setGroupName("admin");
        group.setPrivileges(Arrays.asList("CreateCollection"));

        assertEquals("admin", group.getGroupName());
        assertEquals(Arrays.asList("CreateCollection"), group.getPrivileges());
    }

    @Test
    void toStringContainsFields() {
        PrivilegeGroup group = PrivilegeGroup.builder()
                .groupName("readonly")
                .privileges(Arrays.asList("Search"))
                .build();

        assertEquals("PrivilegeGroup{groupName='readonly', privileges=[Search]}", group.toString());
    }
}
