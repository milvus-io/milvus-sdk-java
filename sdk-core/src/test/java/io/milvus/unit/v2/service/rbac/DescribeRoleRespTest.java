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

import io.milvus.v2.service.rbac.response.DescribeRoleResp;
import io.milvus.v2.service.rbac.response.DescribeRoleResp.GrantInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DescribeRoleRespTest {
    @Test
    void builderBuildsAllFields() {
        GrantInfo grantInfo = GrantInfo.builder()
                .objectType("Collection")
                .objectName("coll")
                .roleName("readonly")
                .grantor("root")
                .privilege("Search")
                .dbName("default")
                .build();

        DescribeRoleResp response = DescribeRoleResp.builder()
                .roleName("readonly")
                .grantInfos(Collections.singletonList(grantInfo))
                .description("desc")
                .build();

        assertEquals("readonly", response.getRoleName());
        assertEquals(Collections.singletonList(grantInfo), response.getGrantInfos());
        assertEquals("desc", response.getDescription());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        DescribeRoleResp response = DescribeRoleResp.builder().build();

        assertEquals("", response.getRoleName());
        assertTrue(response.getGrantInfos().isEmpty());
        assertEquals("", response.getDescription());
    }

    @Test
    void settersUpdateFields() {
        DescribeRoleResp response = DescribeRoleResp.builder().build();

        response.setRoleName("admin");
        response.setGrantInfos(Collections.emptyList());
        response.setDescription("administrator");

        assertEquals("admin", response.getRoleName());
        assertTrue(response.getGrantInfos().isEmpty());
        assertEquals("administrator", response.getDescription());
    }

    @Test
    void grantInfoBuilderBuildsAllFields() {
        GrantInfo grantInfo = GrantInfo.builder()
                .objectType("Global")
                .objectName("*")
                .roleName("admin")
                .grantor("root")
                .privilege("All")
                .dbName("*")
                .build();

        assertEquals("Global", grantInfo.getObjectType());
        assertEquals("*", grantInfo.getObjectName());
        assertEquals("admin", grantInfo.getRoleName());
        assertEquals("root", grantInfo.getGrantor());
        assertEquals("All", grantInfo.getPrivilege());
        assertEquals("*", grantInfo.getDbName());
    }

    @Test
    void grantInfoUnsetFieldsDefaultToNull() {
        GrantInfo grantInfo = GrantInfo.builder().build();

        assertNull(grantInfo.getObjectType());
        assertNull(grantInfo.getObjectName());
        assertNull(grantInfo.getRoleName());
        assertNull(grantInfo.getGrantor());
        assertNull(grantInfo.getPrivilege());
        assertNull(grantInfo.getDbName());
    }

    @Test
    void grantInfoSettersUpdateFields() {
        GrantInfo grantInfo = GrantInfo.builder().build();

        grantInfo.setObjectType("Collection");
        grantInfo.setObjectName("coll");
        grantInfo.setRoleName("role");
        grantInfo.setGrantor("user");
        grantInfo.setPrivilege("Search");
        grantInfo.setDbName("db");

        assertEquals("Collection", grantInfo.getObjectType());
        assertEquals("coll", grantInfo.getObjectName());
        assertEquals("role", grantInfo.getRoleName());
        assertEquals("user", grantInfo.getGrantor());
        assertEquals("Search", grantInfo.getPrivilege());
        assertEquals("db", grantInfo.getDbName());
    }

    @Test
    void grantInfoToStringContainsFields() {
        GrantInfo grantInfo = GrantInfo.builder()
                .objectType("Collection")
                .objectName("coll")
                .roleName("role")
                .grantor("user")
                .privilege("Search")
                .dbName("db")
                .build();

        assertEquals("GrantInfo{objectType='Collection', objectName='coll', roleName='role', grantor='user', privilege='Search', dbName='db'}",
                grantInfo.toString());
    }

    @Test
    void responseToStringContainsFields() {
        List<GrantInfo> grantInfos = Arrays.asList(GrantInfo.builder().privilege("Search").build());
        DescribeRoleResp response = DescribeRoleResp.builder()
                .roleName("readonly")
                .grantInfos(grantInfos)
                .description("desc")
                .build();

        assertEquals("DescribeRoleResp{roleName='readonly', grantInfos=" + grantInfos + ", description='desc'}",
                response.toString());
    }
}
