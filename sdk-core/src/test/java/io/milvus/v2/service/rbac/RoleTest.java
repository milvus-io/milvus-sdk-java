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

package io.milvus.v2.service.rbac;

import io.milvus.v2.BaseTest;
import io.milvus.v2.service.rbac.request.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class RoleTest extends BaseTest {

    Logger logger = LoggerFactory.getLogger(RoleTest.class);

    @Test
    void testListRoles() {
        List<String> roles = client_v2.listRoles();
    }

    @Test
    void testCreateRole() {
        CreateRoleReq request = CreateRoleReq.builder()
                .roleName("test")
                .build();
        client_v2.createRole(request);
    }

    @Test
    void testDescribeRole() {
        DescribeRoleReq describeRoleReq = DescribeRoleReq.builder()
                .roleName("db_rw")
                .build();
        client_v2.describeRole(describeRoleReq);
    }

    @Test
    void testDropRole() {
        DropRoleReq request = DropRoleReq.builder()
                .roleName("test")
                .build();
        client_v2.dropRole(request);
    }

    @Test
    void testGrantPrivilege() {
        GrantPrivilegeReq request = GrantPrivilegeReq.builder()
                .roleName("db_rw")
                .objectName("")
                .objectType("")
                .privilege("")
                .build();
        client_v2.grantPrivilege(request);
    }

    @Test
    void testRevokePrivilege() {
        RevokePrivilegeReq request = RevokePrivilegeReq.builder()
                .roleName("db_rw")
                .objectName("")
                .objectType("")
                .privilege("")
                .build();
        client_v2.revokePrivilege(request);
    }

    @Test
    void testGrantRole() {
        GrantRoleReq request = GrantRoleReq.builder()
                .roleName("db_ro")
                .userName("test")
                .build();
        client_v2.grantRole(request);
    }

    @Test
    void testRevokeRole() {
        RevokeRoleReq request = RevokeRoleReq.builder()
                .roleName("db_ro")
                .userName("test")
                .build();
        client_v2.revokeRole(request);
    }
}