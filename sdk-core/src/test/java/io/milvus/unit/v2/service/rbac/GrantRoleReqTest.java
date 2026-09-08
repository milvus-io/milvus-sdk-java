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

import io.milvus.v2.service.rbac.request.GrantRoleReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class GrantRoleReqTest {
    @Test
    void builderBuildsAllFields() {
        GrantRoleReq request = GrantRoleReq.builder()
                .userName("alice")
                .roleName("readonly")
                .build();

        assertEquals("alice", request.getUserName());
        assertEquals("readonly", request.getRoleName());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        GrantRoleReq request = GrantRoleReq.builder().build();

        assertNull(request.getUserName());
        assertNull(request.getRoleName());
    }

    @Test
    void settersUpdateFields() {
        GrantRoleReq request = GrantRoleReq.builder().build();

        request.setUserName("bob");
        request.setRoleName("admin");

        assertEquals("bob", request.getUserName());
        assertEquals("admin", request.getRoleName());
    }

    @Test
    void toStringContainsFields() {
        GrantRoleReq request = GrantRoleReq.builder()
                .userName("alice")
                .roleName("readonly")
                .build();

        assertEquals("GrantRoleReq{userName='alice', roleName='readonly'}", request.toString());
    }
}
