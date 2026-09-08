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

import io.milvus.v2.service.rbac.request.CreateRoleReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class CreateRoleReqTest {
    @Test
    void builderBuildsAllFields() {
        CreateRoleReq request = CreateRoleReq.builder()
                .roleName("readonly")
                .description("read only role")
                .build();

        assertEquals("readonly", request.getRoleName());
        assertEquals("read only role", request.getDescription());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        CreateRoleReq request = CreateRoleReq.builder().build();

        assertNull(request.getRoleName());
        assertEquals("", request.getDescription());
    }

    @Test
    void settersUpdateFields() {
        CreateRoleReq request = CreateRoleReq.builder().build();

        request.setRoleName("admin");
        request.setDescription("administrator");

        assertEquals("admin", request.getRoleName());
        assertEquals("administrator", request.getDescription());
    }

    @Test
    void toStringContainsFields() {
        CreateRoleReq request = CreateRoleReq.builder()
                .roleName("readonly")
                .description("desc")
                .build();

        assertEquals("CreateRoleReq{roleName='readonly', description='desc'}", request.toString());
    }
}
