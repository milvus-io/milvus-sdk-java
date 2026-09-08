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

import io.milvus.exception.ParamException;
import io.milvus.param.role.RevokeRolePrivilegeParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class RevokeRolePrivilegeParamTest {
    @Test
    void buildAndGet() {
        RevokeRolePrivilegeParam param = RevokeRolePrivilegeParam.newBuilder()
                .withDatabaseName("db1")
                .withRoleName("role1")
                .withObject("Collection")
                .withObjectName("coll1")
                .withPrivilege("Insert")
                .build();

        assertEquals("db1", param.getDatabaseName());
        assertEquals("role1", param.getRoleName());
        assertEquals("Collection", param.getObject());
        assertEquals("coll1", param.getObjectName());
        assertEquals("Insert", param.getPrivilege());
    }

    @Test
    void databaseNameIsOptional() {
        RevokeRolePrivilegeParam param = RevokeRolePrivilegeParam.newBuilder()
                .withRoleName("role1")
                .withObject("Collection")
                .withObjectName("coll1")
                .withPrivilege("Insert")
                .build();

        assertNull(param.getDatabaseName());
    }

    @Test
    void buildFailsWhenRequiredFieldMissing() {
        assertThrows(ParamException.class, () -> RevokeRolePrivilegeParam.newBuilder()
                .withObject("Collection").withObjectName("coll1").withPrivilege("Insert").build());
        assertThrows(ParamException.class, () -> RevokeRolePrivilegeParam.newBuilder()
                .withRoleName("role1").withObjectName("coll1").withPrivilege("Insert").build());
        assertThrows(ParamException.class, () -> RevokeRolePrivilegeParam.newBuilder()
                .withRoleName("role1").withObject("Collection").withPrivilege("Insert").build());
        assertThrows(ParamException.class, () -> RevokeRolePrivilegeParam.newBuilder()
                .withRoleName("role1").withObject("Collection").withObjectName("coll1").build());
    }

    @Test
    void withMethodsRejectNullOrEmpty() {
        assertThrows(IllegalArgumentException.class, () ->
                RevokeRolePrivilegeParam.newBuilder().withRoleName(null));
        assertThrows(IllegalArgumentException.class, () ->
                RevokeRolePrivilegeParam.newBuilder().withRoleName(""));
        assertThrows(IllegalArgumentException.class, () ->
                RevokeRolePrivilegeParam.newBuilder().withObject(null));
        assertThrows(IllegalArgumentException.class, () ->
                RevokeRolePrivilegeParam.newBuilder().withObject(""));
        assertThrows(IllegalArgumentException.class, () ->
                RevokeRolePrivilegeParam.newBuilder().withObjectName(null));
        assertThrows(IllegalArgumentException.class, () ->
                RevokeRolePrivilegeParam.newBuilder().withObjectName(""));
        assertThrows(IllegalArgumentException.class, () ->
                RevokeRolePrivilegeParam.newBuilder().withPrivilege(null));
        assertThrows(IllegalArgumentException.class, () ->
                RevokeRolePrivilegeParam.newBuilder().withPrivilege(""));
    }
}
