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
import io.milvus.param.role.SelectGrantForRoleParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class SelectGrantForRoleParamTest {
    @Test
    void buildAndGet() {
        SelectGrantForRoleParam param = SelectGrantForRoleParam.newBuilder()
                .withDatabaseName("db1")
                .withRoleName("role1")
                .build();

        assertEquals("db1", param.getDatabaseName());
        assertEquals("role1", param.getRoleName());
    }

    @Test
    void databaseNameIsOptional() {
        SelectGrantForRoleParam param = SelectGrantForRoleParam.newBuilder()
                .withRoleName("role1")
                .build();

        assertNull(param.getDatabaseName());
    }

    @Test
    void buildFailsWhenRoleNameMissing() {
        assertThrows(ParamException.class, () -> SelectGrantForRoleParam.newBuilder()
                .withDatabaseName("db1")
                .build());
    }

    @Test
    void withRoleNameRejectsNull() {
        assertThrows(IllegalArgumentException.class, () ->
                SelectGrantForRoleParam.newBuilder().withRoleName(null));
    }
}
