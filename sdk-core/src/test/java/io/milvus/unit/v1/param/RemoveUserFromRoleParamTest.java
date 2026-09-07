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
import io.milvus.param.role.RemoveUserFromRoleParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class RemoveUserFromRoleParamTest {
    @Test
    void buildAndGet() {
        RemoveUserFromRoleParam param = RemoveUserFromRoleParam.newBuilder()
                .withUserName("user1")
                .withRoleName("role1")
                .build();

        assertEquals("user1", param.getUserName());
        assertEquals("role1", param.getRoleName());
    }

    @Test
    void buildFailsWhenUserNameMissing() {
        assertThrows(ParamException.class, () -> RemoveUserFromRoleParam.newBuilder()
                .withRoleName("role1")
                .build());
    }

    @Test
    void buildFailsWhenRoleNameMissing() {
        assertThrows(ParamException.class, () -> RemoveUserFromRoleParam.newBuilder()
                .withUserName("user1")
                .build());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                RemoveUserFromRoleParam.newBuilder().withUserName(null));
        assertThrows(IllegalArgumentException.class, () ->
                RemoveUserFromRoleParam.newBuilder().withRoleName(null));
    }
}
