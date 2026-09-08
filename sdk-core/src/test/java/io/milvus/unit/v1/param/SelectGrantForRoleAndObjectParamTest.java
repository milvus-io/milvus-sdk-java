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
import io.milvus.param.role.SelectGrantForRoleAndObjectParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class SelectGrantForRoleAndObjectParamTest {
    @Test
    void buildAndGet() {
        SelectGrantForRoleAndObjectParam param = SelectGrantForRoleAndObjectParam.newBuilder()
                .withRoleName("role1")
                .withObject("Collection")
                .withObjectName("coll1")
                .build();

        assertEquals("role1", param.getRoleName());
        assertEquals("Collection", param.getObject());
        assertEquals("coll1", param.getObjectName());
    }

    @Test
    void buildFailsWhenRoleNameMissing() {
        assertThrows(ParamException.class, () -> SelectGrantForRoleAndObjectParam.newBuilder()
                .withObject("Collection")
                .withObjectName("coll1")
                .build());
    }

    @Test
    void buildFailsWhenObjectMissing() {
        assertThrows(ParamException.class, () -> SelectGrantForRoleAndObjectParam.newBuilder()
                .withRoleName("role1")
                .withObjectName("coll1")
                .build());
    }

    @Test
    void buildFailsWhenObjectNameMissing() {
        assertThrows(ParamException.class, () -> SelectGrantForRoleAndObjectParam.newBuilder()
                .withRoleName("role1")
                .withObject("Collection")
                .build());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                SelectGrantForRoleAndObjectParam.newBuilder().withRoleName(null));
        assertThrows(IllegalArgumentException.class, () ->
                SelectGrantForRoleAndObjectParam.newBuilder().withObject(null));
        assertThrows(IllegalArgumentException.class, () ->
                SelectGrantForRoleAndObjectParam.newBuilder().withObjectName(null));
    }
}
