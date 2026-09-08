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

import io.milvus.v2.service.rbac.request.DropRoleReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DropRoleReqTest {
    @Test
    void builderBuildsAllFields() {
        DropRoleReq request = DropRoleReq.builder()
                .roleName("readonly")
                .forceDrop(true)
                .build();

        assertEquals("readonly", request.getRoleName());
        assertTrue(request.isForceDrop());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        DropRoleReq request = DropRoleReq.builder().build();

        assertNull(request.getRoleName());
        assertFalse(request.isForceDrop());
    }

    @Test
    void settersUpdateFields() {
        DropRoleReq request = DropRoleReq.builder().build();

        request.setRoleName("admin");
        request.setForceDrop(true);

        assertEquals("admin", request.getRoleName());
        assertTrue(request.isForceDrop());
    }

    @Test
    void forceDropToBooleanToggles() {
        DropRoleReq request = DropRoleReq.builder().forceDrop(true).build();
        assertTrue(request.isForceDrop());

        request.setForceDrop(false);
        assertFalse(request.isForceDrop());
    }

    @Test
    void toStringContainsFields() {
        DropRoleReq request = DropRoleReq.builder()
                .roleName("readonly")
                .forceDrop(true)
                .build();

        assertEquals("DropRoleReq{roleName='readonly', forceDrop=true}", request.toString());
    }
}
