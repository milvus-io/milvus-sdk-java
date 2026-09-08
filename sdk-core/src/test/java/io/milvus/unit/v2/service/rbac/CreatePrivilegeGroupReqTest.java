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

import io.milvus.v2.service.rbac.request.CreatePrivilegeGroupReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class CreatePrivilegeGroupReqTest {
    @Test
    void builderBuildsAllFields() {
        CreatePrivilegeGroupReq request = CreatePrivilegeGroupReq.builder()
                .groupName("readonly")
                .build();

        assertEquals("readonly", request.getGroupName());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        CreatePrivilegeGroupReq request = CreatePrivilegeGroupReq.builder().build();

        assertNull(request.getGroupName());
    }

    @Test
    void setterUpdatesField() {
        CreatePrivilegeGroupReq request = CreatePrivilegeGroupReq.builder().build();

        request.setGroupName("admin");

        assertEquals("admin", request.getGroupName());
    }

    @Test
    void toStringContainsFields() {
        CreatePrivilegeGroupReq request = CreatePrivilegeGroupReq.builder().groupName("readonly").build();

        assertEquals("CreatePrivilegeGroupReq{groupName='readonly'}", request.toString());
    }
}
