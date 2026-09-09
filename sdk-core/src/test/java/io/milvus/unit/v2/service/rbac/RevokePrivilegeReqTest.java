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

import io.milvus.v2.service.rbac.request.RevokePrivilegeReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class RevokePrivilegeReqTest {
    @Test
    void builderBuildsAllFields() {
        RevokePrivilegeReq request = RevokePrivilegeReq.builder()
                .roleName("readonly")
                .dbName("default")
                .objectType("Collection")
                .privilege("Search")
                .objectName("coll")
                .build();

        assertEquals("readonly", request.getRoleName());
        assertEquals("default", request.getDbName());
        assertEquals("Collection", request.getObjectType());
        assertEquals("Search", request.getPrivilege());
        assertEquals("coll", request.getObjectName());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        RevokePrivilegeReq request = RevokePrivilegeReq.builder().build();

        assertNull(request.getRoleName());
        assertNull(request.getDbName());
        assertNull(request.getObjectType());
        assertNull(request.getPrivilege());
        assertNull(request.getObjectName());
    }

    @Test
    void settersUpdateFields() {
        RevokePrivilegeReq request = RevokePrivilegeReq.builder().build();

        request.setRoleName("role");
        request.setDbName("db");
        request.setObjectType("Global");
        request.setPrivilege("All");
        request.setObjectName("*");

        assertEquals("role", request.getRoleName());
        assertEquals("db", request.getDbName());
        assertEquals("Global", request.getObjectType());
        assertEquals("All", request.getPrivilege());
        assertEquals("*", request.getObjectName());
    }

    @Test
    void toStringContainsFields() {
        RevokePrivilegeReq request = RevokePrivilegeReq.builder()
                .roleName("role")
                .dbName("db")
                .objectType("Collection")
                .privilege("Search")
                .objectName("coll")
                .build();

        assertEquals("RevokePrivilegeReq{roleName='role', dbName='db', objectType='Collection', privilege='Search', objectName='coll'}",
                request.toString());
    }
}
