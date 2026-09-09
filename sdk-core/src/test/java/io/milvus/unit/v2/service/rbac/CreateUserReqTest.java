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

import io.milvus.v2.service.rbac.request.CreateUserReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class CreateUserReqTest {
    @Test
    void builderBuildsAllFields() {
        CreateUserReq request = CreateUserReq.builder()
                .userName("alice")
                .password("secret")
                .description("account holder")
                .build();

        assertEquals("alice", request.getUserName());
        assertEquals("secret", request.getPassword());
        assertEquals("account holder", request.getDescription());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        CreateUserReq request = CreateUserReq.builder().build();

        assertNull(request.getUserName());
        assertNull(request.getPassword());
        assertEquals("", request.getDescription());
    }

    @Test
    void settersUpdateFields() {
        CreateUserReq request = CreateUserReq.builder().build();

        request.setUserName("bob");
        request.setPassword("newpass");
        request.setDescription("another user");

        assertEquals("bob", request.getUserName());
        assertEquals("newpass", request.getPassword());
        assertEquals("another user", request.getDescription());
    }

    @Test
    void toStringRedactsPassword() {
        CreateUserReq request = CreateUserReq.builder()
                .userName("alice")
                .password("secret")
                .description("desc")
                .build();

        String text = request.toString();
        assertTrue(text.startsWith("CreateUserReq{userName='alice'"));
        assertFalse(text.contains("secret"));
        assertTrue(text.contains("<redacted>"));
    }

    @Test
    void toStringRedactsNullPassword() {
        CreateUserReq request = CreateUserReq.builder().userName("alice").build();

        assertEquals("CreateUserReq{userName='alice', password='null', description=''}",
                request.toString());
    }
}
