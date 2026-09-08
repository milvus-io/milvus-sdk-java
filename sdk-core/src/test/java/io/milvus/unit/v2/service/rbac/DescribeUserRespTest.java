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

import io.milvus.v2.service.rbac.response.DescribeUserResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DescribeUserRespTest {
    @Test
    void builderBuildsAllFields() {
        List<String> roles = Arrays.asList("readonly", "admin");

        DescribeUserResp response = DescribeUserResp.builder()
                .userName("alice")
                .roles(roles)
                .description("desc")
                .build();

        assertEquals("alice", response.getUserName());
        assertEquals(roles, response.getRoles());
        assertEquals("desc", response.getDescription());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        DescribeUserResp response = DescribeUserResp.builder().build();

        assertEquals("", response.getUserName());
        assertTrue(response.getRoles().isEmpty());
        assertEquals("", response.getDescription());
    }

    @Test
    void settersUpdateFields() {
        DescribeUserResp response = DescribeUserResp.builder().build();

        response.setUserName("bob");
        response.setRoles(Arrays.asList("admin"));
        response.setDescription("another user");

        assertEquals("bob", response.getUserName());
        assertEquals(Arrays.asList("admin"), response.getRoles());
        assertEquals("another user", response.getDescription());
    }

    @Test
    void toStringContainsFields() {
        DescribeUserResp response = DescribeUserResp.builder()
                .userName("alice")
                .roles(Arrays.asList("admin"))
                .description("desc")
                .build();

        assertEquals("DescribeUserResp{userName='alice', roles=[admin], description='desc'}",
                response.toString());
    }
}
