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

package io.milvus.v2.service.rbac;

import io.milvus.v2.BaseTest;
import io.milvus.v2.service.rbac.request.CreateUserReq;
import io.milvus.v2.service.rbac.request.DescribeUserReq;
import io.milvus.v2.service.rbac.request.DropUserReq;
import io.milvus.v2.service.rbac.request.UpdatePasswordReq;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class UserTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(UserTest.class);

    @Test
    void listUsers() {
        List<String> resp = client_v2.listUsers();
        logger.info("resp: {}", resp);
    }

    @Test
    void testDescribeUser() {
        DescribeUserReq req = DescribeUserReq.builder()
                .userName("test")
                .build();
        DescribeUserResp resp = client_v2.describeUser(req);
        logger.info("resp: {}", resp);
    }

    @Test
    void testCreateUser() {
        CreateUserReq req = CreateUserReq.builder()
                .userName("test")
                .password("Zilliz@2023")
                .build();
        client_v2.createUser(req);

    }

    @Test
    void testUpdatePassword() {
        UpdatePasswordReq req = UpdatePasswordReq.builder()
                .userName("test")
                .password("Zilliz@2023")
                .newPassword("Zilliz@2024")
                .build();
        client_v2.updatePassword(req);
    }

    @Test
    void testDropUser() {
        DropUserReq req = DropUserReq.builder()
                .userName("test")
                .build();
        client_v2.dropUser(req);

    }
}