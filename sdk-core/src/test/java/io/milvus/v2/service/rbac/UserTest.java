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

import io.milvus.grpc.CreateCredentialRequest;
import io.milvus.grpc.SelectUserRequest;
import io.milvus.grpc.SelectUserResponse;
import io.milvus.grpc.Status;
import io.milvus.grpc.UpdateCredentialRequest;
import io.milvus.v2.BaseTest;
import io.milvus.v2.service.rbac.request.CreateUserReq;
import io.milvus.v2.service.rbac.request.DescribeUserReq;
import io.milvus.v2.service.rbac.request.DropUserReq;
import io.milvus.v2.service.rbac.request.UpdatePasswordReq;
import io.milvus.v2.service.rbac.request.UpdateUserReq;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        assertEquals("user_test", resp.getUserName());
        assertEquals("user description", resp.getDescription());

        ArgumentCaptor<SelectUserRequest> captor = ArgumentCaptor.forClass(SelectUserRequest.class);
        verify(blockingStub).selectUser(captor.capture());
        assertEquals("test", captor.getValue().getUser().getName());
    }

    @Test
    void testDescribeUserNotFound() {
        Status successStatus = Status.newBuilder().setCode(0).build();
        when(blockingStub.selectUser(any())).thenReturn(SelectUserResponse.newBuilder().setStatus(successStatus).build());

        DescribeUserReq req = DescribeUserReq.builder()
                .userName("missing")
                .build();
        DescribeUserResp resp = client_v2.describeUser(req);
        assertEquals("missing", resp.getUserName());
        assertNotNull(resp.getRoles());
        assertTrue(resp.getRoles().isEmpty());
        assertEquals("", resp.getDescription());
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
    void testCreateUserWithDescription() {
        CreateUserReq req = CreateUserReq.builder()
                .userName("test")
                .password("Zilliz@2023")
                .description("a user for testing")
                .build();
        client_v2.createUser(req);

        ArgumentCaptor<CreateCredentialRequest> captor = ArgumentCaptor.forClass(CreateCredentialRequest.class);
        verify(blockingStub).createCredential(captor.capture());
        assertEquals("test", captor.getValue().getUsername());
        assertEquals("a user for testing", captor.getValue().getDescription());
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
    void testUpdatePasswordWithDescription() {
        UpdatePasswordReq req = UpdatePasswordReq.builder()
                .userName("test")
                .password("Zilliz@2023")
                .newPassword("Zilliz@2024")
                .description("updated description")
                .build();
        client_v2.updatePassword(req);

        ArgumentCaptor<UpdateCredentialRequest> captor = ArgumentCaptor.forClass(UpdateCredentialRequest.class);
        verify(blockingStub).updateCredential(captor.capture());
        assertEquals("test", captor.getValue().getUsername());
        assertEquals("updated description", captor.getValue().getDescription());
    }

    @Test
    void testUpdateUser() {
        UpdateUserReq req = UpdateUserReq.builder()
                .userName("test")
                .description("description only update")
                .build();
        client_v2.updateUser(req);

        ArgumentCaptor<UpdateCredentialRequest> captor = ArgumentCaptor.forClass(UpdateCredentialRequest.class);
        verify(blockingStub).updateCredential(captor.capture());
        assertEquals("test", captor.getValue().getUsername());
        assertEquals("description only update", captor.getValue().getDescription());
        assertEquals("", captor.getValue().getOldPassword());
        assertEquals("", captor.getValue().getNewPassword());
    }

    @Test
    void testDropUser() {
        DropUserReq req = DropUserReq.builder()
                .userName("test")
                .build();
        client_v2.dropUser(req);

    }
}
