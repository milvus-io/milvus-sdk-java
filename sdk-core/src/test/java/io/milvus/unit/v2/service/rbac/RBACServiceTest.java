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

import io.milvus.grpc.CreateCredentialRequest;
import io.milvus.grpc.CreateRoleRequest;
import io.milvus.grpc.DeleteCredentialRequest;
import io.milvus.grpc.DropRoleRequest;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.GrantEntity;
import io.milvus.grpc.GrantorEntity;
import io.milvus.grpc.ListCredUsersRequest;
import io.milvus.grpc.ListCredUsersResponse;
import io.milvus.grpc.ListPrivilegeGroupsRequest;
import io.milvus.grpc.ListPrivilegeGroupsResponse;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.ObjectEntity;
import io.milvus.grpc.OperatePrivilegeGroupRequest;
import io.milvus.grpc.OperatePrivilegeGroupType;
import io.milvus.grpc.OperatePrivilegeRequest;
import io.milvus.grpc.OperatePrivilegeType;
import io.milvus.grpc.OperatePrivilegeV2Request;
import io.milvus.grpc.OperateUserRoleRequest;
import io.milvus.grpc.OperateUserRoleType;
import io.milvus.grpc.PrivilegeEntity;
import io.milvus.grpc.PrivilegeGroupInfo;
import io.milvus.grpc.RoleEntity;
import io.milvus.grpc.RoleResult;
import io.milvus.grpc.SelectGrantRequest;
import io.milvus.grpc.SelectGrantResponse;
import io.milvus.grpc.SelectRoleRequest;
import io.milvus.grpc.SelectRoleResponse;
import io.milvus.grpc.SelectUserRequest;
import io.milvus.grpc.SelectUserResponse;
import io.milvus.grpc.Status;
import io.milvus.grpc.UpdateCredentialRequest;
import io.milvus.grpc.UserEntity;
import io.milvus.grpc.UserResult;
import io.milvus.v2.service.rbac.RBACService;
import io.milvus.v2.service.rbac.request.AddPrivilegesToGroupReq;
import io.milvus.v2.service.rbac.request.AlterRoleReq;
import io.milvus.v2.service.rbac.request.CreatePrivilegeGroupReq;
import io.milvus.v2.service.rbac.request.CreateRoleReq;
import io.milvus.v2.service.rbac.request.CreateUserReq;
import io.milvus.v2.service.rbac.request.DescribeRoleReq;
import io.milvus.v2.service.rbac.request.DescribeUserReq;
import io.milvus.v2.service.rbac.request.DropPrivilegeGroupReq;
import io.milvus.v2.service.rbac.request.DropRoleReq;
import io.milvus.v2.service.rbac.request.DropUserReq;
import io.milvus.v2.service.rbac.request.GrantPrivilegeReq;
import io.milvus.v2.service.rbac.request.GrantPrivilegeReqV2;
import io.milvus.v2.service.rbac.request.GrantRoleReq;
import io.milvus.v2.service.rbac.request.ListPrivilegeGroupsReq;
import io.milvus.v2.service.rbac.request.RemovePrivilegesFromGroupReq;
import io.milvus.v2.service.rbac.request.RevokePrivilegeReq;
import io.milvus.v2.service.rbac.request.RevokePrivilegeReqV2;
import io.milvus.v2.service.rbac.request.RevokeRoleReq;
import io.milvus.v2.service.rbac.request.UpdatePasswordReq;
import io.milvus.v2.service.rbac.request.UpdateUserReq;
import io.milvus.v2.service.rbac.response.DescribeRoleResp;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import io.milvus.v2.service.rbac.response.ListPrivilegeGroupsResp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class RBACServiceTest {
    private MilvusServiceGrpc.MilvusServiceBlockingStub stub;
    private RBACService service;

    @BeforeEach
    void setUp() {
        stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        service = new RBACService();
    }

    private static Status success() {
        return Status.newBuilder().setCode(0).setErrorCode(ErrorCode.Success).build();
    }

    @Test
    void listRolesReturnsNames() {
        SelectRoleResponse response = SelectRoleResponse.newBuilder()
                .setStatus(success())
                .addResults(RoleResult.newBuilder()
                        .setRole(RoleEntity.newBuilder().setName("admin").build())
                        .build())
                .build();
        when(stub.selectRole(any())).thenReturn(response);

        assertEquals(Collections.singletonList("admin"), service.listRoles(stub));
        verify(stub).selectRole(any(SelectRoleRequest.class));
    }

    @Test
    void createRoleReturnsNull() {
        when(stub.createRole(any())).thenReturn(success());
        CreateRoleReq request = CreateRoleReq.builder().roleName("role_a").description("desc").build();

        assertNull(service.createRole(stub, request));

        ArgumentCaptor<CreateRoleRequest> captor = ArgumentCaptor.forClass(CreateRoleRequest.class);
        verify(stub).createRole(captor.capture());
        assertEquals("role_a", captor.getValue().getEntity().getName());
        assertEquals("desc", captor.getValue().getEntity().getDescription());
    }

    @Test
    void alterRoleReturnsNull() {
        when(stub.alterRole(any())).thenReturn(success());
        AlterRoleReq request = AlterRoleReq.builder().roleName("role_a").description("new desc").build();

        assertNull(service.alterRole(stub, request));
        verify(stub).alterRole(any(io.milvus.grpc.AlterRoleRequest.class));
    }

    @Test
    void describeRoleReturnsGrantsAndDescription() {
        SelectGrantResponse grantResponse = SelectGrantResponse.newBuilder()
                .setStatus(success())
                .addEntities(GrantEntity.newBuilder()
                        .setDbName("db")
                        .setObjectName("coll")
                        .setObject(ObjectEntity.newBuilder().setName("Collection").build())
                        .setGrantor(GrantorEntity.newBuilder()
                                .setPrivilege(PrivilegeEntity.newBuilder().setName("Query").build())
                                .setUser(UserEntity.newBuilder().setName("root").build())
                                .build())
                        .build())
                .build();
        SelectRoleResponse roleResponse = SelectRoleResponse.newBuilder()
                .setStatus(success())
                .addResults(RoleResult.newBuilder()
                        .setRole(RoleEntity.newBuilder().setName("role_a").setDescription("desc").build())
                        .build())
                .build();
        when(stub.selectGrant(any())).thenReturn(grantResponse);
        when(stub.selectRole(any())).thenReturn(roleResponse);

        DescribeRoleResp result = service.describeRole(stub,
                DescribeRoleReq.builder().roleName("role_a").dbName("db").build());

        assertEquals("role_a", result.getRoleName());
        assertEquals("desc", result.getDescription());
        assertEquals(1, result.getGrantInfos().size());
        DescribeRoleResp.GrantInfo grant = result.getGrantInfos().get(0);
        assertEquals("db", grant.getDbName());
        assertEquals("coll", grant.getObjectName());
        assertEquals("Collection", grant.getObjectType());
        assertEquals("Query", grant.getPrivilege());
        assertEquals("root", grant.getGrantor());
        verify(stub).selectGrant(any(SelectGrantRequest.class));
    }

    @Test
    void dropRoleReturnsNull() {
        when(stub.dropRole(any())).thenReturn(success());
        DropRoleReq request = DropRoleReq.builder().roleName("role_a").forceDrop(true).build();

        assertNull(service.dropRole(stub, request));

        ArgumentCaptor<DropRoleRequest> captor = ArgumentCaptor.forClass(DropRoleRequest.class);
        verify(stub).dropRole(captor.capture());
        assertTrue(captor.getValue().getForceDrop());
    }

    @Test
    void grantPrivilegeReturnsNull() {
        when(stub.operatePrivilege(any())).thenReturn(success());
        GrantPrivilegeReq request = GrantPrivilegeReq.builder()
                .roleName("role_a")
                .objectName("coll")
                .objectType("Collection")
                .privilege("Query")
                .dbName("db")
                .build();

        assertNull(service.grantPrivilege(stub, request));

        ArgumentCaptor<OperatePrivilegeRequest> captor = ArgumentCaptor.forClass(OperatePrivilegeRequest.class);
        verify(stub).operatePrivilege(captor.capture());
        assertEquals(OperatePrivilegeType.Grant, captor.getValue().getType());
    }

    @Test
    void revokePrivilegeReturnsNull() {
        when(stub.operatePrivilege(any())).thenReturn(success());
        RevokePrivilegeReq request = RevokePrivilegeReq.builder()
                .roleName("role_a")
                .objectName("coll")
                .objectType("Collection")
                .privilege("Query")
                .build();

        assertNull(service.revokePrivilege(stub, request));

        ArgumentCaptor<OperatePrivilegeRequest> captor = ArgumentCaptor.forClass(OperatePrivilegeRequest.class);
        verify(stub).operatePrivilege(captor.capture());
        assertEquals(OperatePrivilegeType.Revoke, captor.getValue().getType());
    }

    @Test
    void grantRoleReturnsNull() {
        when(stub.operateUserRole(any())).thenReturn(success());
        GrantRoleReq request = GrantRoleReq.builder().roleName("role_a").userName("user_a").build();

        assertNull(service.grantRole(stub, request));

        ArgumentCaptor<OperateUserRoleRequest> captor = ArgumentCaptor.forClass(OperateUserRoleRequest.class);
        verify(stub).operateUserRole(captor.capture());
        assertEquals(OperateUserRoleType.AddUserToRole, captor.getValue().getType());
    }

    @Test
    void revokeRoleReturnsNull() {
        when(stub.operateUserRole(any())).thenReturn(success());
        RevokeRoleReq request = RevokeRoleReq.builder().roleName("role_a").userName("user_a").build();

        assertNull(service.revokeRole(stub, request));

        ArgumentCaptor<OperateUserRoleRequest> captor = ArgumentCaptor.forClass(OperateUserRoleRequest.class);
        verify(stub).operateUserRole(captor.capture());
        assertEquals(OperateUserRoleType.RemoveUserFromRole, captor.getValue().getType());
    }

    @Test
    void listUsersReturnsNames() {
        ListCredUsersResponse response = ListCredUsersResponse.newBuilder()
                .setStatus(success())
                .addAllUsernames(Arrays.asList("root", "user_a"))
                .build();
        when(stub.listCredUsers(any())).thenReturn(response);

        assertEquals(Arrays.asList("root", "user_a"), service.listUsers(stub));
        verify(stub).listCredUsers(any(ListCredUsersRequest.class));
    }

    @Test
    void describeUserReturnsRolesAndDescription() {
        SelectUserResponse response = SelectUserResponse.newBuilder()
                .setStatus(success())
                .addResults(UserResult.newBuilder()
                        .setUser(UserEntity.newBuilder().setName("user_a").build())
                        .setDescription("a user")
                        .addRoles(RoleEntity.newBuilder().setName("role_a").build())
                        .build())
                .build();
        when(stub.selectUser(any())).thenReturn(response);

        DescribeUserResp result = service.describeUser(stub,
                DescribeUserReq.builder().userName("user_a").build());

        assertEquals("user_a", result.getUserName());
        assertEquals("a user", result.getDescription());
        assertEquals(Collections.singletonList("role_a"), result.getRoles());
        verify(stub).selectUser(any(SelectUserRequest.class));
    }

    @Test
    void createUserBase64EncodesPassword() {
        when(stub.createCredential(any())).thenReturn(success());
        CreateUserReq request = CreateUserReq.builder().userName("user_a").password("pw").build();

        assertNull(service.createUser(stub, request));

        ArgumentCaptor<CreateCredentialRequest> captor = ArgumentCaptor.forClass(CreateCredentialRequest.class);
        verify(stub).createCredential(captor.capture());
        assertEquals("user_a", captor.getValue().getUsername());
        assertEquals("cHc=", captor.getValue().getPassword());
    }

    @Test
    void updatePasswordReturnsNull() {
        when(stub.updateCredential(any())).thenReturn(success());
        UpdatePasswordReq request = UpdatePasswordReq.builder()
                .userName("user_a")
                .password("old")
                .newPassword("new")
                .build();

        assertNull(service.updatePassword(stub, request));

        ArgumentCaptor<UpdateCredentialRequest> captor = ArgumentCaptor.forClass(UpdateCredentialRequest.class);
        verify(stub).updateCredential(captor.capture());
        assertEquals("user_a", captor.getValue().getUsername());
    }

    @Test
    void updateUserReturnsNull() {
        when(stub.updateCredential(any())).thenReturn(success());
        UpdateUserReq request = UpdateUserReq.builder().userName("user_a").description("updated").build();

        assertNull(service.updateUser(stub, request));
        verify(stub).updateCredential(any(UpdateCredentialRequest.class));
    }

    @Test
    void dropUserReturnsNull() {
        when(stub.deleteCredential(any())).thenReturn(success());
        DropUserReq request = DropUserReq.builder().userName("user_a").build();

        assertNull(service.dropUser(stub, request));

        ArgumentCaptor<DeleteCredentialRequest> captor = ArgumentCaptor.forClass(DeleteCredentialRequest.class);
        verify(stub).deleteCredential(captor.capture());
        assertEquals("user_a", captor.getValue().getUsername());
    }

    @Test
    void createPrivilegeGroupReturnsNull() {
        when(stub.createPrivilegeGroup(any())).thenReturn(success());
        CreatePrivilegeGroupReq request = CreatePrivilegeGroupReq.builder().groupName("g1").build();

        assertNull(service.createPrivilegeGroup(stub, request));
        verify(stub).createPrivilegeGroup(any(io.milvus.grpc.CreatePrivilegeGroupRequest.class));
    }

    @Test
    void dropPrivilegeGroupReturnsNull() {
        when(stub.dropPrivilegeGroup(any())).thenReturn(success());
        DropPrivilegeGroupReq request = DropPrivilegeGroupReq.builder().groupName("g1").build();

        assertNull(service.dropPrivilegeGroup(stub, request));
        verify(stub).dropPrivilegeGroup(any(io.milvus.grpc.DropPrivilegeGroupRequest.class));
    }

    @Test
    void listPrivilegeGroupsReturnsGroups() {
        ListPrivilegeGroupsResponse response = ListPrivilegeGroupsResponse.newBuilder()
                .setStatus(success())
                .addPrivilegeGroups(PrivilegeGroupInfo.newBuilder()
                        .setGroupName("g1")
                        .addPrivileges(PrivilegeEntity.newBuilder().setName("Query").build())
                        .build())
                .build();
        when(stub.listPrivilegeGroups(any())).thenReturn(response);

        ListPrivilegeGroupsResp result = service.listPrivilegeGroups(stub,
                ListPrivilegeGroupsReq.builder().build());

        assertEquals(1, result.getPrivilegeGroups().size());
        assertEquals("g1", result.getPrivilegeGroups().get(0).getGroupName());
        assertEquals(Collections.singletonList("Query"),
                result.getPrivilegeGroups().get(0).getPrivileges());
        verify(stub).listPrivilegeGroups(any(ListPrivilegeGroupsRequest.class));
    }

    @Test
    void addPrivilegesToGroupReturnsNull() {
        when(stub.operatePrivilegeGroup(any())).thenReturn(success());
        AddPrivilegesToGroupReq request = AddPrivilegesToGroupReq.builder()
                .groupName("g1")
                .privileges(Collections.singletonList("Query"))
                .build();

        assertNull(service.addPrivilegesToGroup(stub, request));

        ArgumentCaptor<OperatePrivilegeGroupRequest> captor =
                ArgumentCaptor.forClass(OperatePrivilegeGroupRequest.class);
        verify(stub).operatePrivilegeGroup(captor.capture());
        assertEquals(OperatePrivilegeGroupType.AddPrivilegesToGroup, captor.getValue().getType());
    }

    @Test
    void removePrivilegesFromGroupReturnsNull() {
        when(stub.operatePrivilegeGroup(any())).thenReturn(success());
        RemovePrivilegesFromGroupReq request = RemovePrivilegesFromGroupReq.builder()
                .groupName("g1")
                .privileges(Collections.singletonList("Query"))
                .build();

        assertNull(service.removePrivilegesFromGroup(stub, request));

        ArgumentCaptor<OperatePrivilegeGroupRequest> captor =
                ArgumentCaptor.forClass(OperatePrivilegeGroupRequest.class);
        verify(stub).operatePrivilegeGroup(captor.capture());
        assertEquals(OperatePrivilegeGroupType.RemovePrivilegesFromGroup, captor.getValue().getType());
    }

    @Test
    void grantPrivilegeV2ReturnsNull() {
        when(stub.operatePrivilegeV2(any())).thenReturn(success());
        GrantPrivilegeReqV2 request = GrantPrivilegeReqV2.builder()
                .roleName("role_a")
                .dbName("db")
                .collectionName("coll")
                .privilege("Query")
                .build();

        assertNull(service.grantPrivilegeV2(stub, request));

        ArgumentCaptor<OperatePrivilegeV2Request> captor = ArgumentCaptor.forClass(OperatePrivilegeV2Request.class);
        verify(stub).operatePrivilegeV2(captor.capture());
        assertEquals(OperatePrivilegeType.Grant, captor.getValue().getType());
        assertEquals("coll", captor.getValue().getCollectionName());
    }

    @Test
    void revokePrivilegeV2ReturnsNull() {
        when(stub.operatePrivilegeV2(any())).thenReturn(success());
        RevokePrivilegeReqV2 request = RevokePrivilegeReqV2.builder()
                .roleName("role_a")
                .dbName("db")
                .collectionName("coll")
                .privilege("Query")
                .build();

        assertNull(service.revokePrivilegeV2(stub, request));

        ArgumentCaptor<OperatePrivilegeV2Request> captor = ArgumentCaptor.forClass(OperatePrivilegeV2Request.class);
        verify(stub).operatePrivilegeV2(captor.capture());
        assertEquals(OperatePrivilegeType.Revoke, captor.getValue().getType());
    }
}
