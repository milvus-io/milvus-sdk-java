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

import io.milvus.grpc.*;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.rbac.request.*;
import io.milvus.v2.service.rbac.response.DescribeRoleResp;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import io.milvus.v2.service.rbac.response.ListPrivilegeGroupsResp;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class RBACService extends BaseService {
    public List<String> listRoles(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub) {
        String title = "listRoles";
        SelectRoleRequest request = SelectRoleRequest.newBuilder().build();
        SelectRoleResponse response = blockingStub.selectRole(request);

        rpcUtils.handleResponse(title, response.getStatus());
        return response.getResultsList().stream().map(roleResult -> roleResult.getRole().getName()).collect(Collectors.toList());
    }

    public Void createRole(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateRoleReq request) {
        String title = "createRole";
        CreateRoleRequest createRoleRequest = CreateRoleRequest.newBuilder()
                .setEntity(RoleEntity.newBuilder()
                        .setName(request.getRoleName())
                        .build())
                .build();
        Status status = blockingStub.createRole(createRoleRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public DescribeRoleResp describeRole(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DescribeRoleReq request) {
        String title = "describeRole";
        SelectGrantRequest selectGrantRequest = SelectGrantRequest.newBuilder()
                .setEntity(GrantEntity.newBuilder()
                        .setRole(RoleEntity.newBuilder()
                                .setName(request.getRoleName())
                                .build())
                        .build())
                .build();
        SelectGrantResponse response = blockingStub.selectGrant(selectGrantRequest);
        rpcUtils.handleResponse(title, response.getStatus());
        DescribeRoleResp describeRoleResp = DescribeRoleResp.builder()
                .grantInfos(response.getEntitiesList().stream().map(grantEntity -> DescribeRoleResp.GrantInfo.builder()
                        .dbName(grantEntity.getDbName())
                        .objectName(grantEntity.getObjectName())
                        .objectType(grantEntity.getObject().getName())
                        .privilege(grantEntity.getGrantor().getPrivilege().getName())
                        .grantor(grantEntity.getGrantor().getUser().getName())
                        .build()).collect(Collectors.toList()))
                .build();
        return describeRoleResp;
    }

    public Void dropRole(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropRoleReq request) {
        String title = "dropRole";
        DropRoleRequest dropRoleRequest = DropRoleRequest.newBuilder()
                .setRoleName(request.getRoleName())
                .build();
        Status status = blockingStub.dropRole(dropRoleRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void grantPrivilege(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, GrantPrivilegeReq request) {
        String title = "grantPrivilege";
        GrantEntity entity = GrantEntity.newBuilder()
                .setRole(RoleEntity.newBuilder()
                        .setName(request.getRoleName())
                        .build())
                .setObjectName(request.getObjectName())
                .setObject(ObjectEntity.newBuilder().setName(request.getObjectType()).build())
                .setGrantor(GrantorEntity.newBuilder()
                        .setPrivilege(PrivilegeEntity.newBuilder().setName(request.getPrivilege()).build()).build())
                .build();
        OperatePrivilegeRequest operatePrivilegeRequest = OperatePrivilegeRequest.newBuilder()
                .setEntity(entity)
                .setType(OperatePrivilegeType.Grant)
                .build();
        Status status = blockingStub.operatePrivilege(operatePrivilegeRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void revokePrivilege(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, RevokePrivilegeReq request) {
        String title = "revokePrivilege";
        GrantEntity entity = GrantEntity.newBuilder()
                .setRole(RoleEntity.newBuilder()
                        .setName(request.getRoleName())
                        .build())
                .setObjectName(request.getObjectName())
                .setObject(ObjectEntity.newBuilder().setName(request.getObjectType()).build())
                .setGrantor(GrantorEntity.newBuilder()
                        .setPrivilege(PrivilegeEntity.newBuilder().setName(request.getPrivilege()).build()).build())
                .build();
        OperatePrivilegeRequest operatePrivilegeRequest = OperatePrivilegeRequest.newBuilder()
                .setEntity(entity)
                .setType(OperatePrivilegeType.Revoke)
                .build();
        Status status = blockingStub.operatePrivilege(operatePrivilegeRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void grantRole(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, GrantRoleReq request) {
        String title = "grantRole";
        OperateUserRoleRequest operateUserRoleRequest = OperateUserRoleRequest.newBuilder()
                .setUsername(request.getUserName())
                .setRoleName(request.getRoleName())
                .setType(OperateUserRoleType.AddUserToRole)
                .build();
        Status status = blockingStub.operateUserRole(operateUserRoleRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void revokeRole(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, RevokeRoleReq request) {
        String title = "grantRole";
        OperateUserRoleRequest operateUserRoleRequest = OperateUserRoleRequest.newBuilder()
                .setUsername(request.getUserName())
                .setRoleName(request.getRoleName())
                .setType(OperateUserRoleType.RemoveUserFromRole)
                .build();
        Status status = blockingStub.operateUserRole(operateUserRoleRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public List<String> listUsers(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub) {
        String title = "list users";
        ListCredUsersRequest request = ListCredUsersRequest.newBuilder().build();
        ListCredUsersResponse response = blockingStub.listCredUsers(request);
        rpcUtils.handleResponse(title, response.getStatus());
        return response.getUsernamesList();
    }

    public DescribeUserResp describeUser(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DescribeUserReq request) {
        String title = String.format("describe user %s", request.getUserName());
        // TODO: check user exists
        SelectUserRequest selectUserRequest = SelectUserRequest.newBuilder()
                .setUser(UserEntity.newBuilder().setName(request.getUserName()).build())
                .setIncludeRoleInfo(Boolean.TRUE)
                .build();
        io.milvus.grpc.SelectUserResponse response = blockingStub.selectUser(selectUserRequest);
        rpcUtils.handleResponse(title, response.getStatus());
        DescribeUserResp describeUserResp = DescribeUserResp.builder()
                .roles(response.getResultsList().isEmpty()? null : response.getResultsList().get(0).getRolesList().stream().map(RoleEntity::getName).collect(Collectors.toList()))
                .build();
        return describeUserResp;
    }

    public Void createUser(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateUserReq request) {
        String title = String.format("create user %s", request.getUserName());
        CreateCredentialRequest createCredentialRequest = CreateCredentialRequest.newBuilder()
                .setUsername(request.getUserName())
                .setPassword(Base64.getEncoder().encodeToString(request.getPassword().getBytes(StandardCharsets.UTF_8)))
                .build();
        Status response = blockingStub.createCredential(createCredentialRequest);
        rpcUtils.handleResponse(title, response);

        return null;
    }


    public Void updatePassword(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, UpdatePasswordReq request) {
        String title = String.format("update password for user %s", request.getUserName());
        UpdateCredentialRequest updateCredentialRequest = UpdateCredentialRequest.newBuilder()
                .setUsername(request.getUserName())
                .setOldPassword(Base64.getEncoder().encodeToString(request.getPassword().getBytes(StandardCharsets.UTF_8)))
                .setNewPassword(Base64.getEncoder().encodeToString(request.getNewPassword().getBytes(StandardCharsets.UTF_8)))
                .build();
        Status response = blockingStub.updateCredential(updateCredentialRequest);
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Void dropUser(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropUserReq request) {
        String title = String.format("drop user %s", request.getUserName());
        DeleteCredentialRequest deleteCredentialRequest = DeleteCredentialRequest.newBuilder()
                .setUsername(request.getUserName())
                .build();
        Status response = blockingStub.deleteCredential(deleteCredentialRequest);
        rpcUtils.handleResponse(title, response);

        return null;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public Void createPrivilegeGroup(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreatePrivilegeGroupReq request) {
        String title = String.format("create privilege group %s", request.getGroupName());
        CreatePrivilegeGroupRequest createPrivilegeGroupRequest = CreatePrivilegeGroupRequest.newBuilder()
                .setGroupName(request.getGroupName())
                .build();
        Status response = blockingStub.createPrivilegeGroup(createPrivilegeGroupRequest);
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Void dropPrivilegeGroup(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropPrivilegeGroupReq request) {
        String title = String.format("drop privilege group %s", request.getGroupName());
        DropPrivilegeGroupRequest dropPrivilegeGroupRequest = DropPrivilegeGroupRequest.newBuilder()
                .setGroupName(request.getGroupName())
                .build();
        Status response = blockingStub.dropPrivilegeGroup(dropPrivilegeGroupRequest);
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public ListPrivilegeGroupsResp listPrivilegeGroups(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ListPrivilegeGroupsReq request) {
        String title = "list privilege groups";
        ListPrivilegeGroupsRequest listPrivilegeGroupsRequest = ListPrivilegeGroupsRequest.newBuilder()
                .build();
        ListPrivilegeGroupsResponse response = blockingStub.listPrivilegeGroups(listPrivilegeGroupsRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        List<PrivilegeGroup> privilegeGroups = new ArrayList<>();
        response.getPrivilegeGroupsList().forEach((privilegeGroupInfo)->{
            List<String> privileges = new ArrayList<>();
            privilegeGroupInfo.getPrivilegesList().forEach((privilege)->{
                privileges.add(privilege.getName());
            });
            privilegeGroups.add(PrivilegeGroup.builder().groupName(privilegeGroupInfo.getGroupName()).privileges(privileges).build());
        });

        return ListPrivilegeGroupsResp.builder()
                .privilegeGroups(privilegeGroups)
                .build();
    }

    public Void addPrivilegesToGroup(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AddPrivilegesToGroupReq request) {
        String title = String.format("add privilege to group %s", request.getGroupName());
        OperatePrivilegeGroupRequest.Builder builder = OperatePrivilegeGroupRequest.newBuilder()
                .setGroupName(request.getGroupName())
                .setType(OperatePrivilegeGroupType.AddPrivilegesToGroup);
        for (String privilege : request.getPrivileges()) {
            builder.addPrivileges(PrivilegeEntity.newBuilder().setName(privilege).build());
        }

        Status response = blockingStub.operatePrivilegeGroup(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Void removePrivilegesFromGroup(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, RemovePrivilegesFromGroupReq request) {
        String title = String.format("remove privilege from group %s", request.getGroupName());
        OperatePrivilegeGroupRequest.Builder builder = OperatePrivilegeGroupRequest.newBuilder()
                .setGroupName(request.getGroupName())
                .setType(OperatePrivilegeGroupType.RemovePrivilegesFromGroup);
        for (String privilege : request.getPrivileges()) {
            builder.addPrivileges(PrivilegeEntity.newBuilder().setName(privilege).build());
        }

        Status response = blockingStub.operatePrivilegeGroup(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Void grantPrivilegeV2(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, GrantPrivilegeReqV2 request) {
        String title = "grantPrivilegeV2";
        OperatePrivilegeV2Request.Builder builder = OperatePrivilegeV2Request.newBuilder()
                .setRole(RoleEntity.newBuilder().setName(request.getRoleName()).build())
                .setGrantor(GrantorEntity.newBuilder().setPrivilege(PrivilegeEntity.newBuilder().setName(request.getPrivilege()).build()).build())
                .setDbName(request.getDbName())
                .setCollectionName(request.getCollectionName())
                .setType(OperatePrivilegeType.Grant);

        Status response = blockingStub.operatePrivilegeV2(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Void revokePrivilegeV2(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, RevokePrivilegeReqV2 request) {
        String title = "revokePrivilegeV2";
        OperatePrivilegeV2Request.Builder builder = OperatePrivilegeV2Request.newBuilder()
                .setRole(RoleEntity.newBuilder().setName(request.getRoleName()).build())
                .setGrantor(GrantorEntity.newBuilder().setPrivilege(PrivilegeEntity.newBuilder().setName(request.getPrivilege()).build()).build())
                .setDbName(request.getDbName())
                .setCollectionName(request.getCollectionName())
                .setType(OperatePrivilegeType.Revoke);

        Status response = blockingStub.operatePrivilegeV2(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }
}
