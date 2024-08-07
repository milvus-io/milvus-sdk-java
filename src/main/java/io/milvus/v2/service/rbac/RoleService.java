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

import java.util.List;
import java.util.stream.Collectors;

public class RoleService extends BaseService {

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
}
