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
package io.milvus.v1;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.ListCredUsersResponse;
import io.milvus.grpc.SelectRoleResponse;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.credential.CreateCredentialParam;
import io.milvus.param.credential.ListCredUsersParam;
import io.milvus.param.role.*;
import org.apache.commons.lang3.Validate;


public class RBACExample {
    private static final MilvusServiceClient milvusClient;

    static {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .withAuthorization("root","Milvus")
                .build();
        milvusClient = new MilvusServiceClient(connectParam);
    }

    public static R<RpcStatus> createUser(String userName, String password) {
        return milvusClient.createCredential(CreateCredentialParam.newBuilder()
                .withUsername(userName)
                .withPassword(password)
                .build());
    }

    public static R<RpcStatus> grantUserRole(String userName, String roleName) {
        return milvusClient.addUserToRole(AddUserToRoleParam.newBuilder()
                .withUserName(userName)
                .withRoleName(roleName)
                .build());
    }

    public static R<RpcStatus> revokeUserRole(String userName, String roleName) {
        return milvusClient.removeUserFromRole(RemoveUserFromRoleParam.newBuilder()
                .withUserName(userName)
                .withRoleName(roleName)
                .build());
    }

    public static R<ListCredUsersResponse> listUsers() {
        return milvusClient.listCredUsers(ListCredUsersParam.newBuilder()
                .build());
    }

    public static R<SelectRoleResponse> selectRole(String roleName) {
        return milvusClient.selectRole(SelectRoleParam.newBuilder()
                .withRoleName(roleName)
                .build());
    }

    public static R<RpcStatus> createRole(String roleName) {
        return milvusClient.createRole(CreateRoleParam.newBuilder()
                .withRoleName(roleName)
                .build());
    }

    public static R<RpcStatus> dropRole(String roleName) {
        return milvusClient.dropRole(DropRoleParam.newBuilder()
                .withRoleName(roleName)
                .build());
    }

    public static R<RpcStatus> grantRolePrivilege(String roleName, String objectType, String objectName, String privilege) {
        return milvusClient.grantRolePrivilege(GrantRolePrivilegeParam.newBuilder()
                .withRoleName(roleName)
                .withObject(objectType)
                .withObjectName(objectName)
                .withPrivilege(privilege)
                .build());
    }

    public static R<RpcStatus> revokeRolePrivilege(String roleName, String objectType, String objectName, String privilege) {
        return milvusClient.revokeRolePrivilege(RevokeRolePrivilegeParam.newBuilder()
                .withRoleName(roleName)
                .withObject(objectType)
                .withObjectName(objectName)
                .withPrivilege(privilege)
                .build());
    }


    public static void main(String[] args) {
        // create a role
        R<RpcStatus> resp = createRole("role1");
        Validate.isTrue(resp.getStatus() == R.success().getStatus(), "create role fail!");
        System.out.println("role1 created");

        //create user
        resp = createUser("user", "pwd123456");
        Validate.isTrue(resp.getStatus() == R.success().getStatus(), "create user fail!");
        System.out.println("user created");

        // grant privilege to role.
        // grant object is all collections, grant object type is Collection, and the privilege is CreateCollection
        resp = grantRolePrivilege("role1","Global","*",  "CreateCollection");
        Validate.isTrue(resp.getStatus() == R.success().getStatus(), "bind privileges to role fail!");
        System.out.println("grant privilege to role1");

        // bind role to user
        resp = grantUserRole("user", "role1");
        Validate.isTrue(resp.getStatus() == R.success().getStatus(), "bind role to user fail!");
        System.out.println("bind role1 to user");

        // revoke privilege from role
        resp = revokeRolePrivilege("role1","Global","*",  "CreateCollection");
        Validate.isTrue(resp.getStatus() == R.success().getStatus(), "revoke privileges to role fail!");
        System.out.println("revoke privilege from role1");

        // list role
        R<SelectRoleResponse> resp1 = selectRole("role1");
        Validate.isTrue(resp1.getStatus() == R.success().getStatus(), "select role information fail!");

        // delete a role
        resp = dropRole("role1");
        Validate.isTrue(resp.getStatus() == R.success().getStatus(), "drop role fail!");
        System.out.println("delete role1");
    }
}