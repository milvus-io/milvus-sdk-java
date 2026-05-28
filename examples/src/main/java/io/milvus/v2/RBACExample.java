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

package io.milvus.v2;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.rbac.PrivilegeGroup;
import io.milvus.v2.service.rbac.request.AddPrivilegesToGroupReq;
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
import io.milvus.v2.service.rbac.response.DescribeRoleResp;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import io.milvus.v2.service.rbac.response.ListPrivilegeGroupsResp;

import java.util.Arrays;
import java.util.List;

// Before running this example, make sure you have enabled RBAC in Milvus and have the root user credentials ready.
// Read this doc to know how to enable RBAC in milvus: https://milvus.io/docs/authenticate.md?tab=docker
public class RBACExample {
    private static final String URI = "http://localhost:19530";
    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "Milvus";
    private static final String DEMO_USER = "java_sdk_example_user";
    private static final String DEMO_PASSWORD = "Milvus@2026";
    private static final String DEMO_PASSWORD_UPDATED = "Milvus@2027";
    private static final String DEMO_ROLE = "java_sdk_example_role";
    private static final String DEMO_PRIVILEGE_GROUP = "java_sdk_example_privilege_group";
    private static final String DATABASE_NAME = "default";
    private static final String COLLECTION_NAME = "*";

    public static void main(String[] args) {
        ConnectConfig config = ConnectConfig.builder()
                .uri(URI)
                .username(ROOT_USER)
                .password(ROOT_PASSWORD)
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        try {
            cleanup(client);
            runExample(client);
        } finally {
            cleanup(client);
            client.close();
        }
    }

    private static void runExample(MilvusClientV2 client) {
        print("Initial users", client.listUsers());
        print("Initial roles", client.listRoles());

        client.createUser(CreateUserReq.builder()
                .userName(DEMO_USER)
                .password(DEMO_PASSWORD)
                .build());
        print("After create user", client.listUsers());

        DescribeUserResp userResp = client.describeUser(DescribeUserReq.builder()
                .userName(DEMO_USER)
                .build());
        print("Describe user", userResp);

        client.updatePassword(UpdatePasswordReq.builder()
                .userName(DEMO_USER)
                .password(DEMO_PASSWORD)
                .newPassword(DEMO_PASSWORD_UPDATED)
                .resetConnection(false)
                .build());
        print("Password updated", DEMO_USER);

        client.createRole(CreateRoleReq.builder()
                .roleName(DEMO_ROLE)
                .build());
        print("After create role", client.listRoles());

        client.grantPrivilege(GrantPrivilegeReq.builder()
                .roleName(DEMO_ROLE)
                .objectType("Global")
                .objectName("*")
                .privilege("CreateCollection")
                .build());
        client.grantPrivilege(GrantPrivilegeReq.builder()
                .roleName(DEMO_ROLE)
                .objectType("Collection")
                .objectName("*")
                .privilege("Search")
                .build());

        client.createPrivilegeGroup(CreatePrivilegeGroupReq.builder()
                .groupName(DEMO_PRIVILEGE_GROUP)
                .build());
        client.addPrivilegesToGroup(AddPrivilegesToGroupReq.builder()
                .groupName(DEMO_PRIVILEGE_GROUP)
                .privileges(Arrays.asList("Query", "Load"))
                .build());
        client.removePrivilegesFromGroup(RemovePrivilegesFromGroupReq.builder()
                .groupName(DEMO_PRIVILEGE_GROUP)
                .privileges(Arrays.asList("Load"))
                .build());

        ListPrivilegeGroupsResp groupsResp = client.listPrivilegeGroups(ListPrivilegeGroupsReq.builder().build());
        for (PrivilegeGroup group : groupsResp.getPrivilegeGroups()) {
            if (DEMO_PRIVILEGE_GROUP.equals(group.getGroupName())) {
                print("Privilege group", group);
            }
        }

        client.grantPrivilegeV2(GrantPrivilegeReqV2.builder()
                .roleName(DEMO_ROLE)
                .privilege(DEMO_PRIVILEGE_GROUP)
                .dbName(DATABASE_NAME)
                .collectionName(COLLECTION_NAME)
                .build());

        DescribeRoleResp roleResp = client.describeRole(DescribeRoleReq.builder()
                .roleName(DEMO_ROLE)
                .dbName(DATABASE_NAME)
                .build());
        print("Describe role", roleResp);

        client.grantRole(GrantRoleReq.builder()
                .userName(DEMO_USER)
                .roleName(DEMO_ROLE)
                .build());
        print("User after grant role", client.describeUser(DescribeUserReq.builder()
                .userName(DEMO_USER)
                .build()));

        MilvusClientV2 userClient = new MilvusClientV2(ConnectConfig.builder()
                .uri(URI)
                .username(DEMO_USER)
                .password(DEMO_PASSWORD_UPDATED)
                .build());
        try {
            print("Updated user client ready", userClient.clientIsReady());
            print("Server version", userClient.getServerVersion());
        } finally {
            userClient.close();
        }

        client.revokeRole(RevokeRoleReq.builder()
                .userName(DEMO_USER)
                .roleName(DEMO_ROLE)
                .build());
        print("User after revoke role", client.describeUser(DescribeUserReq.builder()
                .userName(DEMO_USER)
                .build()));

        cleanup(client);
        print("Final users", client.listUsers());
        print("Final roles", client.listRoles());
    }

    private static void cleanup(MilvusClientV2 client) {
        if (userHasRole(client, DEMO_USER, DEMO_ROLE)) {
            client.revokeRole(RevokeRoleReq.builder()
                    .userName(DEMO_USER)
                    .roleName(DEMO_ROLE)
                    .build());
        }

        if (client.listRoles().contains(DEMO_ROLE)) {
            DescribeRoleResp roleResp = client.describeRole(DescribeRoleReq.builder()
                    .roleName(DEMO_ROLE)
                    .dbName(DATABASE_NAME)
                    .build());
            for (DescribeRoleResp.GrantInfo grantInfo : roleResp.getGrantInfos()) {
                if (DEMO_PRIVILEGE_GROUP.equals(grantInfo.getPrivilege())) {
                    client.revokePrivilegeV2(RevokePrivilegeReqV2.builder()
                            .roleName(DEMO_ROLE)
                            .privilege(DEMO_PRIVILEGE_GROUP)
                            .dbName(DATABASE_NAME)
                            .collectionName(COLLECTION_NAME)
                            .build());
                } else if ("CreateCollection".equals(grantInfo.getPrivilege())) {
                    client.revokePrivilege(RevokePrivilegeReq.builder()
                            .roleName(DEMO_ROLE)
                            .objectType("Global")
                            .objectName("*")
                            .privilege("CreateCollection")
                            .build());
                } else if ("Search".equals(grantInfo.getPrivilege())) {
                    client.revokePrivilege(RevokePrivilegeReq.builder()
                            .roleName(DEMO_ROLE)
                            .objectType("Collection")
                            .objectName("*")
                            .privilege("Search")
                            .build());
                }
            }

            client.dropRole(DropRoleReq.builder()
                    .roleName(DEMO_ROLE)
                    .forceDrop(true)
                    .build());
        }

        if (client.listUsers().contains(DEMO_USER)) {
            client.dropUser(DropUserReq.builder()
                    .userName(DEMO_USER)
                    .build());
        }

        if (privilegeGroupExists(client, DEMO_PRIVILEGE_GROUP)) {
            client.removePrivilegesFromGroup(RemovePrivilegesFromGroupReq.builder()
                    .groupName(DEMO_PRIVILEGE_GROUP)
                    .privileges(Arrays.asList("Query", "Load"))
                    .build());
            client.dropPrivilegeGroup(DropPrivilegeGroupReq.builder()
                    .groupName(DEMO_PRIVILEGE_GROUP)
                    .build());
        }
    }

    private static boolean userHasRole(MilvusClientV2 client, String userName, String roleName) {
        if (!client.listUsers().contains(userName)) {
            return false;
        }

        DescribeUserResp userResp = client.describeUser(DescribeUserReq.builder()
                .userName(userName)
                .build());
        List<String> roles = userResp.getRoles();
        return roles != null && roles.contains(roleName);
    }

    private static boolean privilegeGroupExists(MilvusClientV2 client, String groupName) {
        ListPrivilegeGroupsResp groupsResp = client.listPrivilegeGroups(ListPrivilegeGroupsReq.builder().build());
        for (PrivilegeGroup group : groupsResp.getPrivilegeGroups()) {
            if (groupName.equals(group.getGroupName())) {
                return true;
            }
        }
        return false;
    }

    private static void print(String label, Object value) {
        System.out.printf("%-32s%s%n", label + ":", value);
    }
}
