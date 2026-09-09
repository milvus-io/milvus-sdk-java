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

package io.milvus.tutorial.rbac;

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
import io.milvus.v2.service.rbac.request.GrantPrivilegeReqV2;
import io.milvus.v2.service.rbac.request.GrantRoleReq;
import io.milvus.v2.service.rbac.request.ListPrivilegeGroupsReq;
import io.milvus.v2.service.rbac.request.RemovePrivilegesFromGroupReq;
import io.milvus.v2.service.rbac.request.RevokePrivilegeReqV2;
import io.milvus.v2.service.rbac.request.RevokeRoleReq;
import io.milvus.v2.service.rbac.response.DescribeRoleResp;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import io.milvus.v2.service.rbac.response.ListPrivilegeGroupsResp;

import java.util.Arrays;

/**
 * Tutorial 8: Role-based access control (RBAC).
 *
 * <p>Demonstrates access management with {@code MilvusClientV2} using an administrator credential:
 * create a user, a role, and a privilege group, add privileges to the group, attach the group to
 * the role, and grant the role to the user. The tutorial then inspects the assignments with
 * {@code describeUser} / {@code describeRole} and cleans everything up.
 *
 * <p>The {@code MILVUS_TOKEN} must identify an administrator allowed to create users, roles,
 * privilege groups, and grants.
 *
 * <p>Connection defaults to {@code MILVUS_URI=http://localhost:19530} and
 * {@code MILVUS_TOKEN=root:Milvus}. Override them with environment variables when needed.
 */
public class App {
    private static final String DATABASE_NAME = "default";
    private static final String COLLECTION_NAME = "*";

    public static void main(String[] args) {
        String uri = System.getenv().getOrDefault("MILVUS_URI", "http://localhost:19530");
        String token = System.getenv().getOrDefault("MILVUS_TOKEN", "root:Milvus");

        ConnectConfig config = ConnectConfig.builder()
                .uri(uri)
                .token(token)
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        try {
            // Milvus 2.6 defaults maxUsernameLength to 32 bytes. Keep the username compact while
            // retaining the thread ID and millisecond timestamp for uniqueness across runs.
            String suffix = System.currentTimeMillis() + "_" + Thread.currentThread().getId();
            String username = "u" + suffix;
            String role = "java_tutorial_role_" + suffix;
            String group = "java_tutorial_group_" + suffix;
            // The tutorial never logs in as this user, so the password is a throwaway generated
            // per run rather than a hard-coded literal.
            String password = "JavaTutorial!" + suffix;

            // createUser creates a login identity. The password is its secret and the description
            // is optional administrator-facing metadata.
            client.createUser(CreateUserReq.builder()
                    .userName(username)
                    .password(password)
                    .description("temporary Java SDK tutorial user")
                    .build());
            System.out.println("Created user " + username);

            // createRole creates a named permission container; privileges are attached in later
            // calls.
            client.createRole(CreateRoleReq.builder()
                    .roleName(role)
                    .description("temporary Java SDK tutorial role")
                    .build());
            System.out.println("Created role " + role);

            // createPrivilegeGroup creates a reusable custom group identified by its name.
            client.createPrivilegeGroup(CreatePrivilegeGroupReq.builder()
                    .groupName(group)
                    .build());
            System.out.println("Created privilege group " + group);

            // addPrivilegesToGroup adds built-in privileges to the group. Query permits scalar
            // reads and Search permits vector searches.
            client.addPrivilegesToGroup(AddPrivilegesToGroupReq.builder()
                    .groupName(group)
                    .privileges(Arrays.asList("Query", "Search"))
                    .build());
            System.out.println("Added Query and Search privileges to group");

            // grantPrivilegeV2 attaches the custom group to the role. The collection name "*"
            // covers all collections in the database.
            client.grantPrivilegeV2(GrantPrivilegeReqV2.builder()
                    .roleName(role)
                    .privilege(group)
                    .dbName(DATABASE_NAME)
                    .collectionName(COLLECTION_NAME)
                    .build());
            System.out.println("Granted privilege group to role");

            // grantRole assigns the named role to the named user.
            client.grantRole(GrantRoleReq.builder()
                    .userName(username)
                    .roleName(role)
                    .build());
            System.out.println("Granted role to user");

            // describeUser returns user metadata and assigned role names.
            DescribeUserResp userResp = client.describeUser(DescribeUserReq.builder()
                    .userName(username)
                    .build());
            System.out.println("User " + userResp.getUserName() + " roles=" + userResp.getRoles());

            // describeRole returns role metadata and grants scoped to the database.
            DescribeRoleResp roleResp = client.describeRole(DescribeRoleReq.builder()
                    .roleName(role)
                    .dbName(DATABASE_NAME)
                    .build());
            System.out.println("Role " + roleResp.getRoleName() + " grants=" + roleResp.getGrantInfos());

            cleanup(client, username, role, group);
        } finally {
            client.close();
        }
    }

    private static void cleanup(MilvusClientV2 client, String username, String role, String group) {
        // revokeRole removes the user's role assignment before any resource deletion.
        if (client.listUsers().contains(username)) {
            DescribeUserResp userResp = client.describeUser(DescribeUserReq.builder()
                    .userName(username)
                    .build());
            if (userResp.getRoles().contains(role)) {
                client.revokeRole(RevokeRoleReq.builder()
                        .userName(username)
                        .roleName(role)
                        .build());
                System.out.println("Revoked role from user");
            }
        }

        // revokePrivilegeV2 detaches the group from this role/database/collection scope.
        if (client.listRoles().contains(role)) {
            client.revokePrivilegeV2(RevokePrivilegeReqV2.builder()
                    .roleName(role)
                    .privilege(group)
                    .dbName(DATABASE_NAME)
                    .collectionName(COLLECTION_NAME)
                    .build());
            System.out.println("Revoked privilege group from role");
        }

        // removePrivilegesFromGroup removes the built-in privileges before deleting the group.
        ListPrivilegeGroupsResp groupsResp = client.listPrivilegeGroups(ListPrivilegeGroupsReq.builder().build());
        for (PrivilegeGroup privilegeGroup : groupsResp.getPrivilegeGroups()) {
            if (group.equals(privilegeGroup.getGroupName())) {
                client.removePrivilegesFromGroup(RemovePrivilegesFromGroupReq.builder()
                        .groupName(group)
                        .privileges(Arrays.asList("Query", "Search"))
                        .build());
                client.dropPrivilegeGroup(DropPrivilegeGroupReq.builder()
                        .groupName(group)
                        .build());
                System.out.println("Removed and dropped privilege group");
            }
        }

        if (client.listRoles().contains(role)) {
            client.dropRole(DropRoleReq.builder()
                    .roleName(role)
                    .forceDrop(true)
                    .build());
            System.out.println("Dropped role");
        }

        if (client.listUsers().contains(username)) {
            client.dropUser(DropUserReq.builder()
                    .userName(username)
                    .build());
            System.out.println("Dropped user");
        }
    }
}
