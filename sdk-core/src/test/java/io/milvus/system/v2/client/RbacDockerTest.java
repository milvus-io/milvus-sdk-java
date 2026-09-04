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

package io.milvus.system.v2.client;

import io.milvus.support.v2.MilvusV2DockerTestBase;
import io.milvus.v2.service.rbac.PrivilegeGroup;
import io.milvus.v2.service.rbac.request.AddPrivilegesToGroupReq;
import io.milvus.v2.service.rbac.request.CreatePrivilegeGroupReq;
import io.milvus.v2.service.rbac.request.ListPrivilegeGroupsReq;
import io.milvus.v2.service.rbac.response.ListPrivilegeGroupsResp;
import java.util.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("system")
class RbacDockerTest extends MilvusV2DockerTestBase {
    @Test
    void testRBAC() {
        client.createPrivilegeGroup(CreatePrivilegeGroupReq.builder()
                .groupName("dummy")
                .build());
        client.addPrivilegesToGroup(AddPrivilegesToGroupReq.builder()
                .groupName("dummy")
                .privileges(Collections.singletonList("CreateCollection"))
                .build());

        ListPrivilegeGroupsResp resp = client.listPrivilegeGroups(ListPrivilegeGroupsReq.builder().build());
        List<PrivilegeGroup> groups = resp.getPrivilegeGroups();
        Map<String, List<String>> groupsPrivileges = new HashMap<>();
        for (PrivilegeGroup group : groups) {
            groupsPrivileges.put(group.getGroupName(), group.getPrivileges());
        }
        Assertions.assertTrue(groupsPrivileges.containsKey("dummy"));
        Assertions.assertEquals(1, groupsPrivileges.get("dummy").size());
        Assertions.assertEquals("CreateCollection", groupsPrivileges.get("dummy").get(0));
    }

}
