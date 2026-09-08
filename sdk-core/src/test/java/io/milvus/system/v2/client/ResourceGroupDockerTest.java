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
import io.milvus.common.resourcegroup.NodeInfo;
import io.milvus.common.resourcegroup.ResourceGroupConfig;
import io.milvus.common.resourcegroup.ResourceGroupLimit;
import io.milvus.common.resourcegroup.ResourceGroupTransfer;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.resourcegroup.request.*;
import io.milvus.v2.service.resourcegroup.response.DescribeResourceGroupResp;
import io.milvus.v2.service.resourcegroup.response.ListResourceGroupsResp;
import org.apache.commons.lang3.StringUtils;
import java.util.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("system")
class ResourceGroupDockerTest extends MilvusV2DockerTestBase {
    @Test
    void testResourceGroup() {
        String groupA = "group_A";
        String groupDefault = "__default_resource_group";
        client.createResourceGroup(CreateResourceGroupReq.builder()
                .groupName(groupA)
                .config(ResourceGroupConfig.newBuilder()
                        .withRequests(new ResourceGroupLimit(3))
                        .withLimits(new ResourceGroupLimit(4))
                        .withFrom(Collections.singletonList(new ResourceGroupTransfer(groupDefault)))
                        .withTo(Collections.singletonList(new ResourceGroupTransfer(groupDefault)))
                        .build())
                .build());

        ListResourceGroupsResp listResp = client.listResourceGroups(ListResourceGroupsReq.builder().build());
        List<String> groupNames = listResp.getGroupNames();
        Assertions.assertEquals(2, groupNames.size());
        Assertions.assertTrue(groupNames.contains(groupA));
        Assertions.assertTrue(groupNames.contains(groupDefault));

        // A
        DescribeResourceGroupResp descResp = client.describeResourceGroup(DescribeResourceGroupReq.builder()
                .groupName(groupA)
                .build());
        Assertions.assertEquals(groupA, descResp.getGroupName());
        Assertions.assertEquals(3, descResp.getCapacity());
        Assertions.assertEquals(1, descResp.getNumberOfAvailableNode());

        ResourceGroupConfig config = descResp.getConfig();
        Assertions.assertEquals(3, config.getRequests().getNodeNum());
        Assertions.assertEquals(4, config.getLimits().getNodeNum());

        Assertions.assertEquals(1, config.getFrom().size());
        Assertions.assertEquals(groupDefault, config.getFrom().get(0).getResourceGroupName());
        Assertions.assertEquals(1, config.getTo().size());
        Assertions.assertEquals(groupDefault, config.getTo().get(0).getResourceGroupName());

        List<NodeInfo> nodes = descResp.getNodes();
        Assertions.assertEquals(1, nodes.size());
        Assertions.assertTrue(nodes.get(0).getNodeId() > 0L);
        Assertions.assertTrue(StringUtils.isNotEmpty(nodes.get(0).getAddress()));
        Assertions.assertTrue(StringUtils.isNotEmpty(nodes.get(0).getHostname()));

        // update
        Map<String, ResourceGroupConfig> resourceGroups = new HashMap<>();
        resourceGroups.put(groupA, ResourceGroupConfig.newBuilder()
                .withRequests(new ResourceGroupLimit(0))
                .withLimits(new ResourceGroupLimit(0))
                .build());
        client.updateResourceGroups(UpdateResourceGroupsReq.builder()
                .resourceGroups(resourceGroups)
                .build());

        descResp = client.describeResourceGroup(DescribeResourceGroupReq.builder()
                .groupName(groupA)
                .build());

        config = descResp.getConfig();
        Assertions.assertEquals(0, config.getRequests().getNodeNum());
        Assertions.assertEquals(0, config.getLimits().getNodeNum());
        Assertions.assertTrue(config.getFrom().isEmpty());
        Assertions.assertTrue(config.getTo().isEmpty());

        // drop
        client.dropResourceGroup(DropResourceGroupReq.builder()
                .groupName(groupA)
                .build());

        // transfer
        String collectionName = generator.generate(10);
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .dimension(DIMENSION)
                .build());
    }

}
