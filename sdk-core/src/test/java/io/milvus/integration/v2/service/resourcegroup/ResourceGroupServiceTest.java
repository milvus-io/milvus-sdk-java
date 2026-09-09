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

package io.milvus.integration.v2.service.resourcegroup;

import io.milvus.common.resourcegroup.ResourceGroupConfig;
import io.milvus.common.resourcegroup.ResourceGroupLimit;
import io.milvus.grpc.CreateResourceGroupRequest;
import io.milvus.grpc.DescribeResourceGroupRequest;
import io.milvus.grpc.DescribeResourceGroupResponse;
import io.milvus.grpc.DropResourceGroupRequest;
import io.milvus.grpc.ListResourceGroupsResponse;
import io.milvus.grpc.NodeInfo;
import io.milvus.grpc.ResourceGroup;
import io.milvus.grpc.Status;
import io.milvus.grpc.TransferNodeRequest;
import io.milvus.grpc.TransferReplicaRequest;
import io.milvus.grpc.UpdateResourceGroupsRequest;
import io.milvus.support.v2.BaseTest;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.resourcegroup.request.CreateResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.DescribeResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.DropResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.ListResourceGroupsReq;
import io.milvus.v2.service.resourcegroup.request.TransferNodeReq;
import io.milvus.v2.service.resourcegroup.request.TransferReplicaReq;
import io.milvus.v2.service.resourcegroup.request.UpdateResourceGroupsReq;
import io.milvus.v2.service.resourcegroup.response.DescribeResourceGroupResp;
import io.milvus.v2.service.resourcegroup.response.ListResourceGroupsResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("integration")
class ResourceGroupServiceTest extends BaseTest {

    Logger logger = LoggerFactory.getLogger(ResourceGroupServiceTest.class);

    @Test
    void testCreateResourceGroup() {
        Status successStatus = Status.newBuilder().setCode(0).build();
        when(blockingStub.createResourceGroup(any())).thenReturn(successStatus);

        ResourceGroupConfig config = ResourceGroupConfig.newBuilder()
                .withRequests(new ResourceGroupLimit(1))
                .withLimits(new ResourceGroupLimit(1))
                .build();
        CreateResourceGroupReq request = CreateResourceGroupReq.builder()
                .groupName("rg_1")
                .config(config)
                .build();
        client_v2.createResourceGroup(request);

        ArgumentCaptor<CreateResourceGroupRequest> captor = ArgumentCaptor.forClass(CreateResourceGroupRequest.class);
        verify(blockingStub).createResourceGroup(captor.capture());
        assertEquals("rg_1", captor.getValue().getResourceGroup());
        assertEquals(1, captor.getValue().getConfig().getRequests().getNodeNum());
    }

    @Test
    void testCreateResourceGroupWithoutConfig() {
        CreateResourceGroupReq request = CreateResourceGroupReq.builder()
                .groupName("rg_1")
                .build();
        assertThrows(MilvusClientException.class, () -> client_v2.createResourceGroup(request));
    }

    @Test
    void testUpdateResourceGroups() {
        Status successStatus = Status.newBuilder().setCode(0).build();
        when(blockingStub.updateResourceGroups(any())).thenReturn(successStatus);

        Map<String, ResourceGroupConfig> groups = new HashMap<>();
        groups.put("rg_1", ResourceGroupConfig.newBuilder()
                .withRequests(new ResourceGroupLimit(1))
                .withLimits(new ResourceGroupLimit(2))
                .build());
        UpdateResourceGroupsReq request = UpdateResourceGroupsReq.builder()
                .resourceGroups(groups)
                .build();
        client_v2.updateResourceGroups(request);

        ArgumentCaptor<UpdateResourceGroupsRequest> captor = ArgumentCaptor.forClass(UpdateResourceGroupsRequest.class);
        verify(blockingStub).updateResourceGroups(captor.capture());
        assertEquals(1, captor.getValue().getResourceGroupsMap().get("rg_1").getRequests().getNodeNum());
        assertEquals(2, captor.getValue().getResourceGroupsMap().get("rg_1").getLimits().getNodeNum());
    }

    @Test
    void testUpdateResourceGroupsEmpty() {
        UpdateResourceGroupsReq request = UpdateResourceGroupsReq.builder()
                .build();
        assertThrows(MilvusClientException.class, () -> client_v2.updateResourceGroups(request));
    }

    @Test
    void testDropResourceGroup() {
        Status successStatus = Status.newBuilder().setCode(0).build();
        when(blockingStub.dropResourceGroup(any())).thenReturn(successStatus);

        DropResourceGroupReq request = DropResourceGroupReq.builder()
                .groupName("rg_1")
                .build();
        client_v2.dropResourceGroup(request);

        ArgumentCaptor<DropResourceGroupRequest> captor = ArgumentCaptor.forClass(DropResourceGroupRequest.class);
        verify(blockingStub).dropResourceGroup(captor.capture());
        assertEquals("rg_1", captor.getValue().getResourceGroup());
    }

    @Test
    void testListResourceGroups() {
        Status successStatus = Status.newBuilder().setCode(0).build();
        when(blockingStub.listResourceGroups(any())).thenReturn(ListResourceGroupsResponse.newBuilder()
                .setStatus(successStatus)
                .addResourceGroups("__default_resource_group")
                .addResourceGroups("rg_1")
                .build());

        ListResourceGroupsResp resp = client_v2.listResourceGroups(ListResourceGroupsReq.builder().build());
        assertEquals(Arrays.asList("__default_resource_group", "rg_1"), resp.getGroupNames());

        verify(blockingStub).listResourceGroups(any());
    }

    @Test
    void testDescribeResourceGroup() {
        Status successStatus = Status.newBuilder().setCode(0).build();
        io.milvus.grpc.ResourceGroupConfig grpcConfig = io.milvus.grpc.ResourceGroupConfig.newBuilder()
                .setRequests(io.milvus.grpc.ResourceGroupLimit.newBuilder().setNodeNum(1))
                .setLimits(io.milvus.grpc.ResourceGroupLimit.newBuilder().setNodeNum(2))
                .build();
        ResourceGroup rgroup = ResourceGroup.newBuilder()
                .setName("rg_1")
                .setCapacity(4)
                .setNumAvailableNode(3)
                .putNumLoadedReplica("test", 2)
                .putNumOutgoingNode("rg_2", 1)
                .putNumIncomingNode("rg_3", 1)
                .setConfig(grpcConfig)
                .addNodes(NodeInfo.newBuilder().setNodeId(1L).setAddress("host1").setHostname("host1").build())
                .build();
        when(blockingStub.describeResourceGroup(any())).thenReturn(DescribeResourceGroupResponse.newBuilder()
                .setStatus(successStatus)
                .setResourceGroup(rgroup)
                .build());

        DescribeResourceGroupResp resp = client_v2.describeResourceGroup(DescribeResourceGroupReq.builder()
                .groupName("rg_1")
                .build());
        assertEquals("rg_1", resp.getGroupName());
        assertEquals(4, resp.getCapacity());
        assertEquals(3, resp.getNumberOfAvailableNode());
        assertEquals(2, resp.getNumberOfLoadedReplica().get("test"));
        assertEquals(1, resp.getNumberOfOutgoingNode().get("rg_2"));
        assertEquals(1, resp.getNumberOfIncomingNode().get("rg_3"));
        assertEquals(1, resp.getConfig().getRequests().getNodeNum());
        assertEquals(2, resp.getConfig().getLimits().getNodeNum());
        assertEquals(1, resp.getNodes().size());
        assertEquals("host1", resp.getNodes().get(0).getAddress());

        ArgumentCaptor<DescribeResourceGroupRequest> captor = ArgumentCaptor.forClass(DescribeResourceGroupRequest.class);
        verify(blockingStub).describeResourceGroup(captor.capture());
        assertEquals("rg_1", captor.getValue().getResourceGroup());
    }

    @Test
    void testTransferNode() {
        Status successStatus = Status.newBuilder().setCode(0).build();
        when(blockingStub.transferNode(any())).thenReturn(successStatus);

        TransferNodeReq request = TransferNodeReq.builder()
                .sourceGroupName("rg_1")
                .targetGroupName("rg_2")
                .numOfNodes(1)
                .build();
        client_v2.transferNode(request);

        ArgumentCaptor<TransferNodeRequest> captor = ArgumentCaptor.forClass(TransferNodeRequest.class);
        verify(blockingStub).transferNode(captor.capture());
        assertEquals("rg_1", captor.getValue().getSourceResourceGroup());
        assertEquals("rg_2", captor.getValue().getTargetResourceGroup());
        assertEquals(1, captor.getValue().getNumNode());
    }

    @Test
    void testTransferNodeWithoutSourceGroup() {
        TransferNodeReq request = TransferNodeReq.builder()
                .targetGroupName("rg_2")
                .numOfNodes(1)
                .build();
        assertThrows(MilvusClientException.class, () -> client_v2.transferNode(request));
    }

    @Test
    void testTransferReplica() {
        Status successStatus = Status.newBuilder().setCode(0).build();
        when(blockingStub.transferReplica(any())).thenReturn(successStatus);

        TransferReplicaReq request = TransferReplicaReq.builder()
                .sourceGroupName("rg_1")
                .targetGroupName("rg_2")
                .collectionName("test")
                .numberOfReplicas(2L)
                .build();
        client_v2.transferReplica(request);

        ArgumentCaptor<TransferReplicaRequest> captor = ArgumentCaptor.forClass(TransferReplicaRequest.class);
        verify(blockingStub).transferReplica(captor.capture());
        assertEquals("rg_1", captor.getValue().getSourceResourceGroup());
        assertEquals("rg_2", captor.getValue().getTargetResourceGroup());
        assertEquals("test", captor.getValue().getCollectionName());
        assertEquals(2L, captor.getValue().getNumReplica());
    }

    @Test
    void testTransferReplicaWithoutCollectionName() {
        TransferReplicaReq request = TransferReplicaReq.builder()
                .sourceGroupName("rg_1")
                .targetGroupName("rg_2")
                .build();
        assertThrows(MilvusClientException.class, () -> client_v2.transferReplica(request));
    }
}
