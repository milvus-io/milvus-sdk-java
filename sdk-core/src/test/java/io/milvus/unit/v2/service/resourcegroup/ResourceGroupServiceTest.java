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

package io.milvus.unit.v2.service.resourcegroup;

import io.milvus.common.resourcegroup.ResourceGroupConfig;
import io.milvus.common.resourcegroup.ResourceGroupLimit;
import io.milvus.common.resourcegroup.ResourceGroupTransfer;
import io.milvus.grpc.CreateResourceGroupRequest;
import io.milvus.grpc.DescribeResourceGroupRequest;
import io.milvus.grpc.DescribeResourceGroupResponse;
import io.milvus.grpc.DropResourceGroupRequest;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.ListResourceGroupsRequest;
import io.milvus.grpc.ListResourceGroupsResponse;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.NodeInfo;
import io.milvus.grpc.ResourceGroup;
import io.milvus.grpc.Status;
import io.milvus.grpc.TransferNodeRequest;
import io.milvus.grpc.TransferReplicaRequest;
import io.milvus.grpc.UpdateResourceGroupsRequest;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.resourcegroup.ResourceGroupService;
import io.milvus.v2.service.resourcegroup.request.CreateResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.DescribeResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.DropResourceGroupReq;
import io.milvus.v2.service.resourcegroup.request.ListResourceGroupsReq;
import io.milvus.v2.service.resourcegroup.request.TransferNodeReq;
import io.milvus.v2.service.resourcegroup.request.TransferReplicaReq;
import io.milvus.v2.service.resourcegroup.request.UpdateResourceGroupsReq;
import io.milvus.v2.service.resourcegroup.response.DescribeResourceGroupResp;
import io.milvus.v2.service.resourcegroup.response.ListResourceGroupsResp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class ResourceGroupServiceTest {
    private MilvusServiceGrpc.MilvusServiceBlockingStub stub;
    private ResourceGroupService service;

    @BeforeEach
    void setUp() {
        stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        service = new ResourceGroupService();
    }

    private static Status success() {
        return Status.newBuilder().setCode(0).setErrorCode(ErrorCode.Success).build();
    }

    private static ResourceGroupConfig config(int requests, int limits) {
        return ResourceGroupConfig.newBuilder()
                .withRequests(new ResourceGroupLimit(requests))
                .withLimits(new ResourceGroupLimit(limits))
                .withFrom(Collections.singletonList(new ResourceGroupTransfer("rg2")))
                .withTo(Collections.emptyList())
                .build();
    }

    @Test
    void createResourceGroupReturnsNull() {
        when(stub.createResourceGroup(any())).thenReturn(success());
        CreateResourceGroupReq request = CreateResourceGroupReq.builder()
                .groupName("rg")
                .config(config(2, 8))
                .build();

        assertNull(service.createResourceGroup(stub, request));

        ArgumentCaptor<CreateResourceGroupRequest> captor = ArgumentCaptor.forClass(CreateResourceGroupRequest.class);
        verify(stub).createResourceGroup(captor.capture());
        assertEquals("rg", captor.getValue().getResourceGroup());
        assertEquals(2, captor.getValue().getConfig().getRequests().getNodeNum());
    }

    @Test
    void createResourceGroupRejectsNullConfig() {
        CreateResourceGroupReq request = CreateResourceGroupReq.builder()
                .groupName("rg")
                .config(null)
                .build();

        assertThrows(MilvusClientException.class, () -> service.createResourceGroup(stub, request));
    }

    @Test
    void updateResourceGroupsReturnsNull() {
        when(stub.updateResourceGroups(any())).thenReturn(success());
        Map<String, ResourceGroupConfig> resourceGroups = new HashMap<>();
        resourceGroups.put("rg", config(2, 8));
        UpdateResourceGroupsReq request = UpdateResourceGroupsReq.builder()
                .resourceGroups(resourceGroups)
                .build();

        assertNull(service.updateResourceGroups(stub, request));
        verify(stub).updateResourceGroups(any(UpdateResourceGroupsRequest.class));
    }

    @Test
    void updateResourceGroupsRejectsEmptyMap() {
        UpdateResourceGroupsReq request = UpdateResourceGroupsReq.builder()
                .resourceGroups(Collections.emptyMap())
                .build();

        assertThrows(MilvusClientException.class, () -> service.updateResourceGroups(stub, request));
    }

    @Test
    void dropResourceGroupReturnsNull() {
        when(stub.dropResourceGroup(any())).thenReturn(success());
        DropResourceGroupReq request = DropResourceGroupReq.builder().groupName("rg").build();

        assertNull(service.dropResourceGroup(stub, request));

        ArgumentCaptor<DropResourceGroupRequest> captor = ArgumentCaptor.forClass(DropResourceGroupRequest.class);
        verify(stub).dropResourceGroup(captor.capture());
        assertEquals("rg", captor.getValue().getResourceGroup());
    }

    @Test
    void listResourceGroupsReturnsNames() {
        ListResourceGroupsResponse response = ListResourceGroupsResponse.newBuilder()
                .setStatus(success())
                .addAllResourceGroups(Arrays.asList("__default_resource_group", "rg"))
                .build();
        when(stub.listResourceGroups(any())).thenReturn(response);

        ListResourceGroupsResp result = service.listResourceGroups(stub,
                ListResourceGroupsReq.builder().build());

        assertEquals(Arrays.asList("__default_resource_group", "rg"), result.getGroupNames());
        verify(stub).listResourceGroups(any(ListResourceGroupsRequest.class));
    }

    @Test
    void describeResourceGroupMapsAllFields() {
        DescribeResourceGroupResponse response = DescribeResourceGroupResponse.newBuilder()
                .setStatus(success())
                .setResourceGroup(ResourceGroup.newBuilder()
                        .setName("rg")
                        .setCapacity(10)
                        .setNumAvailableNode(5)
                        .putNumLoadedReplica("coll", 2)
                        .putNumOutgoingNode("rg2", 1)
                        .putNumIncomingNode("rg3", 1)
                        .setConfig(io.milvus.grpc.ResourceGroupConfig.newBuilder()
                                .setRequests(io.milvus.grpc.ResourceGroupLimit.newBuilder().setNodeNum(2).build())
                                .setLimits(io.milvus.grpc.ResourceGroupLimit.newBuilder().setNodeNum(8).build())
                                .build())
                        .addNodes(NodeInfo.newBuilder()
                                .setNodeId(11L)
                                .setAddress("addr")
                                .setHostname("host")
                                .build())
                        .build())
                .build();
        when(stub.describeResourceGroup(any())).thenReturn(response);

        DescribeResourceGroupResp result = service.describeResourceGroup(stub,
                DescribeResourceGroupReq.builder().groupName("rg").build());

        assertEquals("rg", result.getGroupName());
        assertEquals(10, result.getCapacity());
        assertEquals(5, result.getNumberOfAvailableNode());
        assertEquals(2, result.getNumberOfLoadedReplica().get("coll"));
        assertEquals(1, result.getNumberOfOutgoingNode().get("rg2"));
        assertEquals(1, result.getNumberOfIncomingNode().get("rg3"));
        assertEquals(2, result.getConfig().getRequests().getNodeNum());
        assertEquals(1, result.getNodes().size());
        assertEquals(11L, result.getNodes().get(0).getNodeId());
        assertEquals("addr", result.getNodes().get(0).getAddress());
        verify(stub).describeResourceGroup(any(DescribeResourceGroupRequest.class));
    }

    @Test
    void transferNodeReturnsNull() {
        when(stub.transferNode(any())).thenReturn(success());
        TransferNodeReq request = TransferNodeReq.builder()
                .sourceGroupName("rg1")
                .targetGroupName("rg2")
                .numOfNodes(2)
                .build();

        assertNull(service.transferNode(stub, request));

        ArgumentCaptor<TransferNodeRequest> captor = ArgumentCaptor.forClass(TransferNodeRequest.class);
        verify(stub).transferNode(captor.capture());
        assertEquals("rg1", captor.getValue().getSourceResourceGroup());
        assertEquals("rg2", captor.getValue().getTargetResourceGroup());
        assertEquals(2, captor.getValue().getNumNode());
    }

    @Test
    void transferNodeRejectsEmptySourceGroup() {
        TransferNodeReq request = TransferNodeReq.builder()
                .sourceGroupName("")
                .targetGroupName("rg2")
                .numOfNodes(2)
                .build();

        assertThrows(MilvusClientException.class, () -> service.transferNode(stub, request));
    }

    @Test
    void transferNodeRejectsEmptyTargetGroup() {
        TransferNodeReq request = TransferNodeReq.builder()
                .sourceGroupName("rg1")
                .targetGroupName(null)
                .numOfNodes(2)
                .build();

        assertThrows(MilvusClientException.class, () -> service.transferNode(stub, request));
    }

    @Test
    void transferReplicaReturnsNull() {
        when(stub.transferReplica(any())).thenReturn(success());
        TransferReplicaReq request = TransferReplicaReq.builder()
                .sourceGroupName("rg1")
                .targetGroupName("rg2")
                .collectionName("coll")
                .databaseName("db")
                .numberOfReplicas(1L)
                .build();

        assertNull(service.transferReplica(stub, request));

        ArgumentCaptor<TransferReplicaRequest> captor = ArgumentCaptor.forClass(TransferReplicaRequest.class);
        verify(stub).transferReplica(captor.capture());
        assertEquals("coll", captor.getValue().getCollectionName());
        assertEquals("db", captor.getValue().getDbName());
    }

    @Test
    void transferReplicaRejectsEmptyCollection() {
        TransferReplicaReq request = TransferReplicaReq.builder()
                .sourceGroupName("rg1")
                .targetGroupName("rg2")
                .collectionName(null)
                .build();

        assertThrows(MilvusClientException.class, () -> service.transferReplica(stub, request));
    }
}
