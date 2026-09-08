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

package io.milvus.unit.v2.service.partition;

import io.milvus.grpc.BoolResponse;
import io.milvus.grpc.CreatePartitionRequest;
import io.milvus.grpc.DropPartitionRequest;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.GetPartitionStatisticsRequest;
import io.milvus.grpc.GetPartitionStatisticsResponse;
import io.milvus.grpc.HasPartitionRequest;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.LoadPartitionsRequest;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.ReleasePartitionsRequest;
import io.milvus.grpc.ShowPartitionsRequest;
import io.milvus.grpc.ShowPartitionsResponse;
import io.milvus.grpc.Status;
import java.util.concurrent.TimeUnit;
import io.milvus.v2.service.partition.PartitionService;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.DropPartitionReq;
import io.milvus.v2.service.partition.request.GetPartitionStatsReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import io.milvus.v2.service.partition.request.ListPartitionsReq;
import io.milvus.v2.service.partition.request.LoadPartitionsReq;
import io.milvus.v2.service.partition.request.ReleasePartitionsReq;
import io.milvus.v2.service.partition.response.GetPartitionStatsResp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class PartitionServiceTest {
    private MilvusServiceGrpc.MilvusServiceBlockingStub stub;
    private PartitionService service;

    @BeforeEach
    void setUp() {
        stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.withDeadlineAfter(anyLong(), any(TimeUnit.class))).thenReturn(stub);
        service = new PartitionService();
        service.setEndpoint("host:19530");
        service.setCurrentDbName("db");
    }

    private static Status success() {
        return Status.newBuilder().setCode(0).setErrorCode(ErrorCode.Success).build();
    }

    @Test
    void createPartitionReturnsNull() {
        when(stub.createPartition(any())).thenReturn(success());
        CreatePartitionReq request = CreatePartitionReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .partitionName("p")
                .build();

        assertNull(service.createPartition(stub, request));

        ArgumentCaptor<CreatePartitionRequest> captor = ArgumentCaptor.forClass(CreatePartitionRequest.class);
        verify(stub).createPartition(captor.capture());
        assertEquals("coll", captor.getValue().getCollectionName());
        assertEquals("p", captor.getValue().getPartitionName());
        assertEquals("db", captor.getValue().getDbName());
    }

    @Test
    void dropPartitionReturnsNull() {
        when(stub.dropPartition(any())).thenReturn(success());
        DropPartitionReq request = DropPartitionReq.builder()
                .collectionName("coll")
                .partitionName("p")
                .build();

        assertNull(service.dropPartition(stub, request));

        ArgumentCaptor<DropPartitionRequest> captor = ArgumentCaptor.forClass(DropPartitionRequest.class);
        verify(stub).dropPartition(captor.capture());
        assertEquals("p", captor.getValue().getPartitionName());
    }

    @Test
    void hasPartitionReturnsValue() {
        BoolResponse response = BoolResponse.newBuilder().setStatus(success()).setValue(true).build();
        when(stub.hasPartition(any())).thenReturn(response);
        HasPartitionReq request = HasPartitionReq.builder()
                .collectionName("coll")
                .partitionName("p")
                .build();

        assertTrue(service.hasPartition(stub, request));
        verify(stub).hasPartition(any(HasPartitionRequest.class));
    }

    @Test
    void hasPartitionReturnsFalseWhenAbsent() {
        BoolResponse response = BoolResponse.newBuilder().setStatus(success()).setValue(false).build();
        when(stub.hasPartition(any())).thenReturn(response);
        HasPartitionReq request = HasPartitionReq.builder()
                .collectionName("coll")
                .partitionName("p")
                .build();

        assertFalse(service.hasPartition(stub, request));
    }

    @Test
    void listPartitionsReturnsNames() {
        ShowPartitionsResponse response = ShowPartitionsResponse.newBuilder()
                .setStatus(success())
                .addAllPartitionNames(Arrays.asList("_default", "p1"))
                .build();
        when(stub.showPartitions(any())).thenReturn(response);
        ListPartitionsReq request = ListPartitionsReq.builder()
                .collectionName("coll")
                .build();

        assertEquals(Arrays.asList("_default", "p1"), service.listPartitions(stub, request));
        verify(stub).showPartitions(any(ShowPartitionsRequest.class));
    }

    @Test
    void getPartitionStatsReturnsRowCount() {
        GetPartitionStatisticsResponse response = GetPartitionStatisticsResponse.newBuilder()
                .setStatus(success())
                .addStats(KeyValuePair.newBuilder().setKey("row_count").setValue("42").build())
                .build();
        when(stub.getPartitionStatistics(any())).thenReturn(response);
        GetPartitionStatsReq request = GetPartitionStatsReq.builder()
                .collectionName("coll")
                .partitionName("p")
                .build();

        GetPartitionStatsResp result = service.getPartitionStats(stub, request);

        assertEquals(42L, result.getNumOfEntities());
        assertEquals("42", result.getStats().get("row_count"));
        verify(stub).getPartitionStatistics(any(GetPartitionStatisticsRequest.class));
    }

    @Test
    void loadPartitionsReturnsNullWithoutSync() {
        when(stub.loadPartitions(any())).thenReturn(success());
        LoadPartitionsReq request = LoadPartitionsReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .partitionNames(Collections.singletonList("p"))
                .numReplicas(1)
                .loadFields(Collections.emptyList())
                .resourceGroups(Collections.emptyList())
                .sync(false)
                .build();

        assertNull(service.loadPartitions(stub, request));

        ArgumentCaptor<LoadPartitionsRequest> captor = ArgumentCaptor.forClass(LoadPartitionsRequest.class);
        verify(stub).loadPartitions(captor.capture());
        assertEquals(1, captor.getValue().getReplicaNumber());
        assertEquals("db", captor.getValue().getDbName());
    }

    @Test
    void releasePartitionsReturnsNull() {
        when(stub.releasePartitions(any())).thenReturn(success());
        ReleasePartitionsReq request = ReleasePartitionsReq.builder()
                .collectionName("coll")
                .partitionNames(Collections.singletonList("p"))
                .build();

        assertNull(service.releasePartitions(stub, request));

        ArgumentCaptor<ReleasePartitionsRequest> captor = ArgumentCaptor.forClass(ReleasePartitionsRequest.class);
        verify(stub).releasePartitions(captor.capture());
        assertEquals("p", captor.getValue().getPartitionNames(0));
    }
}
