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

package io.milvus.unit.v2.service.snapshot;

import io.milvus.grpc.DescribeSnapshotResponse;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.GetRestoreSnapshotStateResponse;
import io.milvus.grpc.ListRestoreSnapshotJobsResponse;
import io.milvus.grpc.ListSnapshotsResponse;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.PinSnapshotDataResponse;
import io.milvus.grpc.RestoreSnapshotResponse;
import io.milvus.grpc.RestoreSnapshotInfo;
import io.milvus.grpc.Status;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.snapshot.SnapshotService;
import io.milvus.v2.service.snapshot.request.CreateSnapshotReq;
import io.milvus.v2.service.snapshot.request.DescribeSnapshotReq;
import io.milvus.v2.service.snapshot.request.DropSnapshotReq;
import io.milvus.v2.service.snapshot.request.GetRestoreSnapshotStateReq;
import io.milvus.v2.service.snapshot.request.ListRestoreSnapshotJobsReq;
import io.milvus.v2.service.snapshot.request.ListSnapshotsReq;
import io.milvus.v2.service.snapshot.request.PinSnapshotDataReq;
import io.milvus.v2.service.snapshot.request.RestoreSnapshotReq;
import io.milvus.v2.service.snapshot.request.UnpinSnapshotDataReq;
import io.milvus.v2.service.snapshot.response.DescribeSnapshotResp;
import io.milvus.v2.service.snapshot.response.GetRestoreSnapshotStateResp;
import io.milvus.v2.service.snapshot.response.ListRestoreSnapshotJobsResp;
import io.milvus.v2.service.snapshot.response.ListSnapshotsResp;
import io.milvus.v2.service.snapshot.response.PinSnapshotDataResp;
import io.milvus.v2.service.snapshot.response.RestoreSnapshotResp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class SnapshotServiceTest {
    private MilvusServiceGrpc.MilvusServiceBlockingStub stub;
    private SnapshotService service;

    @BeforeEach
    void setUp() {
        stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        service = new SnapshotService();
    }

    private static Status success() {
        return Status.newBuilder().setCode(0).setErrorCode(ErrorCode.Success).build();
    }

    @Test
    void createSnapshotReturnsNull() {
        when(stub.createSnapshot(any())).thenReturn(success());

        assertNull(service.createSnapshot(stub, CreateSnapshotReq.builder()
                .snapshotName("snap1")
                .collectionName("coll")
                .databaseName("db")
                .description("desc")
                .compactionProtectionSeconds(100L)
                .build()));
        verify(stub).createSnapshot(any());
    }

    @Test
    void createSnapshotRejectsEmptyName() {
        CreateSnapshotReq request = CreateSnapshotReq.builder()
                .snapshotName("")
                .collectionName("coll")
                .build();

        assertThrows(MilvusClientException.class, () -> service.createSnapshot(stub, request));
    }

    @Test
    void createSnapshotRejectsNegativeProtectionSeconds() {
        CreateSnapshotReq request = CreateSnapshotReq.builder()
                .snapshotName("snap1")
                .collectionName("coll")
                .compactionProtectionSeconds(-1L)
                .build();

        assertThrows(MilvusClientException.class, () -> service.createSnapshot(stub, request));
    }

    @Test
    void dropSnapshotReturnsNull() {
        when(stub.dropSnapshot(any())).thenReturn(success());

        assertNull(service.dropSnapshot(stub, DropSnapshotReq.builder()
                .snapshotName("snap1")
                .collectionName("coll")
                .build()));
        verify(stub).dropSnapshot(any());
    }

    @Test
    void dropSnapshotRejectsEmptyCollection() {
        DropSnapshotReq request = DropSnapshotReq.builder()
                .snapshotName("snap1")
                .collectionName(null)
                .build();

        assertThrows(MilvusClientException.class, () -> service.dropSnapshot(stub, request));
    }

    @Test
    void listSnapshotsReturnsNames() {
        ListSnapshotsResponse response = ListSnapshotsResponse.newBuilder()
                .setStatus(success())
                .addAllSnapshots(Collections.singletonList("snap1"))
                .build();
        when(stub.listSnapshots(any())).thenReturn(response);

        ListSnapshotsResp result = service.listSnapshots(stub, ListSnapshotsReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .build());

        assertEquals(Collections.singletonList("snap1"), result.getSnapshots());
        verify(stub).listSnapshots(any());
    }

    @Test
    void describeSnapshotReturnsMetadata() {
        DescribeSnapshotResponse response = DescribeSnapshotResponse.newBuilder()
                .setStatus(success())
                .setName("snap1")
                .setDescription("desc")
                .setCollectionName("coll")
                .addAllPartitionNames(Collections.singletonList("p"))
                .setCreateTs(100L)
                .setS3Location("s3://bucket")
                .build();
        when(stub.describeSnapshot(any())).thenReturn(response);

        DescribeSnapshotResp result = service.describeSnapshot(stub, DescribeSnapshotReq.builder()
                .snapshotName("snap1")
                .collectionName("coll")
                .build());

        assertEquals("snap1", result.getName());
        assertEquals("desc", result.getDescription());
        assertEquals("coll", result.getCollectionName());
        assertEquals(Collections.singletonList("p"), result.getPartitionNames());
        assertEquals(100L, result.getCreateTs());
        assertEquals("s3://bucket", result.getS3Location());
    }

    @Test
    void restoreSnapshotReturnsJobId() {
        RestoreSnapshotResponse response = RestoreSnapshotResponse.newBuilder()
                .setStatus(success())
                .setJobId(9L)
                .build();
        when(stub.restoreSnapshot(any())).thenReturn(response);

        RestoreSnapshotResp result = service.restoreSnapshot(stub, RestoreSnapshotReq.builder()
                .snapshotName("snap1")
                .sourceCollectionName("coll")
                .targetCollectionName("coll2")
                .sourceDbName("db")
                .targetDbName("db2")
                .build());

        assertEquals(9L, result.getJobId());
        verify(stub).restoreSnapshot(any());
    }

    @Test
    void restoreSnapshotRejectsEmptySourceCollection() {
        RestoreSnapshotReq request = RestoreSnapshotReq.builder()
                .snapshotName("snap1")
                .sourceCollectionName("")
                .targetCollectionName("coll2")
                .build();

        assertThrows(MilvusClientException.class, () -> service.restoreSnapshot(stub, request));
    }

    @Test
    void getRestoreSnapshotStateReturnsJobInfo() {
        GetRestoreSnapshotStateResponse response = GetRestoreSnapshotStateResponse.newBuilder()
                .setStatus(success())
                .setInfo(RestoreSnapshotInfo.newBuilder()
                        .setJobId(9L)
                        .setSnapshotName("snap1")
                        .setDbName("db")
                        .setCollectionName("coll")
                        .setState(io.milvus.grpc.RestoreSnapshotState.RestoreSnapshotExecuting)
                        .setProgress(50)
                        .setReason("")
                        .setStartTime(1L)
                        .setTimeCost(2L)
                        .build())
                .build();
        when(stub.getRestoreSnapshotState(any())).thenReturn(response);

        GetRestoreSnapshotStateResp result = service.getRestoreSnapshotState(stub,
                GetRestoreSnapshotStateReq.builder().jobId(9L).build());

        assertEquals(9L, result.getJobInfo().getJobId());
        assertEquals("snap1", result.getJobInfo().getSnapshotName());
        assertEquals("RestoreSnapshotExecuting", result.getJobInfo().getState());
        assertEquals(50, result.getJobInfo().getProgress());
        verify(stub).getRestoreSnapshotState(any());
    }

    @Test
    void getRestoreSnapshotStateRejectsNonPositiveJobId() {
        GetRestoreSnapshotStateReq request = GetRestoreSnapshotStateReq.builder().jobId(0L).build();

        assertThrows(MilvusClientException.class, () -> service.getRestoreSnapshotState(stub, request));
    }

    @Test
    void listRestoreSnapshotJobsReturnsJobs() {
        ListRestoreSnapshotJobsResponse response = ListRestoreSnapshotJobsResponse.newBuilder()
                .setStatus(success())
                .addJobs(RestoreSnapshotInfo.newBuilder()
                        .setJobId(9L)
                        .setSnapshotName("snap1")
                        .setCollectionName("coll")
                        .build())
                .build();
        when(stub.listRestoreSnapshotJobs(any())).thenReturn(response);

        ListRestoreSnapshotJobsResp result = service.listRestoreSnapshotJobs(stub,
                ListRestoreSnapshotJobsReq.builder()
                        .collectionName("coll")
                        .databaseName("db")
                        .build());

        assertEquals(1, result.getJobs().size());
        assertEquals(9L, result.getJobs().get(0).getJobId());
        verify(stub).listRestoreSnapshotJobs(any());
    }

    @Test
    void pinSnapshotDataReturnsPinId() {
        PinSnapshotDataResponse response = PinSnapshotDataResponse.newBuilder()
                .setStatus(success())
                .setPinId(5L)
                .build();
        when(stub.pinSnapshotData(any())).thenReturn(response);

        PinSnapshotDataResp result = service.pinSnapshotData(stub, PinSnapshotDataReq.builder()
                .snapshotName("snap1")
                .collectionName("coll")
                .databaseName("db")
                .ttlSeconds(60L)
                .build());

        assertEquals(5L, result.getPinId());
        verify(stub).pinSnapshotData(any());
    }

    @Test
    void pinSnapshotDataRejectsNegativeTtl() {
        PinSnapshotDataReq request = PinSnapshotDataReq.builder()
                .snapshotName("snap1")
                .collectionName("coll")
                .ttlSeconds(-1L)
                .build();

        assertThrows(MilvusClientException.class, () -> service.pinSnapshotData(stub, request));
    }

    @Test
    void unpinSnapshotDataReturnsNull() {
        when(stub.unpinSnapshotData(any())).thenReturn(success());

        assertNull(service.unpinSnapshotData(stub, UnpinSnapshotDataReq.builder().pinId(5L).build()));
        verify(stub).unpinSnapshotData(any());
    }

    @Test
    void unpinSnapshotDataRejectsNonPositivePinId() {
        UnpinSnapshotDataReq request = UnpinSnapshotDataReq.builder().pinId(-1L).build();

        assertThrows(MilvusClientException.class, () -> service.unpinSnapshotData(stub, request));
    }
}
