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

package io.milvus.unit.v2.service.utility;

import com.google.gson.JsonObject;
import io.milvus.grpc.CheckHealthResponse;
import io.milvus.grpc.CompactionMergeInfo;
import io.milvus.grpc.CompactionState;
import io.milvus.grpc.ConnectResponse;
import io.milvus.grpc.DescribeAliasResponse;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.FileResourceInfo;
import io.milvus.grpc.FlushAllResponse;
import io.milvus.grpc.FlushResponse;
import io.milvus.grpc.GetCompactionPlansResponse;
import io.milvus.grpc.GetCompactionStateResponse;
import io.milvus.grpc.GetFlushAllStateResponse;
import io.milvus.grpc.GetFlushStateResponse;
import io.milvus.grpc.GetPersistentSegmentInfoResponse;
import io.milvus.grpc.GetQuerySegmentInfoResponse;
import io.milvus.grpc.GetRefreshExternalCollectionProgressResponse;
import io.milvus.grpc.GetVersionResponse;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.ListAliasesResponse;
import io.milvus.grpc.ListFileResourcesResponse;
import io.milvus.grpc.ListRefreshExternalCollectionJobsResponse;
import io.milvus.grpc.ManualCompactionResponse;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.PersistentSegmentInfo;
import io.milvus.grpc.QuerySegmentInfo;
import io.milvus.grpc.QuotaState;
import io.milvus.grpc.RefreshExternalCollectionJobInfo;
import io.milvus.grpc.RefreshExternalCollectionResponse;
import io.milvus.grpc.SegmentLevel;
import io.milvus.grpc.SegmentState;
import io.milvus.grpc.ServerInfo;
import io.milvus.grpc.Status;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.utility.UtilityService;
import io.milvus.v2.service.utility.request.AddFileResourceReq;
import io.milvus.v2.service.utility.request.AlterAliasReq;
import io.milvus.v2.service.utility.request.CompactReq;
import io.milvus.v2.service.utility.request.CreateAliasReq;
import io.milvus.v2.service.utility.request.DescribeAliasReq;
import io.milvus.v2.service.utility.request.DropAliasReq;
import io.milvus.v2.service.utility.request.FlushAllReq;
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.service.utility.request.GetCompactionPlansReq;
import io.milvus.v2.service.utility.request.GetCompactionStateReq;
import io.milvus.v2.service.utility.request.GetFlushAllStateReq;
import io.milvus.v2.service.utility.request.GetPersistentSegmentInfoReq;
import io.milvus.v2.service.utility.request.GetQuerySegmentInfoReq;
import io.milvus.v2.service.utility.request.GetRefreshExternalCollectionProgressReq;
import io.milvus.v2.service.utility.request.GetServerVersionReq;
import io.milvus.v2.service.utility.request.ListAliasesReq;
import io.milvus.v2.service.utility.request.ListFileResourcesReq;
import io.milvus.v2.service.utility.request.ListRefreshExternalCollectionJobsReq;
import io.milvus.v2.service.utility.request.RefreshExternalCollectionReq;
import io.milvus.v2.service.utility.request.RemoveFileResourceReq;
import io.milvus.v2.service.utility.response.CheckHealthResp;
import io.milvus.v2.service.utility.response.CompactResp;
import io.milvus.v2.service.utility.response.DescribeAliasResp;
import io.milvus.v2.service.utility.response.FlushAllResp;
import io.milvus.v2.service.utility.response.FlushResp;
import io.milvus.v2.service.utility.response.GetCompactionPlansResp;
import io.milvus.v2.service.utility.response.GetCompactionStateResp;
import io.milvus.v2.service.utility.response.GetFlushAllStateResp;
import io.milvus.v2.service.utility.response.GetPersistentSegmentInfoResp;
import io.milvus.v2.service.utility.response.GetQuerySegmentInfoResp;
import io.milvus.v2.service.utility.response.GetServerVersionResp;
import io.milvus.v2.service.utility.response.ListAliasResp;
import io.milvus.v2.service.utility.response.ListFileResourcesResp;
import io.milvus.v2.service.utility.response.RefreshExternalCollectionResp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class UtilityServiceTest {
    private MilvusServiceGrpc.MilvusServiceBlockingStub stub;
    private UtilityService service;

    @BeforeEach
    void setUp() {
        stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        service = new UtilityService();
        service.setEndpoint("host:19530");
        service.setCurrentDbName("db");
    }

    @AfterEach
    void tearDown() {
        io.milvus.common.utils.cache.SchemaCache.getInstance().clear();
        io.milvus.common.utils.cache.CollectionTsCache.getInstance().clear();
    }

    private static Status success() {
        return Status.newBuilder().setCode(0).setErrorCode(ErrorCode.Success).build();
    }

    @Test
    void getServerVersionWithoutDetail() {
        GetVersionResponse response = GetVersionResponse.newBuilder()
                .setStatus(success())
                .setVersion("v2.5.0")
                .build();
        when(stub.getVersion(any())).thenReturn(response);

        GetServerVersionResp result = service.getServerVersion(stub,
                GetServerVersionReq.builder().build());

        assertEquals("v2.5.0", result.getVersion());
    }

    @Test
    void getServerVersionWithDetail() {
        ConnectResponse response = ConnectResponse.newBuilder()
                .setStatus(success())
                .setServerInfo(ServerInfo.newBuilder()
                        .setBuildTags("v2.5.0")
                        .setBuildTime("2024-01-01")
                        .setGitCommit("abc")
                        .setGoVersion("1.21")
                        .setDeployMode("standalone")
                        .build())
                .build();
        when(stub.connect(any())).thenReturn(response);

        GetServerVersionResp result = service.getServerVersion(stub,
                GetServerVersionReq.builder().detail(true).build());

        assertEquals("v2.5.0", result.getVersion());
        assertEquals("2024-01-01", result.getBuildTime());
        assertEquals("abc", result.getGitCommit());
        assertEquals("1.21", result.getGoVersion());
        assertEquals("standalone", result.getDeployMode());
    }

    @Test
    void flushReturnsSegmentIdsAndTs() {
        FlushResponse response = FlushResponse.newBuilder()
                .setStatus(success())
                .setDbName("db")
                .putCollSegIDs("coll", LongArray.newBuilder().addData(1L).addData(2L).build())
                .putCollFlushTs("coll", 100L)
                .build();
        when(stub.flush(any())).thenReturn(response);

        FlushResp result = service.flush(stub, FlushReq.builder()
                .collectionNames(Collections.singletonList("coll"))
                .databaseName("db")
                .build());

        assertEquals("db", result.getDatabaseName());
        assertEquals(2, result.getCollectionSegmentIDs().get("coll").size());
        assertEquals(100L, result.getCollectionFlushTs().get("coll"));
    }

    @Test
    void flushRejectsEmptyCollectionNames() {
        FlushReq request = FlushReq.builder().collectionNames(Collections.emptyList()).build();

        assertThrows(MilvusClientException.class, () -> service.flush(stub, request));
    }

    @Test
    void flushAllReturnsTs() {
        FlushAllResponse response = FlushAllResponse.newBuilder()
                .setStatus(success())
                .setFlushAllTs(123L)
                .build();
        when(stub.flushAll(any())).thenReturn(response);

        FlushAllResp result = service.flushAll(stub, FlushAllReq.builder().databaseName("db").build());

        assertEquals(123L, result.getFlushAllTs());
    }

    @Test
    void getFlushAllStateReturnsFlushed() {
        GetFlushAllStateResponse response = GetFlushAllStateResponse.newBuilder()
                .setStatus(success())
                .setFlushed(true)
                .build();
        when(stub.getFlushAllState(any())).thenReturn(response);

        GetFlushAllStateResp result = service.getFlushAllState(stub,
                GetFlushAllStateReq.builder().databaseName("db").flushAllTs(1L).build());

        assertTrue(result.getFlushed());
    }

    @Test
    void waitFlushReturnsNullWhenAlreadyFlushed() {
        GetFlushStateResponse flushed = GetFlushStateResponse.newBuilder().setFlushed(true).build();
        when(stub.getFlushState(any())).thenReturn(flushed);
        Map<String, List<Long>> segmentIds = new HashMap<>();
        segmentIds.put("coll", Collections.singletonList(1L));
        Map<String, Long> flushTs = new HashMap<>();
        flushTs.put("coll", 100L);
        FlushResp flushResp = FlushResp.builder()
                .databaseName("db")
                .collectionSegmentIDs(segmentIds)
                .collectionFlushTs(flushTs)
                .build();

        assertNull(service.waitFlush(stub, flushResp));
        verify(stub).getFlushState(any());
    }

    @Test
    void waitFlushAllReturnsNullWhenAlreadyFlushed() {
        GetFlushAllStateResponse response = GetFlushAllStateResponse.newBuilder()
                .setStatus(success())
                .setFlushed(true)
                .build();
        when(stub.getFlushAllState(any())).thenReturn(response);
        FlushAllResp flushAllResp = FlushAllResp.builder().flushAllTs(1L).build();
        FlushAllReq request = FlushAllReq.builder().databaseName("db").build();

        assertNull(service.waitFlushAll(stub, flushAllResp, request));
    }

    @Test
    void compactReturnsCompactionId() {
        DescribeCollectionResponse desc = DescribeCollectionResponse.newBuilder()
                .setStatus(success())
                .setCollectionID(42L)
                .build();
        ManualCompactionResponse compact = ManualCompactionResponse.newBuilder()
                .setStatus(success())
                .setCompactionID(7L)
                .build();
        when(stub.describeCollection(any())).thenReturn(desc);
        when(stub.manualCompaction(any())).thenReturn(compact);

        CompactResp result = service.compact(stub, CompactReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .build());

        assertEquals(7L, result.getCompactionID());
        verify(stub).describeCollection(any());
        verify(stub).manualCompaction(any());
    }

    @Test
    void getCompactionStateMapsState() {
        GetCompactionStateResponse response = GetCompactionStateResponse.newBuilder()
                .setStatus(success())
                .setState(CompactionState.Executing)
                .setExecutingPlanNo(1L)
                .setTimeoutPlanNo(0L)
                .setCompletedPlanNo(0L)
                .build();
        when(stub.getCompactionState(any())).thenReturn(response);

        GetCompactionStateResp result = service.getCompactionState(stub,
                GetCompactionStateReq.builder().compactionID(1L).build());

        assertEquals(io.milvus.v2.common.CompactionState.Executing, result.getState());
        assertEquals(1L, result.getExecutingPlanNo());
    }

    @Test
    void getCompactionPlansMapsPlans() {
        GetCompactionPlansResponse response = GetCompactionPlansResponse.newBuilder()
                .setStatus(success())
                .setState(CompactionState.Completed)
                .addMergeInfos(CompactionMergeInfo.newBuilder()
                        .setTarget(1L)
                        .addSources(2L)
                        .build())
                .build();
        when(stub.getCompactionStateWithPlans(any())).thenReturn(response);

        GetCompactionPlansResp result = service.getCompactionPlans(stub,
                GetCompactionPlansReq.builder().compactionID(1L).build());

        assertEquals(1L, result.getCompactionId());
        assertEquals(io.milvus.v2.common.CompactionState.Completed, result.getState());
        assertEquals(1, result.getPlans().size());
        assertEquals(1L, result.getPlans().get(0).getTarget());
    }

    @Test
    void createAliasReturnsNull() {
        when(stub.createAlias(any())).thenReturn(success());

        assertNull(service.createAlias(stub, CreateAliasReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .alias("alias1")
                .build()));
        verify(stub).createAlias(any());
    }

    @Test
    void dropAliasReturnsNull() {
        when(stub.dropAlias(any())).thenReturn(success());

        assertNull(service.dropAlias(stub, DropAliasReq.builder()
                .alias("alias1")
                .databaseName("db")
                .build()));
        verify(stub).dropAlias(any());
    }

    @Test
    void alterAliasReturnsNull() {
        when(stub.alterAlias(any())).thenReturn(success());

        assertNull(service.alterAlias(stub, AlterAliasReq.builder()
                .alias("alias1")
                .collectionName("coll")
                .databaseName("db")
                .build()));
        verify(stub).alterAlias(any());
    }

    @Test
    void describeAliasReturnsAlias() {
        DescribeAliasResponse response = DescribeAliasResponse.newBuilder()
                .setStatus(success())
                .setDbName("db")
                .setCollection("coll")
                .setAlias("alias1")
                .build();
        when(stub.describeAlias(any())).thenReturn(response);

        DescribeAliasResp result = service.describeAlias(stub,
                DescribeAliasReq.builder().alias("alias1").databaseName("db").build());

        assertEquals("db", result.getDatabaseName());
        assertEquals("coll", result.getCollectionName());
        assertEquals("alias1", result.getAlias());
    }

    @Test
    void listAliasesReturnsAlias() {
        ListAliasesResponse response = ListAliasesResponse.newBuilder()
                .setStatus(success())
                .setCollectionName("coll")
                .setDbName("db")
                .addAllAliases(Collections.singletonList("alias1"))
                .build();
        when(stub.listAliases(any())).thenReturn(response);

        ListAliasResp result = service.listAliases(stub,
                ListAliasesReq.builder().collectionName("coll").databaseName("db").build());

        assertEquals("coll", result.getCollectionName());
        assertEquals("db", result.getDbName());
        assertEquals(Collections.singletonList("alias1"), result.getAlias());
    }

    @Test
    void checkHealthMapsHealthy() {
        CheckHealthResponse response = CheckHealthResponse.newBuilder()
                .setStatus(success())
                .setIsHealthy(true)
                .addReasons("ok")
                .addQuotaStates(QuotaState.ReadLimited)
                .build();
        when(stub.checkHealth(any())).thenReturn(response);

        CheckHealthResp result = service.checkHealth(stub);

        assertTrue(result.getIsHealthy());
        assertEquals(Collections.singletonList("ok"), result.getReasons());
        assertEquals(Collections.singletonList("ReadLimited"), result.getQuotaStates());
    }

    @Test
    void getPersistentSegmentInfoMapsSegments() {
        GetPersistentSegmentInfoResponse response = GetPersistentSegmentInfoResponse.newBuilder()
                .setStatus(success())
                .addInfos(PersistentSegmentInfo.newBuilder()
                        .setSegmentID(1L)
                        .setCollectionID(2L)
                        .setPartitionID(3L)
                        .setNumRows(10L)
                        .setState(SegmentState.Flushed)
                        .setLevel(SegmentLevel.L1)
                        .setStorageVersion(5L)
                        .setIsSorted(true)
                        .build())
                .build();
        when(stub.getPersistentSegmentInfo(any())).thenReturn(response);

        GetPersistentSegmentInfoResp result = service.getPersistentSegmentInfo(stub,
                GetPersistentSegmentInfoReq.builder().collectionName("coll").build());

        assertEquals(1, result.getSegmentInfos().size());
        GetPersistentSegmentInfoResp.PersistentSegmentInfo info = result.getSegmentInfos().get(0);
        assertEquals(1L, info.getSegmentID());
        assertEquals("Flushed", info.getState());
        assertEquals("L1", info.getLevel());
    }

    @Test
    void getQuerySegmentInfoMapsSegments() {
        GetQuerySegmentInfoResponse response = GetQuerySegmentInfoResponse.newBuilder()
                .setStatus(success())
                .addInfos(QuerySegmentInfo.newBuilder()
                        .setSegmentID(1L)
                        .setCollectionID(2L)
                        .setPartitionID(3L)
                        .setMemSize(1024L)
                        .setNumRows(10L)
                        .setIndexName("idx")
                        .setIndexID(9L)
                        .setState(SegmentState.Sealed)
                        .setLevel(SegmentLevel.L2)
                        .addAllNodeIds(Collections.singletonList(7L))
                        .build())
                .build();
        when(stub.getQuerySegmentInfo(any())).thenReturn(response);

        GetQuerySegmentInfoResp result = service.getQuerySegmentInfo(stub,
                GetQuerySegmentInfoReq.builder().collectionName("coll").build());

        assertEquals(1, result.getSegmentInfos().size());
        GetQuerySegmentInfoResp.QuerySegmentInfo info = result.getSegmentInfos().get(0);
        assertEquals("idx", info.getIndexName());
        assertEquals("Sealed", info.getState());
        assertEquals(Collections.singletonList(7L), info.getNodeIDs());
    }

    @Test
    void refreshExternalCollectionReturnsJobId() {
        RefreshExternalCollectionResponse response = RefreshExternalCollectionResponse.newBuilder()
                .setStatus(success())
                .setJobId(11L)
                .build();
        when(stub.refreshExternalCollection(any())).thenReturn(response);
        JsonObject spec = new JsonObject();
        spec.addProperty("path", "/data");

        RefreshExternalCollectionResp result = service.refreshExternalCollection(stub,
                RefreshExternalCollectionReq.builder()
                        .collectionName("coll")
                        .databaseName("db")
                        .externalSource("file")
                        .externalSpec(spec)
                        .build());

        assertEquals(11L, result.getJobId());
        verify(stub).refreshExternalCollection(any());
    }

    @Test
    void getRefreshExternalCollectionProgressReturnsJobInfo() {
        GetRefreshExternalCollectionProgressResponse response =
                GetRefreshExternalCollectionProgressResponse.newBuilder()
                        .setStatus(success())
                        .setJobInfo(RefreshExternalCollectionJobInfo.newBuilder()
                                .setJobId(11L)
                                .setCollectionName("coll")
                .setState(io.milvus.grpc.RefreshExternalCollectionState.RefreshInProgress)
                .setProgress(50L)
                .setReason("")
                .setExternalSource("file")
                .setStartTime(1L)
                .setEndTime(2L)
                .build())
                        .build();
        when(stub.getRefreshExternalCollectionProgress(any())).thenReturn(response);

        io.milvus.v2.service.utility.response.GetRefreshExternalCollectionProgressResp result =
                service.getRefreshExternalCollectionProgress(stub,
                        GetRefreshExternalCollectionProgressReq.builder().jobId(11L).build());

        assertEquals(11L, result.getJobInfo().getJobId());
        assertEquals("RefreshInProgress", result.getJobInfo().getState());
        assertEquals(50, result.getJobInfo().getProgress());
    }

    @Test
    void listRefreshExternalCollectionJobsReturnsJobs() {
        ListRefreshExternalCollectionJobsResponse response =
                ListRefreshExternalCollectionJobsResponse.newBuilder()
                        .setStatus(success())
                        .addJobs(RefreshExternalCollectionJobInfo.newBuilder()
                                .setJobId(11L)
                                .setCollectionName("coll")
                .setState(io.milvus.grpc.RefreshExternalCollectionState.RefreshInProgress)
                .setProgress(10L)
                .build())
                        .build();
        when(stub.listRefreshExternalCollectionJobs(any())).thenReturn(response);

        io.milvus.v2.service.utility.response.ListRefreshExternalCollectionJobsResp result =
                service.listRefreshExternalCollectionJobs(stub,
                        ListRefreshExternalCollectionJobsReq.builder()
                                .collectionName("coll")
                                .databaseName("db")
                                .build());

        assertEquals(1, result.getJobs().size());
        assertEquals(11L, result.getJobs().get(0).getJobId());
    }

    @Test
    void addFileResourceReturnsNull() {
        when(stub.addFileResource(any())).thenReturn(success());

        assertNull(service.addFileResource(stub, AddFileResourceReq.builder()
                .name("res")
                .path("/tmp/file")
                .build()));
        verify(stub).addFileResource(any());
    }

    @Test
    void addFileResourceRejectsEmptyName() {
        AddFileResourceReq request = AddFileResourceReq.builder()
                .name("")
                .path("/tmp/file")
                .build();

        assertThrows(MilvusClientException.class, () -> service.addFileResource(stub, request));
    }

    @Test
    void addFileResourceRejectsEmptyPath() {
        AddFileResourceReq request = AddFileResourceReq.builder()
                .name("res")
                .path(null)
                .build();

        assertThrows(MilvusClientException.class, () -> service.addFileResource(stub, request));
    }

    @Test
    void removeFileResourceReturnsNull() {
        when(stub.removeFileResource(any())).thenReturn(success());

        assertNull(service.removeFileResource(stub, RemoveFileResourceReq.builder().name("res").build()));
        verify(stub).removeFileResource(any());
    }

    @Test
    void removeFileResourceRejectsEmptyName() {
        RemoveFileResourceReq request = RemoveFileResourceReq.builder().name(null).build();

        assertThrows(MilvusClientException.class, () -> service.removeFileResource(stub, request));
    }

    @Test
    void listFileResourcesReturnsResources() {
        ListFileResourcesResponse response = ListFileResourcesResponse.newBuilder()
                .setStatus(success())
                .addResources(FileResourceInfo.newBuilder()
                        .setName("res")
                        .setPath("/tmp/file")
                        .build())
                .build();
        when(stub.listFileResources(any())).thenReturn(response);

        ListFileResourcesResp result = service.listFileResources(stub,
                ListFileResourcesReq.builder().build());

        assertEquals(1, result.getResources().size());
        assertEquals("res", result.getResources().get(0).getName());
        assertEquals("/tmp/file", result.getResources().get(0).getPath());
    }
}
