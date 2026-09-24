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

package io.milvus.integration.v2.service.utility;

import com.google.gson.JsonObject;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.grpc.CompactionMergeInfo;
import io.milvus.grpc.DescribeCollectionRequest;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FlushAllRequest;
import io.milvus.grpc.FlushAllResponse;
import io.milvus.grpc.GetCompactionPlansRequest;
import io.milvus.grpc.GetCompactionPlansResponse;
import io.milvus.grpc.GetCompactionStateRequest;
import io.milvus.grpc.GetCompactionStateResponse;
import io.milvus.grpc.GetFlushAllStateRequest;
import io.milvus.grpc.GetFlushAllStateResponse;
import io.milvus.grpc.ManualCompactionRequest;
import io.milvus.grpc.ManualCompactionResponse;
import io.milvus.grpc.Status;
import io.milvus.support.v2.BaseTest;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.CompactionPlan;
import io.milvus.v2.common.CompactionState;
import io.milvus.v2.common.CompactionTaskState;
import io.milvus.v2.common.CompactionType;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.utility.OptimizeTask;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("integration")
class UtilityTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(UtilityTest.class);

    @BeforeEach
    @AfterEach
    void clearTimestampCache() {
        CollectionTsCache.getInstance().clear();
    }

    @Test
    void testCreateAlias() {
        CollectionTsCache.getInstance().set("", "default", "test", 100L);
        CreateAliasReq req = CreateAliasReq.builder()
                .collectionName("test")
                .alias("test_alias")
                .build();
        client_v2.createAlias(req);
        assertEquals(100L, CollectionTsCache.getInstance().get("", "default", "test_alias"));
    }

    @Test
    void testDropAlias() {
        CollectionTsCache.getInstance().set("", "default", "test_alias", 100L);
        DropAliasReq req = DropAliasReq.builder()
                .alias("test_alias")
                .build();
        client_v2.dropAlias(req);
        assertEquals(0L, CollectionTsCache.getInstance().get("", "default", "test_alias"));
    }

    @Test
    void testAlterAlias() {
        CollectionTsCache.getInstance().set("", "default", "test", 100L);
        CollectionTsCache.getInstance().set("", "default", "test_alias", 50L);
        AlterAliasReq req = AlterAliasReq.builder()
                .collectionName("test")
                .alias("test_alias")
                .build();
        client_v2.alterAlias(req);
        assertEquals(100L, CollectionTsCache.getInstance().get("", "default", "test_alias"));
    }

    @Test
    void describeAlias() {
        DescribeAliasReq req = DescribeAliasReq.builder()
                .alias("test_alias")
                .build();
        DescribeAliasResp statusR = client_v2.describeAlias(req);
    }

    @Test
    void listAliases() {
        ListAliasesReq req = ListAliasesReq.builder()
                .collectionName("test")
                .build();
        ListAliasResp statusR = client_v2.listAliases(req);
        assertEquals("test", statusR.getCollectionName());
        assertEquals("default", statusR.getDbName());
        assertTrue(statusR.getAlias().contains("test_alias"));
    }

    @Test
    void getCompactionPlans() {
        Status success = Status.newBuilder().setCode(0).build();
        when(blockingStub.getCompactionStateWithPlans(any(GetCompactionPlansRequest.class)))
                .thenReturn(GetCompactionPlansResponse.newBuilder()
                        .setStatus(success)
                        .setState(io.milvus.grpc.CompactionState.Executing)
                        .addMergeInfos(CompactionMergeInfo.newBuilder()
                                .setPlanId(10L)
                                .setTriggerId(20L)
                                .setCollectionId(30L)
                                .setPartitionId(40L)
                                .setChannel("ch-0")
                                .setType(io.milvus.grpc.CompactionType.CompactionTypeMerge)
                                .setState(io.milvus.grpc.CompactionTaskState.CompactionTaskStateCompleted)
                                .setFailureReason("")
                                .setTarget(1L)
                                .addTargets(1L)
                                .addTargets(5L)
                                .addSources(2L)
                                .build())
                        .build());

        GetCompactionPlansReq req = GetCompactionPlansReq.builder()
                .compactionID(123L)
                .build();
        GetCompactionPlansResp resp = client_v2.getCompactionPlans(req);
        assertEquals(123L, resp.getCompactionId());
        assertEquals(CompactionState.Executing, resp.getState());
        assertEquals(1, resp.getPlans().size());
        CompactionPlan plan = resp.getPlans().get(0);
        assertEquals(Long.valueOf(10L), plan.getPlanId());
        assertEquals(Long.valueOf(20L), plan.getTriggerId());
        assertEquals(Long.valueOf(30L), plan.getCollectionId());
        assertEquals(Long.valueOf(40L), plan.getPartitionId());
        assertEquals("ch-0", plan.getChannel());
        assertEquals(CompactionType.Merge, plan.getCompactionType());
        assertEquals(CompactionTaskState.Completed, plan.getState());
        assertEquals("", plan.getFailureReason());
        assertEquals(1L, plan.getTarget());
        assertTrue(plan.getTargets().contains(1L));
        assertTrue(plan.getTargets().contains(5L));
        assertTrue(plan.getSources().contains(2L));
    }

    @Test
    void testGetCompactionStateRejectsNullCompactionId() {
        GetCompactionStateReq req = GetCompactionStateReq.builder().build();
        assertThrows(MilvusClientException.class, () -> client_v2.getCompactionState(req));
    }

    @Test
    void testGetCompactionPlansRejectsNullCompactionId() {
        GetCompactionPlansReq req = GetCompactionPlansReq.builder().build();
        assertThrows(MilvusClientException.class, () -> client_v2.getCompactionPlans(req));
    }

    @Test
    void testListCompactionTasks() {
        Status success = Status.newBuilder().setCode(0).build();
        when(blockingStub.getCompactionStateWithPlans(any(GetCompactionPlansRequest.class)))
                .thenReturn(GetCompactionPlansResponse.newBuilder()
                        .setStatus(success)
                        .setState(io.milvus.grpc.CompactionState.Completed)
                        .addMergeInfos(CompactionMergeInfo.newBuilder()
                                .setPlanId(10L)
                                .setState(io.milvus.grpc.CompactionTaskState.CompactionTaskStateFailed)
                                .setFailureReason("oom")
                                .setTarget(1L)
                                .addTargets(1L)
                                .addSources(2L)
                                .build())
                        .build());

        ListCompactionTasksReq req = ListCompactionTasksReq.builder()
                .collectionName("test")
                .build();
        GetCompactionPlansResp resp = client_v2.listCompactionTasks(req);
        assertEquals("test", resp.getCollectionName());
        assertEquals(CompactionState.Completed, resp.getState());
        assertEquals(1, resp.getPlans().size());
        CompactionPlan plan = resp.getPlans().get(0);
        assertEquals(Long.valueOf(10L), plan.getPlanId());
        assertEquals(CompactionTaskState.Failed, plan.getState());
        assertEquals("oom", plan.getFailureReason());
        assertEquals(1L, plan.getTarget());
        assertTrue(plan.getTargets().contains(1L));
        assertTrue(plan.getSources().contains(2L));

        ArgumentCaptor<GetCompactionPlansRequest> captor =
                ArgumentCaptor.forClass(GetCompactionPlansRequest.class);
        verify(blockingStub).getCompactionStateWithPlans(captor.capture());
        assertEquals("test", captor.getValue().getCollectionName());
    }

    @Test
    void testListCompactionTasksRejectsEmptyCollectionName() {
        ListCompactionTasksReq req = ListCompactionTasksReq.builder()
                .collectionName("")
                .build();
        assertThrows(MilvusClientException.class, () -> client_v2.listCompactionTasks(req));
    }

    @Test
    void testRefreshExternalCollection() {
        JsonObject spec = new JsonObject();
        spec.addProperty("format", "parquet");
        RefreshExternalCollectionReq req = RefreshExternalCollectionReq.builder()
                .collectionName("ext_coll")
                .externalSource("s3://bucket/path")
                .externalSpec(spec)
                .build();
        RefreshExternalCollectionResp resp = client_v2.refreshExternalCollection(req);
        assertEquals(12345L, resp.getJobId());
    }

    @Test
    void testRefreshExternalCollectionWithDatabase() {
        RefreshExternalCollectionReq req = RefreshExternalCollectionReq.builder()
                .databaseName("my_db")
                .collectionName("ext_coll")
                .build();
        RefreshExternalCollectionResp resp = client_v2.refreshExternalCollection(req);
        assertEquals(12345L, resp.getJobId());
    }

    @Test
    void testCompactConvertsTargetSizeUnitToMegabytes() {
        Status success = Status.newBuilder().setCode(0).build();
        when(blockingStub.describeCollection(any(DescribeCollectionRequest.class)))
                .thenReturn(DescribeCollectionResponse.newBuilder()
                        .setStatus(success)
                        .setCollectionID(100L)
                        .build());
        when(blockingStub.manualCompaction(any(ManualCompactionRequest.class)))
                .thenReturn(ManualCompactionResponse.newBuilder()
                        .setStatus(success)
                        .setCompactionID(200L)
                        .build());

        CompactReq request = CompactReq.builder()
                .collectionName("test")
                .targetSize(2L)
                .targetSizeUnit("GB")
                .build();
        CompactResp response = client_v2.compact(request);

        assertEquals(Long.valueOf(200L), response.getCompactionID());
        ArgumentCaptor<ManualCompactionRequest> captor =
                ArgumentCaptor.forClass(ManualCompactionRequest.class);
        verify(blockingStub).manualCompaction(captor.capture());
        assertEquals(2048L, captor.getValue().getTargetSize());
        assertEquals("test", captor.getValue().getCollectionName());
    }

    @Test
    void testGetRefreshExternalCollectionProgress() {
        GetRefreshExternalCollectionProgressReq req = GetRefreshExternalCollectionProgressReq.builder()
                .jobId(12345L)
                .build();
        GetRefreshExternalCollectionProgressResp resp = client_v2.getRefreshExternalCollectionProgress(req);
        RefreshExternalCollectionJobInfo jobInfo = resp.getJobInfo();
        assertNotNull(jobInfo);
        assertEquals(12345L, jobInfo.getJobId());
        assertEquals("ext_coll", jobInfo.getCollectionName());
        assertEquals("RefreshCompleted", jobInfo.getState());
        assertEquals(100, jobInfo.getProgress());
        assertEquals("", jobInfo.getReason());
        assertEquals("s3://bucket/path", jobInfo.getExternalSource());
        assertEquals("{\"format\":\"parquet\"}", jobInfo.getExternalSpec());
        assertEquals(1000L, jobInfo.getStartTime());
        assertEquals(2000L, jobInfo.getEndTime());
    }

    @Test
    void testListRefreshExternalCollectionJobs() {
        ListRefreshExternalCollectionJobsReq req = ListRefreshExternalCollectionJobsReq.builder()
                .collectionName("ext_coll")
                .build();
        ListRefreshExternalCollectionJobsResp resp = client_v2.listRefreshExternalCollectionJobs(req);
        assertNotNull(resp.getJobs());
        assertEquals(2, resp.getJobs().size());

        RefreshExternalCollectionJobInfo job1 = resp.getJobs().get(0);
        assertEquals(12345L, job1.getJobId());
        assertEquals("RefreshCompleted", job1.getState());
        assertEquals(100, job1.getProgress());

        RefreshExternalCollectionJobInfo job2 = resp.getJobs().get(1);
        assertEquals(12346L, job2.getJobId());
        assertEquals("RefreshInProgress", job2.getState());
        assertEquals(50, job2.getProgress());
        assertEquals("s3://bucket/path2", job2.getExternalSource());
        assertEquals(0L, job2.getEndTime());
    }

    @Test
    void testListRefreshExternalCollectionJobsWithDatabase() {
        ListRefreshExternalCollectionJobsReq req = ListRefreshExternalCollectionJobsReq.builder()
                .databaseName("my_db")
                .collectionName("ext_coll")
                .build();
        ListRefreshExternalCollectionJobsResp resp = client_v2.listRefreshExternalCollectionJobs(req);
        assertNotNull(resp.getJobs());
        assertEquals(2, resp.getJobs().size());
    }

    @Test
    void testAddFileResource() {
        AddFileResourceReq req = AddFileResourceReq.builder()
                .name("test_resource")
                .path("/data/test.parquet")
                .build();
        client_v2.addFileResource(req);
    }

    @Test
    void testRemoveFileResource() {
        RemoveFileResourceReq req = RemoveFileResourceReq.builder()
                .name("test_resource")
                .build();
        client_v2.removeFileResource(req);
    }

    @Test
    void testListFileResources() {
        ListFileResourcesReq req = ListFileResourcesReq.builder().build();
        ListFileResourcesResp resp = client_v2.listFileResources(req);
        assertNotNull(resp.getResources());
        assertEquals(2, resp.getResources().size());

        FileResourceInfo info1 = resp.getResources().get(0);
        assertEquals("test_resource", info1.getName());
        assertEquals("/data/test.parquet", info1.getPath());

        FileResourceInfo info2 = resp.getResources().get(1);
        assertEquals("test_resource_2", info2.getName());
        assertEquals("/data/test2.parquet", info2.getPath());
    }

    @Test
    void testGetPersistentSegmentInfo() {
        GetPersistentSegmentInfoResp resp = client_v2.getPersistentSegmentInfo(GetPersistentSegmentInfoReq.builder()
                .collectionName("test")
                .build());

        assertEquals(1, resp.getSegmentInfos().size());
        GetPersistentSegmentInfoResp.PersistentSegmentInfo info = resp.getSegmentInfos().get(0);
        assertEquals(1L, info.getSegmentID());
        assertEquals(2L, info.getCollectionID());
        assertEquals(3L, info.getPartitionID());
        assertEquals("test", info.getCollectionName());
        assertEquals(4L, info.getNumOfRows());
        assertEquals("Flushed", info.getState());
        assertEquals("L1", info.getLevel());
        assertEquals(5L, info.getStorageVersion());
        assertTrue(info.getIsSorted());
    }

    @Test
    void testGetQuerySegmentInfo() {
        GetQuerySegmentInfoResp resp = client_v2.getQuerySegmentInfo(GetQuerySegmentInfoReq.builder()
                .collectionName("test")
                .build());

        assertEquals(1, resp.getSegmentInfos().size());
        GetQuerySegmentInfoResp.QuerySegmentInfo info = resp.getSegmentInfos().get(0);
        assertEquals("test", info.getCollectionName());
        assertEquals(6L, info.getSegmentID());
        assertEquals(7L, info.getCollectionID());
        assertEquals(8L, info.getPartitionID());
        assertEquals(9L, info.getMemSize());
        assertEquals(10L, info.getNumOfRows());
        assertEquals("test_index", info.getIndexName());
        assertEquals(11L, info.getIndexID());
        assertEquals("Sealed", info.getState());
        assertEquals("L1", info.getLevel());
        assertEquals(1, info.getNodeIDs().size());
        assertEquals(12L, info.getNodeIDs().get(0));
        assertEquals(13L, info.getStorageVersion());
        assertTrue(info.getIsSorted());
    }

    @Test
    void testFlushAll() throws Exception {
        Status success = Status.newBuilder().setCode(0).build();
        when(blockingStub.flushAll(any(FlushAllRequest.class)))
                .thenReturn(FlushAllResponse.newBuilder().setStatus(success).setFlushAllTs(123L).build());

        // flushAll() polls waitFlushAll() on a stub built from client_v2.channel, which is null in BaseTest.
        // Point the channel at a mock that immediately delivers a flushed=true state so the poll exits fast.
        ManagedChannel channel = mock(ManagedChannel.class);
        setChannelField(client_v2, channel);

        ClientCall<Object, Object> call = mock(ClientCall.class);
        when(channel.newCall(any(), any())).thenReturn(call);
        doAnswer(invocation -> {
            ClientCall.Listener<Object> listener = invocation.getArgument(0);
            listener.onMessage(GetFlushAllStateResponse.newBuilder().setStatus(success).setFlushed(true).build());
            listener.onClose(io.grpc.Status.OK, new Metadata());
            return null;
        }).when(call).start(any(), any());

        try {
            FlushAllReq req = FlushAllReq.builder().build();
            FlushAllResp resp = client_v2.flushAll(req);
            assertEquals(Long.valueOf(123L), resp.getFlushAllTs());
            verify(blockingStub).flushAll(any(FlushAllRequest.class));
        } finally {
            setChannelField(client_v2, null);
        }
    }

    @Test
    void testGetFlushAllState() {
        Status success = Status.newBuilder().setCode(0).build();
        when(blockingStub.getFlushAllState(any(GetFlushAllStateRequest.class)))
                .thenReturn(GetFlushAllStateResponse.newBuilder().setStatus(success).setFlushed(true).build());

        GetFlushAllStateReq req = GetFlushAllStateReq.builder()
                .databaseName("default")
                .flushAllTs(123L)
                .build();
        GetFlushAllStateResp resp = client_v2.getFlushAllState(req);
        assertEquals(Boolean.TRUE, resp.getFlushed());
    }

    @Test
    void testGetCompactionState() {
        Status success = Status.newBuilder().setCode(0).build();
        when(blockingStub.getCompactionState(any(GetCompactionStateRequest.class)))
                .thenReturn(GetCompactionStateResponse.newBuilder()
                        .setStatus(success)
                        .setState(io.milvus.grpc.CompactionState.Completed)
                        .setExecutingPlanNo(1L)
                        .setTimeoutPlanNo(2L)
                        .setCompletedPlanNo(3L)
                        .build());

        GetCompactionStateReq req = GetCompactionStateReq.builder()
                .compactionID(200L)
                .build();
        GetCompactionStateResp resp = client_v2.getCompactionState(req);
        assertEquals(CompactionState.Completed, resp.getState());
        assertEquals(Long.valueOf(1L), resp.getExecutingPlanNo());
        assertEquals(Long.valueOf(2L), resp.getTimeoutPlanNo());
        assertEquals(Long.valueOf(3L), resp.getCompletedPlanNo());
    }

    @Test
    void testOptimize() {
        Status success = Status.newBuilder().setCode(0).build();
        when(blockingStub.manualCompaction(any(ManualCompactionRequest.class)))
                .thenReturn(ManualCompactionResponse.newBuilder().setStatus(success).setCompactionID(200L).build());
        when(blockingStub.getCompactionState(any(GetCompactionStateRequest.class)))
                .thenReturn(GetCompactionStateResponse.newBuilder()
                        .setStatus(success)
                        .setState(io.milvus.grpc.CompactionState.Completed)
                        .build());

        OptimizeReq req = OptimizeReq.builder()
                .collectionName("test")
                .targetSize("1GB")
                .async(true)
                .timeout(10000L)
                .build();
        OptimizeTask task = client_v2.optimize(req);
        assertNotNull(task);
        verify(blockingStub, timeout(5000)).manualCompaction(any(ManualCompactionRequest.class));
    }

    @Test
    void testOptimizeRejectsEmptyCollectionName() {
        assertThrows(IllegalArgumentException.class, () -> OptimizeReq.builder().build());
    }

    private static void setChannelField(MilvusClientV2 client, ManagedChannel channel) throws Exception {
        Field field = MilvusClientV2.class.getDeclaredField("channel");
        field.setAccessible(true);
        field.set(client, channel);
    }
}
