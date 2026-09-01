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

package io.milvus.v2.service.utility;

import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.grpc.ManualCompactionRequest;
import io.milvus.grpc.Status;
import io.milvus.v2.BaseTest;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.DescribeAliasResp;
import io.milvus.v2.service.utility.response.GetPersistentSegmentInfoResp;
import io.milvus.v2.service.utility.response.GetQuerySegmentInfoResp;
import io.milvus.v2.service.utility.response.ListAliasResp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    }

    @Test
    void testListAliasesWithDbName() {
        io.milvus.grpc.ListAliasesResponse response = io.milvus.grpc.ListAliasesResponse.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .setDbName("test_db")
                .setCollectionName("test")
                .addAliases("test_alias")
                .build();
        when(blockingStub.listAliases(any())).thenReturn(response);

        ListAliasResp resp = client_v2.listAliases(ListAliasesReq.builder()
                .collectionName("test")
                .build());
        assertEquals("test_db", resp.getDbName());
        assertEquals("test", resp.getCollectionName());
        assertEquals(1, resp.getAlias().size());
    }

    @Test
    void testCompactTargetSizeUnit() {
        CompactReq req = CompactReq.builder()
                .collectionName("test")
                .targetSize(1L)
                .targetSizeUnit("gb")
                .build();
        client_v2.compact(req);

        ArgumentCaptor<ManualCompactionRequest> captor = ArgumentCaptor.forClass(ManualCompactionRequest.class);
        verify(blockingStub).manualCompaction(captor.capture());
        assertEquals(1024L, captor.getValue().getTargetSize());
    }

    @Test
    void testCompactInvalidTargetSizeRejected() {
        CompactReq req = CompactReq.builder()
                .collectionName("test")
                .targetSize(0L)
                .build();
        assertThrows(MilvusClientException.class, () -> client_v2.compact(req));
        verify(blockingStub, never()).manualCompaction(any(ManualCompactionRequest.class));
    }

    @Test
    void testCompactInvalidTargetSizeUnitRejected() {
        CompactReq req = CompactReq.builder()
                .collectionName("test")
                .targetSize(1L)
                .targetSizeUnit("xx")
                .build();
        assertThrows(MilvusClientException.class, () -> client_v2.compact(req));
        verify(blockingStub, never()).manualCompaction(any(ManualCompactionRequest.class));
    }

    @Test
    void testCompactBlankTargetSizeUnitDefaultsToMb() {
        CompactReq req = CompactReq.builder()
                .collectionName("test")
                .targetSize(2L)
                .targetSizeUnit(" ")
                .build();
        client_v2.compact(req);

        ArgumentCaptor<ManualCompactionRequest> captor = ArgumentCaptor.forClass(ManualCompactionRequest.class);
        verify(blockingStub).manualCompaction(captor.capture());
        assertEquals(2L, captor.getValue().getTargetSize());
    }

    @Test
    void testCompactTooSmallTargetSizeRejected() {
        CompactReq req = CompactReq.builder()
                .collectionName("test")
                .targetSize(1L)
                .targetSizeUnit("kb")
                .build();
        assertThrows(MilvusClientException.class, () -> client_v2.compact(req));
        verify(blockingStub, never()).manualCompaction(any(ManualCompactionRequest.class));
    }

    @Test
    void testCompactLargeTargetSizeNoOverflow() {
        CompactReq req = CompactReq.builder()
                .collectionName("test")
                .targetSize(1000L)
                .targetSizeUnit("tb")
                .build();
        client_v2.compact(req);

        ArgumentCaptor<ManualCompactionRequest> captor = ArgumentCaptor.forClass(ManualCompactionRequest.class);
        verify(blockingStub).manualCompaction(captor.capture());
        assertEquals(1000L * 1024L * 1024L, captor.getValue().getTargetSize());
    }

    @Test
    void testCompactOversizeTargetSizeRejected() {
        CompactReq req = CompactReq.builder()
                .collectionName("test")
                .targetSize(Long.MAX_VALUE)
                .targetSizeUnit("pb")
                .build();
        assertThrows(MilvusClientException.class, () -> client_v2.compact(req));
        verify(blockingStub, never()).manualCompaction(any(ManualCompactionRequest.class));
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
        assertEquals("test", info.getCollectionName());
    }
}
