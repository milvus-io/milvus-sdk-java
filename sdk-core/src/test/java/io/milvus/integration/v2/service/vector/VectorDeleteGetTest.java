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

package io.milvus.integration.v2.service.vector;

import io.milvus.grpc.*;
import io.milvus.support.v2.BaseTest;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("integration")
class VectorDeleteGetTest extends BaseTest {

    Logger logger = LoggerFactory.getLogger(VectorDeleteGetTest.class);

    @Test
    void testDelete() {
        DeleteReq request = DeleteReq.builder()
                .collectionName("test")
                .filter("id > 0")
                .build();
        DeleteResp statusR = client_v2.delete(request);
        logger.info(statusR.toString());
    }

    @Test
    void testDeleteById() {
        DeleteReq request = DeleteReq.builder()
                .collectionName("test")
                .ids(Collections.singletonList("0"))
                .build();
        DeleteResp statusR = client_v2.delete(request);
        logger.info(statusR.toString());
    }

    @Test
    void testGet() {
        GetReq request = GetReq.builder()
                .collectionName("test2")
                .ids(Collections.singletonList("447198483337881033"))
                .build();
        GetResp statusR = client_v2.get(request);
        logger.info(statusR.toString());
    }

    @Test
    void testGetWithEmptyIdsReturnsEmptyWithoutRpc() {
        GetReq request = GetReq.builder()
                .collectionName("book")
                .ids(Collections.emptyList())
                .build();

        GetResp resp = client_v2.get(request);

        Assertions.assertNotNull(resp.getGetResults());
        Assertions.assertTrue(resp.getGetResults().isEmpty());
        verify(blockingStub, never()).query(any(QueryRequest.class));
    }

    @Test
    void testGetInheritsQueryRespMetadata() {
        QueryResults queryResults = QueryResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0)
                        .putExtraInfo("report_value", "123")
                        .putExtraInfo("scanned_remote_bytes", "456")
                        .putExtraInfo("scanned_total_bytes", "789")
                        .putExtraInfo("cache_hit_ratio", "0.5")
                        .build())
                .setSessionTs(888L)
                .build();
        when(blockingStub.query(any(QueryRequest.class))).thenReturn(queryResults);

        GetReq request = GetReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .build();
        GetResp resp = client_v2.get(request);

        Assertions.assertTrue(resp instanceof QueryResp);
        Assertions.assertEquals(888L, resp.getSessionTs());
        Assertions.assertEquals(123L, resp.getCost());
        Assertions.assertEquals(456L, resp.getScannedRemoteBytes());
        Assertions.assertEquals(789L, resp.getScannedTotalBytes());
        Assertions.assertEquals(0.5f, resp.getCacheHitRatio());
        Assertions.assertSame(resp.getGetResults(), resp.getQueryResults());
    }

    @Test
    void testGetIgnoresMalformedScanMetrics() {
        QueryResults queryResults = QueryResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0)
                        .putExtraInfo("report_value", "123")
                        .putExtraInfo("scanned_remote_bytes", "not-a-number")
                        .putExtraInfo("cache_hit_ratio", "oops")
                        .build())
                .setSessionTs(1L)
                .build();
        when(blockingStub.query(any(QueryRequest.class))).thenReturn(queryResults);

        GetReq request = GetReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .build();
        GetResp resp = client_v2.get(request);

        Assertions.assertEquals(123L, resp.getCost());
        Assertions.assertNull(resp.getScannedRemoteBytes());
        Assertions.assertNull(resp.getScannedTotalBytes());
        Assertions.assertNull(resp.getCacheHitRatio());
    }

    @Test
    void testDeleteRejectsInvalidIdType() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.delete(DeleteReq.builder().collectionName("book")
                        .ids(Collections.singletonList(1.0d)).build()));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("must be integers or strings"));
    }

    @Test
    void testDeleteRejectsEmptyIds() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.delete(DeleteReq.builder().collectionName("book")
                        .ids(Collections.emptyList()).build()));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("must not be empty"));
    }

    @Test
    void testDeleteRejectsNestedIds() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.delete(DeleteReq.builder().collectionName("book")
                        .ids(Collections.singletonList(Collections.singletonList(1L))).build()));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("nested lists are not allowed"));
    }

    @Test
    void testDeleteRejectsNullElementInIds() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.delete(DeleteReq.builder().collectionName("book")
                        .ids(Arrays.asList(1L, null)).build()));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("got: null"));
    }

    @Test
    void testGetWithMultiplePartitions() {
        GetReq request = GetReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .partitionName("p1")
                .partitionNames(Arrays.asList("p2", "p3"))
                .build();

        client_v2.get(request);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        Assertions.assertEquals(Arrays.asList("p1", "p2", "p3"),
                captor.getValue().getPartitionNamesList());
    }

    @Test
    void testGetWithOverlappingPartitionsDedup() {
        GetReq request = GetReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .partitionName("p1")
                .partitionNames(Arrays.asList("p1", "p2"))
                .build();

        client_v2.get(request);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        Assertions.assertEquals(Arrays.asList("p1", "p2"),
                captor.getValue().getPartitionNamesList());
    }

    @Test
    void testGetAsync() throws Exception {
        GetReq request = GetReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .outputFields(Collections.singletonList("*"))
                .build();

        GetResp response = client_v2.getAsync(request).get(1, TimeUnit.SECONDS);

        Assertions.assertNotNull(response);
        verify(futureStub).query(any(QueryRequest.class));
        verify(blockingStub, never()).query(any(QueryRequest.class));
    }

    @Test
    void testGetAsyncWithEmptyIdsReturnsEmptyWithoutRpc() throws Exception {
        GetReq request = GetReq.builder()
                .collectionName("book")
                .ids(Collections.emptyList())
                .build();

        GetResp resp = client_v2.getAsync(request).get(1, TimeUnit.SECONDS);

        Assertions.assertNotNull(resp.getGetResults());
        Assertions.assertTrue(resp.getGetResults().isEmpty());
        verify(futureStub, never()).query(any(QueryRequest.class));
    }
}
