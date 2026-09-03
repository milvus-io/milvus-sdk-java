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

package io.milvus.v2.service.vector;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.JsonObject;
import io.milvus.common.interceptor.ClientRequestInterceptor;
import io.milvus.common.utils.JsonUtils;
import io.milvus.common.utils.cache.SchemaCache;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.v2.BaseTest;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VectorTest extends BaseTest {

    Logger logger = LoggerFactory.getLogger(VectorTest.class);

    @Test
    void testInsert() {

        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            JsonObject vector = new JsonObject();
            List<Float> vectorList = new ArrayList<>();
            vectorList.add(1.0f);
            vectorList.add(2.0f);
            vector.add("vector", JsonUtils.toJsonTree(vectorList));
            vector.addProperty("id", (long) i);
            data.add(vector);
        }

        InsertReq request = InsertReq.builder()
                .collectionName("test")
                .data(data)
                .build();
        InsertResp statusR = client_v2.insert(request);
        logger.info(statusR.toString());
    }

    @Test
    void testUpsert() {

        JsonObject jsonObject = new JsonObject();
        List<Float> vectorList = new ArrayList<>();
        vectorList.add(2.0f);
        vectorList.add(3.0f);
        jsonObject.add("vector", JsonUtils.toJsonTree(vectorList));
        jsonObject.addProperty("id", 0L);
        UpsertReq request = UpsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(jsonObject))
                .build();

        UpsertResp statusR = client_v2.upsert(request);
        logger.info(statusR.toString());
    }

    @Test
    void testInsertEmptyDataNoRpc() {
        InsertReq request = InsertReq.builder()
                .collectionName("test")
                .data(Collections.emptyList())
                .build();
        InsertResp resp = client_v2.insert(request);
        Assertions.assertEquals(0L, resp.getInsertCnt());
        Assertions.assertTrue(resp.getPrimaryKeys().isEmpty());
        verify(blockingStub, never()).insert(any(InsertRequest.class));
        verify(blockingStub, never()).describeCollection(any(DescribeCollectionRequest.class));
    }

    @Test
    void testUpsertEmptyDataNoRpc() {
        UpsertReq request = UpsertReq.builder()
                .collectionName("test")
                .data(Collections.emptyList())
                .build();
        UpsertResp resp = client_v2.upsert(request);
        Assertions.assertEquals(0L, resp.getUpsertCnt());
        Assertions.assertTrue(resp.getPrimaryKeys().isEmpty());
        verify(blockingStub, never()).upsert(any(UpsertRequest.class));
        verify(blockingStub, never()).describeCollection(any(DescribeCollectionRequest.class));
    }

    @Test
    void testGetEmptyIdsNoRpc() {
        GetReq request = GetReq.builder()
                .collectionName("test")
                .ids(Collections.emptyList())
                .build();
        GetResp resp = client_v2.get(request);
        Assertions.assertTrue(resp.getGetResults().isEmpty());
        verify(blockingStub, never()).query(any(QueryRequest.class));
    }

    @Test
    void testSearchInvalidLimitRejected() {
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(0)
                .build();
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.search(request));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        verify(blockingStub, never()).search(any(SearchRequest.class));
    }

    @Test
    void testHybridSearchInvalidLimitRejected() {
        AnnSearchReq annSearchReq = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        HybridSearchReq request = HybridSearchReq.builder()
                .collectionName("test")
                .searchRequests(Collections.singletonList(annSearchReq))
                .limit(0)
                .build();
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.hybridSearch(request));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        verify(blockingStub, never()).hybridSearch(any(HybridSearchRequest.class));
    }

    @Test
    void testSearchAsyncInvalidLimitReturnsFailedFuture() throws Exception {
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(0)
                .build();
        CompletableFuture<SearchResp> future = client_v2.searchAsync(request);
        ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                () -> future.get(1, TimeUnit.SECONDS));
        Assertions.assertTrue(exception.getCause() instanceof MilvusClientException);
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS,
                ((MilvusClientException) exception.getCause()).getErrorCode());
        verify(futureStub, never()).search(any(SearchRequest.class));
    }

    @Test
    void testHybridSearchAsyncInvalidLimitReturnsFailedFuture() throws Exception {
        AnnSearchReq annSearchReq = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        HybridSearchReq request = HybridSearchReq.builder()
                .collectionName("test")
                .searchRequests(Collections.singletonList(annSearchReq))
                .limit(0)
                .build();
        CompletableFuture<SearchResp> future = client_v2.hybridSearchAsync(request);
        ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                () -> future.get(1, TimeUnit.SECONDS));
        Assertions.assertTrue(exception.getCause() instanceof MilvusClientException);
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS,
                ((MilvusClientException) exception.getCause()).getErrorCode());
        verify(futureStub, never()).hybridSearch(any(HybridSearchRequest.class));
    }

    @Test
    void testUpsertWithFieldOps() {
        JsonObject jsonObject = new JsonObject();
        List<Float> vectorList = new ArrayList<>();
        vectorList.add(2.0f);
        vectorList.add(3.0f);
        jsonObject.add("vector", JsonUtils.toJsonTree(vectorList));
        jsonObject.addProperty("id", 0L);
        UpsertReq request = UpsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(jsonObject))
                .fieldOps(Collections.singletonList(UpsertReq.FieldPartialUpdateOp.builder()
                        .fieldName("vector")
                        .opType(UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_APPEND)
                        .build()))
                .build();

        client_v2.upsert(request);

        ArgumentCaptor<UpsertRequest> captor = ArgumentCaptor.forClass(UpsertRequest.class);
        verify(blockingStub).upsert(captor.capture());
        UpsertRequest rpcRequest = captor.getValue();
        Assertions.assertTrue(rpcRequest.getPartialUpdate());
        Assertions.assertEquals(1, rpcRequest.getFieldOpsCount());
        Assertions.assertEquals("vector", rpcRequest.getFieldOps(0).getFieldName());
        Assertions.assertEquals(io.milvus.grpc.FieldPartialUpdateOp.OpType.ARRAY_APPEND, rpcRequest.getFieldOps(0).getOp());
    }

    @Test
    void testQuery() {
        QueryReq req = QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(0))
                .limit(10)
                //.outputFields(Collections.singletonList("count(*)"))
                .build();
        QueryResp resultsR = client_v2.query(req);

        logger.info(resultsR.toString());
    }

    @Test
    void testQueryAsync() throws Exception {
        QueryReq request = QueryReq.builder()
                .collectionName("book")
                .filter("id > 0")
                .limit(10)
                .build();

        QueryResp response = client_v2.queryAsync(request).get(1, TimeUnit.SECONDS);

        Assertions.assertNotNull(response);
        verify(futureStub).query(any(QueryRequest.class));
        verify(blockingStub, never()).query(any(QueryRequest.class));
    }

    @Test
    void testQueryAsyncWithIdsLoadsSchemaAsynchronously() throws Exception {
        SchemaCache.getInstance().clear();
        SettableFuture<DescribeCollectionResponse> schemaFuture = SettableFuture.create();
        when(futureStub.describeCollection(any())).thenReturn(schemaFuture);
        QueryReq request = QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .limit(10)
                .build();

        try {
            CompletableFuture<QueryResp> resultFuture = client_v2.queryAsync(request);

            Assertions.assertFalse(resultFuture.isDone());
            verify(futureStub).describeCollection(any(DescribeCollectionRequest.class));
            verify(blockingStub, never()).describeCollection(any(DescribeCollectionRequest.class));
            verify(futureStub, never()).query(any(QueryRequest.class));

            schemaFuture.set(describeCollectionResponse());
            Assertions.assertNotNull(resultFuture.get(1, TimeUnit.SECONDS));
            ArgumentCaptor<QueryRequest> queryCaptor = ArgumentCaptor.forClass(QueryRequest.class);
            verify(futureStub).query(queryCaptor.capture());
            Assertions.assertEquals("id in [1]", queryCaptor.getValue().getExpr());
        } finally {
            SchemaCache.getInstance().clear();
        }
    }

    @Test
    void testQueryAsyncCancellationDuringSchemaLoadCancelsDescribeRpc() {
        SchemaCache.getInstance().clear();
        SettableFuture<DescribeCollectionResponse> schemaFuture = SettableFuture.create();
        when(futureStub.describeCollection(any())).thenReturn(schemaFuture);
        QueryReq request = QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .build();

        try {
            CompletableFuture<QueryResp> resultFuture = client_v2.queryAsync(request);

            Assertions.assertTrue(resultFuture.cancel(true));
            Assertions.assertTrue(schemaFuture.isCancelled());
            verify(futureStub, never()).query(any(QueryRequest.class));
        } finally {
            SchemaCache.getInstance().clear();
        }
    }

    @Test
    void testQueryAsyncCancellationPreservesSharedSchemaLoad() throws Exception {
        SchemaCache.getInstance().clear();
        SettableFuture<DescribeCollectionResponse> schemaFuture = SettableFuture.create();
        when(futureStub.describeCollection(any())).thenReturn(schemaFuture);
        QueryReq request = QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .build();

        try {
            CompletableFuture<QueryResp> cancelled = client_v2.queryAsync(request);
            CompletableFuture<QueryResp> remaining = client_v2.queryAsync(request);

            Assertions.assertTrue(cancelled.cancel(true));
            Assertions.assertFalse(schemaFuture.isCancelled());

            schemaFuture.set(describeCollectionResponse());
            Assertions.assertNotNull(remaining.get(1, TimeUnit.SECONDS));
            verify(futureStub).describeCollection(any(DescribeCollectionRequest.class));
            verify(futureStub).query(any(QueryRequest.class));
        } finally {
            SchemaCache.getInstance().clear();
        }
    }

    @Test
    void testQueryAsyncKeepsCapturedRequestIdWhenSynchronousSchemaLoadCompletes() throws Exception {
        SchemaCache.getInstance().clear();
        ThreadLocal<String> requestId = configureClientRequestId();
        CountDownLatch schemaLoadStarted = new CountDownLatch(1);
        CountDownLatch releaseSchemaLoad = new CountDownLatch(1);
        when(blockingStub.describeCollection(any())).thenAnswer(invocation -> {
            schemaLoadStarted.countDown();
            if (!releaseSchemaLoad.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting to finish the synchronous schema load");
            }
            return describeCollectionResponse();
        });
        QueryReq request = QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .build();

        CompletableFuture<QueryResp> synchronous = CompletableFuture.supplyAsync(() -> {
            requestId.set("synchronous-request");
            return client_v2.query(request);
        });

        try {
            Assertions.assertTrue(schemaLoadStarted.await(5, TimeUnit.SECONDS));
            requestId.set("async-request");
            CompletableFuture<QueryResp> asynchronous = client_v2.queryAsync(request);

            requestId.set("unrelated-request");
            releaseSchemaLoad.countDown();

            Assertions.assertNotNull(synchronous.get(1, TimeUnit.SECONDS));
            Assertions.assertNotNull(asynchronous.get(1, TimeUnit.SECONDS));
            verify(futureStub).withOption(
                    ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION, "async-request");
            verify(futureStub, never()).withOption(
                    ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION, "synchronous-request");
        } finally {
            releaseSchemaLoad.countDown();
            requestId.remove();
            SchemaCache.getInstance().clear();
        }
    }

    @Test
    void testQueryAsyncUsesPreparedRequestAfterSchemaLoad() throws Exception {
        SchemaCache.getInstance().clear();
        SettableFuture<DescribeCollectionResponse> schemaFuture = SettableFuture.create();
        when(futureStub.describeCollection(any())).thenReturn(schemaFuture);
        List<String> outputFields = new ArrayList<>(Collections.singletonList("original_field"));
        QueryReq request = QueryReq.builder()
                .databaseName("original_db")
                .collectionName("original_collection")
                .ids(Collections.singletonList(1L))
                .outputFields(outputFields)
                .limit(10)
                .build();

        try {
            CompletableFuture<QueryResp> first = client_v2.queryAsync(request);
            CompletableFuture<QueryResp> second = client_v2.queryAsync(request);

            request.setDatabaseName("mutated_db");
            request.setCollectionName("mutated_collection");
            request.setLimit(20);
            outputFields.add("mutated_field");

            schemaFuture.set(describeCollectionResponse());
            first.get(1, TimeUnit.SECONDS);
            second.get(1, TimeUnit.SECONDS);

            ArgumentCaptor<QueryRequest> queryCaptor = ArgumentCaptor.forClass(QueryRequest.class);
            verify(futureStub, times(2)).query(queryCaptor.capture());
            for (QueryRequest rpcRequest : queryCaptor.getAllValues()) {
                Assertions.assertEquals("original_db", rpcRequest.getDbName());
                Assertions.assertEquals("original_collection", rpcRequest.getCollectionName());
                Assertions.assertEquals(Collections.singletonList("original_field"),
                        rpcRequest.getOutputFieldsList());
                Assertions.assertEquals("10", getParam(rpcRequest.getQueryParamsList(), Constant.LIMIT));
            }
        } finally {
            SchemaCache.getInstance().clear();
        }
    }

    @Test
    void testQueryAsyncCancellationPropagatesToQueryRpc() {
        SettableFuture<QueryResults> grpcFuture = SettableFuture.create();
        when(futureStub.query(any())).thenReturn(grpcFuture);
        QueryReq request = QueryReq.builder()
                .collectionName("book")
                .filter("id > 0")
                .build();

        CompletableFuture<QueryResp> resultFuture = client_v2.queryAsync(request);
        Assertions.assertTrue(resultFuture.cancel(true));

        Assertions.assertTrue(grpcFuture.isCancelled());
    }

    @Test
    void testQueryAsyncValidationFailureCompletesExceptionally() {
        QueryReq request = QueryReq.builder()
                .collectionName("book")
                .filter("id > 0")
                .ids(Collections.singletonList(1L))
                .build();

        CompletableFuture<QueryResp> future = Assertions.assertDoesNotThrow(
                () -> client_v2.queryAsync(request));
        ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                () -> future.get(1, TimeUnit.SECONDS));

        Assertions.assertTrue(exception.getCause() instanceof MilvusClientException);
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS,
                ((MilvusClientException) exception.getCause()).getErrorCode());
        verify(futureStub, never()).query(any(QueryRequest.class));
    }

    @Test
    void testSearch() {
        List<Float> vectorList = new ArrayList<>();
        vectorList.add(1.0f);
        vectorList.add(2.0f);
        SearchReq request = SearchReq.builder()
                .collectionName("test2")
                .data(Collections.singletonList(new FloatVec(vectorList)))
                .limit(10)
                .offset(0L)
                .build();
        SearchResp statusR = client_v2.search(request);
        logger.info(statusR.toString());
        Assertions.assertEquals(123L, statusR.getCost());
        Assertions.assertEquals(456L, statusR.getScannedRemoteBytes());
        Assertions.assertEquals(789L, statusR.getScannedTotalBytes());
        Assertions.assertEquals(0.5f, statusR.getCacheHitRatio());
    }

    @Test
    void testSearchAsync() throws Exception {
        SearchReq request = SearchReq.builder()
                .collectionName("test2")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();

        SearchResp response = client_v2.searchAsync(request).get(1, TimeUnit.SECONDS);

        Assertions.assertEquals(123L, response.getCost());
        Assertions.assertEquals(456L, response.getScannedRemoteBytes());
        Assertions.assertEquals(789L, response.getScannedTotalBytes());
        Assertions.assertEquals(0.5f, response.getCacheHitRatio());
        verify(futureStub).search(any(SearchRequest.class));
        verify(blockingStub, never()).search(any(SearchRequest.class));
    }

    @Test
    void testSearchAsyncServerFailureCompletesExceptionally() {
        SearchResults failedResponse = SearchResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(1).setReason("search failed").build())
                .build();
        when(futureStub.search(any())).thenReturn(Futures.immediateFuture(failedResponse));
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();

        ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                () -> client_v2.searchAsync(request).get(1, TimeUnit.SECONDS));

        Assertions.assertTrue(exception.getCause() instanceof MilvusClientException);
        Assertions.assertEquals(ErrorCode.SERVER_ERROR,
                ((MilvusClientException) exception.getCause()).getErrorCode());
    }

    @Test
    void testSearchAsyncCancellationPropagatesToGrpcFuture() {
        SettableFuture<SearchResults> grpcFuture = SettableFuture.create();
        when(futureStub.search(any())).thenReturn(grpcFuture);
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();

        CompletableFuture<SearchResp> future = client_v2.searchAsync(request);
        Assertions.assertTrue(future.cancel(true));

        Assertions.assertTrue(grpcFuture.isCancelled());
    }

    @Test
    void testHybridSearchAsync() throws Exception {
        AnnSearchReq annSearchReq = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        HybridSearchReq request = HybridSearchReq.builder()
                .collectionName("test")
                .searchRequests(Collections.singletonList(annSearchReq))
                .limit(10)
                .build();

        SearchResp response = client_v2.hybridSearchAsync(request).get(1, TimeUnit.SECONDS);

        Assertions.assertNotNull(response);
        Assertions.assertEquals(123L, response.getCost());
        verify(futureStub).hybridSearch(any(HybridSearchRequest.class));
        verify(blockingStub, never()).hybridSearch(any(HybridSearchRequest.class));
    }

    @Test
    void testInsertCostDecodedFromStatusExtraInfo() {
        Status status = Status.newBuilder()
                .setCode(0)
                .putExtraInfo("report_value", "456")
                .build();
        MutationResult mutationResult = MutationResult.newBuilder()
                .setStatus(status)
                .setInsertCnt(1L)
                .build();
        when(blockingStub.insert(any())).thenReturn(mutationResult);

        JsonObject row = new JsonObject();
        row.addProperty("id", 0L);
        row.add("vector", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));
        InsertReq request = InsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(row))
                .build();
        InsertResp resp = client_v2.insert(request);
        Assertions.assertEquals(456L, resp.getCost());
    }

    @Test
    void testUpsertCostDecodedFromStatusExtraInfo() {
        Status status = Status.newBuilder()
                .setCode(0)
                .putExtraInfo("report_value", "789")
                .build();
        MutationResult mutationResult = MutationResult.newBuilder()
                .setStatus(status)
                .setUpsertCnt(1L)
                .build();
        when(blockingStub.upsert(any())).thenReturn(mutationResult);

        JsonObject row = new JsonObject();
        row.addProperty("id", 0L);
        row.add("vector", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));
        UpsertReq request = UpsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(row))
                .build();
        UpsertResp resp = client_v2.upsert(request);
        Assertions.assertEquals(789L, resp.getCost());
    }

    @Test
    void testDeleteCostDecodedFromStatusExtraInfo() {
        Status status = Status.newBuilder()
                .setCode(0)
                .putExtraInfo("report_value", "321")
                .build();
        MutationResult mutationResult = MutationResult.newBuilder()
                .setStatus(status)
                .setDeleteCnt(1L)
                .build();
        when(blockingStub.delete(any())).thenReturn(mutationResult);

        DeleteReq request = DeleteReq.builder()
                .collectionName("test")
                .filter("id > 0")
                .build();
        DeleteResp resp = client_v2.delete(request);
        Assertions.assertEquals(321L, resp.getCost());
    }

    @Test
    void testInsertCostDefaultsToZeroForInvalidReportValue() {
        Status status = Status.newBuilder()
                .setCode(0)
                .putExtraInfo("report_value", "not-a-number")
                .build();
        MutationResult mutationResult = MutationResult.newBuilder()
                .setStatus(status)
                .setInsertCnt(1L)
                .build();
        when(blockingStub.insert(any())).thenReturn(mutationResult);

        JsonObject row = new JsonObject();
        row.addProperty("id", 0L);
        row.add("vector", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));
        InsertReq request = InsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(row))
                .build();
        InsertResp resp = client_v2.insert(request);
        Assertions.assertEquals(0L, resp.getCost());
    }

    @Test
    void testInsertCostDefaultsToZeroWhenReportValueAbsent() {
        // the status carries no extra_info at all: cost must default to 0
        MutationResult mutationResult = MutationResult.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .setInsertCnt(1L)
                .build();
        when(blockingStub.insert(any())).thenReturn(mutationResult);

        JsonObject row = new JsonObject();
        row.addProperty("id", 0L);
        row.add("vector", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));
        InsertReq request = InsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(row))
                .build();
        InsertResp resp = client_v2.insert(request);
        Assertions.assertEquals(0L, resp.getCost());
    }

    @Test
    void testDeleteCostDefaultsToZeroWhenReportValueAbsent() {
        MutationResult mutationResult = MutationResult.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .setDeleteCnt(1L)
                .build();
        when(blockingStub.delete(any())).thenReturn(mutationResult);

        DeleteReq request = DeleteReq.builder()
                .collectionName("test")
                .filter("id > 0")
                .build();
        DeleteResp resp = client_v2.delete(request);
        Assertions.assertEquals(0L, resp.getCost());
    }

    @Test
    void testDeleteReqConsistencyLevelRoundTrip() {
        DeleteReq request = DeleteReq.builder()
                .collectionName("test")
                .filter("id > 0")
                .consistencyLevel(ConsistencyLevel.SESSION)
                .build();
        Assertions.assertEquals(ConsistencyLevel.SESSION, request.getConsistencyLevel());
        request.setConsistencyLevel(ConsistencyLevel.BOUNDED);
        Assertions.assertEquals(ConsistencyLevel.BOUNDED, request.getConsistencyLevel());
        request.setFilterTemplateValues(Collections.emptyMap());
        request.setDatabaseName("db");
        request.setCollectionName("coll");
        request.setPartitionName("p");
        request.setIds(Collections.singletonList(1L));
        Assertions.assertEquals("db", request.getDatabaseName());
        Assertions.assertEquals("coll", request.getCollectionName());
        Assertions.assertEquals("p", request.getPartitionName());
        Assertions.assertEquals(Collections.singletonList(1L), request.getIds());
    }

    @Test
    void testInsertRespSettersRoundTrip() {
        InsertResp resp = InsertResp.builder().build();
        resp.setInsertCnt(5L);
        resp.setPrimaryKeys(Arrays.asList(1L, 2L));
        resp.setCost(10L);
        Assertions.assertEquals(5L, resp.getInsertCnt());
        Assertions.assertEquals(Arrays.asList(1L, 2L), resp.getPrimaryKeys());
        Assertions.assertEquals(10L, resp.getCost());
    }

    @Test
    void testUpsertRespSettersRoundTrip() {
        UpsertResp resp = UpsertResp.builder().build();
        resp.setUpsertCnt(5L);
        resp.setPrimaryKeys(Arrays.asList(1L, 2L));
        resp.setCost(10L);
        Assertions.assertEquals(5L, resp.getUpsertCnt());
        Assertions.assertEquals(Arrays.asList(1L, 2L), resp.getPrimaryKeys());
        Assertions.assertEquals(10L, resp.getCost());
    }

    @Test
    void testDeleteRespSettersRoundTrip() {
        DeleteResp resp = DeleteResp.builder().build();
        resp.setDeleteCnt(5L);
        resp.setCost(10L);
        Assertions.assertEquals(5L, resp.getDeleteCnt());
        Assertions.assertEquals(10L, resp.getCost());
    }

    @Test
    void testDeleteConsistencyLevelSetOnWire() {
        DeleteReq request = DeleteReq.builder()
                .collectionName("test")
                .filter("id > 0")
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build();
        client_v2.delete(request);

        ArgumentCaptor<DeleteRequest> captor = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(blockingStub).delete(captor.capture());
        Assertions.assertEquals(ConsistencyLevel.STRONG.getCode(), captor.getValue().getConsistencyLevelValue());
    }

    @Test
    void testQueryWithoutConsistencyUsesDefaultConsistency() {
        QueryReq request = QueryReq.builder()
                .collectionName("test")
                .filter("id > 0")
                .build();
        client_v2.query(request);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        Assertions.assertTrue(captor.getValue().getUseDefaultConsistency());
    }

    @Test
    void testAsyncDqlRetriesReusePreparedRpcRequests() throws Exception {
        ThreadLocal<String> requestId = configureClientRequestId();
        client_v2.retryConfig(io.milvus.v2.client.RetryConfig.builder()
                .maxRetryTimes(2)
                .initialBackOffMs(0)
                .maxBackOffMs(0)
                .build());

        SettableFuture<QueryResults> firstQuery = SettableFuture.create();
        when(futureStub.query(any())).thenReturn(firstQuery,
                Futures.immediateFuture(QueryResults.newBuilder().build()));
        QueryReq queryReq = QueryReq.builder()
                .collectionName("query_original")
                .filter("id > 0")
                .build();
        requestId.set("query-request");
        CompletableFuture<QueryResp> queryFuture = client_v2.queryAsync(queryReq);
        requestId.set("changed-after-query");
        queryReq.setCollectionName("query_mutated");
        firstQuery.setException(io.grpc.Status.UNAVAILABLE.asRuntimeException());
        queryFuture.get(1, TimeUnit.SECONDS);
        ArgumentCaptor<QueryRequest> queryCaptor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(futureStub, times(2)).query(queryCaptor.capture());
        queryCaptor.getAllValues().forEach(rpcRequest ->
                Assertions.assertEquals("query_original", rpcRequest.getCollectionName()));
        verify(futureStub, times(2)).withOption(
                ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION, "query-request");

        clearInvocations(futureStub);
        SettableFuture<SearchResults> firstSearch = SettableFuture.create();
        when(futureStub.search(any())).thenReturn(firstSearch,
                Futures.immediateFuture(SearchResults.newBuilder().build()));
        SearchReq searchReq = SearchReq.builder()
                .collectionName("search_original")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        requestId.set("search-request");
        CompletableFuture<SearchResp> searchFuture = client_v2.searchAsync(searchReq);
        requestId.set("changed-after-search");
        searchReq.setCollectionName("search_mutated");
        firstSearch.setException(io.grpc.Status.UNAVAILABLE.asRuntimeException());
        searchFuture.get(1, TimeUnit.SECONDS);
        ArgumentCaptor<SearchRequest> searchCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(futureStub, times(2)).search(searchCaptor.capture());
        searchCaptor.getAllValues().forEach(rpcRequest ->
                Assertions.assertEquals("search_original", rpcRequest.getCollectionName()));
        verify(futureStub, times(2)).withOption(
                ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION, "search-request");

        clearInvocations(futureStub);
        SettableFuture<SearchResults> firstHybridSearch = SettableFuture.create();
        when(futureStub.hybridSearch(any())).thenReturn(firstHybridSearch,
                Futures.immediateFuture(SearchResults.newBuilder().build()));
        AnnSearchReq annSearchReq = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        HybridSearchReq hybridSearchReq = HybridSearchReq.builder()
                .collectionName("hybrid_original")
                .searchRequests(Collections.singletonList(annSearchReq))
                .limit(10)
                .build();
        requestId.set("hybrid-request");
        CompletableFuture<SearchResp> hybridFuture = client_v2.hybridSearchAsync(hybridSearchReq);
        requestId.set("changed-after-hybrid");
        hybridSearchReq.setCollectionName("hybrid_mutated");
        annSearchReq.setVectorFieldName("mutated_vector");
        firstHybridSearch.setException(io.grpc.Status.UNAVAILABLE.asRuntimeException());
        hybridFuture.get(1, TimeUnit.SECONDS);
        ArgumentCaptor<HybridSearchRequest> hybridCaptor = ArgumentCaptor.forClass(HybridSearchRequest.class);
        verify(futureStub, times(2)).hybridSearch(hybridCaptor.capture());
        hybridCaptor.getAllValues().forEach(rpcRequest -> {
            Assertions.assertEquals("hybrid_original", rpcRequest.getCollectionName());
            Assertions.assertEquals("vector",
                    getParam(rpcRequest.getRequests(0).getSearchParamsList(), Constant.VECTOR_FIELD));
        });
        verify(futureStub, times(2)).withOption(
                ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION, "hybrid-request");
        requestId.remove();
    }

    @Test
    void testSearchWithTemplateExpression() {
        List<Float> vectorList = new ArrayList<>();
        vectorList.add(1.0f);
        vectorList.add(2.0f);

        Map<String, Map<String, Object>> expressionTemplateValues = new HashMap<>();
        Map<String, Object> params = new HashMap<>();
        params.put("max", 10);
        expressionTemplateValues.put("id < {max}", params);

        List<Object> list = Arrays.asList(1, 2, 3);
        Map<String, Object> params2 = new HashMap<>();
        params2.put("list", list);
        expressionTemplateValues.put("id in {list}", params2);

        expressionTemplateValues.forEach((key, value) -> {
            SearchReq request = SearchReq.builder()
                    .collectionName("test")
                    .data(Collections.singletonList(new FloatVec(vectorList)))
                    .limit(10)
                    .offset(0L)
                    .filter(key)
                    .filterTemplateValues(value)
                    .build();
            SearchResp statusR = client_v2.search(request);
            logger.info(statusR.toString());
            System.out.println(statusR);
        });
    }

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
    void testGetAsync() throws Exception {
        SchemaCache.getInstance().clear();
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
    void testCallAfterCloseFailsFast() throws Exception {
        client_v2.close(3);

        GetReq getRequest = GetReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .build();
        QueryReq queryRequest = QueryReq.builder()
                .collectionName("book")
                .filter("id > 0")
                .build();
        SearchReq searchRequest = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();

        assertSyncCallFailsAfterClose(() -> client_v2.get(getRequest));
        assertSyncCallFailsAfterClose(() -> client_v2.query(queryRequest));
        assertSyncCallFailsAfterClose(() -> client_v2.search(searchRequest));

        assertAsyncCallFailsAfterClose(client_v2.getAsync(getRequest));
        assertAsyncCallFailsAfterClose(client_v2.queryAsync(queryRequest));
        assertAsyncCallFailsAfterClose(client_v2.searchAsync(searchRequest));
        assertAsyncCallFailsAfterClose(client_v2.hybridSearchAsync(HybridSearchReq.builder()
                .collectionName("test")
                .searchRequests(Collections.singletonList(AnnSearchReq.builder()
                        .vectorFieldName("vector")
                        .vectors(Collections.singletonList(
                                new FloatVec(Arrays.asList(1.0f, 2.0f))))
                        .limit(10)
                        .build()))
                .limit(10)
                .build()));

        verify(blockingStub, never()).query(any(QueryRequest.class));
        verify(blockingStub, never()).search(any(SearchRequest.class));
        verify(futureStub, never()).query(any(QueryRequest.class));
        verify(futureStub, never()).search(any(SearchRequest.class));
        verify(futureStub, never()).hybridSearch(any(HybridSearchRequest.class));
    }

    private static void assertSyncCallFailsAfterClose(Supplier<?> call) {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                call::get);
        Assertions.assertEquals(ErrorCode.CLIENT_ERROR, exception.getErrorCode());
        Assertions.assertEquals("MilvusClient is closed", exception.getMessage());
    }

    private static <T> void assertAsyncCallFailsAfterClose(CompletableFuture<T> future) throws Exception {
        ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                () -> future.get(1, TimeUnit.SECONDS));
        Assertions.assertTrue(exception.getCause() instanceof MilvusClientException);
        Assertions.assertEquals(ErrorCode.CLIENT_ERROR,
                ((MilvusClientException) exception.getCause()).getErrorCode());
        Assertions.assertEquals("MilvusClient is closed", exception.getCause().getMessage());
    }

    private DescribeCollectionResponse describeCollectionResponse() {
        return DescribeCollectionResponse.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .setCollectionName("book")
                .setSchema(CollectionSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setName("id")
                                .setDataType(DataType.Int64)
                                .setIsPrimaryKey(true)
                                .build())
                        .addFields(FieldSchema.newBuilder()
                                .setName("vector")
                                .setDataType(DataType.FloatVector)
                                .addTypeParams(KeyValuePair.newBuilder()
                                        .setKey("dim")
                                        .setValue("2")
                                        .build())
                                .build())
                        .build())
                .build();
    }

    private ThreadLocal<String> configureClientRequestId() throws ReflectiveOperationException {
        ThreadLocal<String> requestId = new ThreadLocal<>();
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .clientRequestId(requestId)
                .build();
        Field connectConfig = MilvusClientV2.class.getDeclaredField("connectConfig");
        connectConfig.setAccessible(true);
        connectConfig.set(client_v2, config);
        return requestId;
    }

    private String getParam(List<KeyValuePair> params, String key) {
        return params.stream()
                .filter(param -> key.equals(param.getKey()))
                .map(KeyValuePair::getValue)
                .findFirst()
                .orElse(null);
    }
}
