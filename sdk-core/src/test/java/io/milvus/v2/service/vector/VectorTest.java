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
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.FunctionChain;
import io.milvus.v2.service.vector.request.FunctionChainArg;
import io.milvus.v2.service.vector.request.FunctionChainExpr;
import io.milvus.v2.service.vector.request.FunctionChainOp;
import io.milvus.v2.service.vector.request.FunctionChainStage;
import io.milvus.v2.service.vector.request.aggregation.AggDirection;
import io.milvus.v2.service.vector.request.aggregation.MetricOps;
import io.milvus.v2.service.vector.request.aggregation.MetricSpec;
import io.milvus.v2.service.vector.request.aggregation.OrderByField;
import io.milvus.v2.service.vector.request.aggregation.OrderSpec;
import io.milvus.v2.service.vector.request.aggregation.SearchAggregation;
import io.milvus.v2.service.vector.request.aggregation.SortSpec;
import io.milvus.v2.service.vector.request.aggregation.TopHitsSpec;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.*;
import io.milvus.v2.service.vector.response.aggregation.AggregationBucket;
import io.milvus.v2.service.vector.response.aggregation.AggregationHit;
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
    void testInsertAndUpsertExposeServerCost() {
        MutationResult result = MutationResult.newBuilder()
                .setInsertCnt(2L)
                .setUpsertCnt(2L)
                .setStatus(Status.newBuilder().setCode(0)
                        .putExtraInfo("report_value", "123")
                        .build())
                .build();
        when(blockingStub.insert(any())).thenReturn(result);
        when(blockingStub.upsert(any())).thenReturn(result);

        JsonObject row = new JsonObject();
        row.add("vector", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));
        row.addProperty("id", 1L);

        InsertResp insertResp = client_v2.insert(InsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(row))
                .build());
        Assertions.assertEquals(123L, insertResp.getCost());

        UpsertResp upsertResp = client_v2.upsert(UpsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(row))
                .build());
        Assertions.assertEquals(123L, upsertResp.getCost());
    }

    @Test
    void testDeleteExposesCost() {
        MutationResult result = MutationResult.newBuilder()
                .setDeleteCnt(2L)
                .setStatus(Status.newBuilder().setCode(0)
                        .putExtraInfo("report_value", "456")
                        .build())
                .build();
        when(blockingStub.delete(any())).thenReturn(result);

        DeleteResp resp = client_v2.delete(DeleteReq.builder()
                .collectionName("test")
                .ids(Arrays.asList(10L, 20L))
                .build());

        Assertions.assertEquals(2L, resp.getDeleteCnt());
        Assertions.assertEquals(456L, resp.getCost());
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
    void testQueryExposesServerCost() {
        QueryResults response = QueryResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0)
                        .putExtraInfo("report_value", "321")
                        .build())
                .setSessionTs(100L)
                .build();
        when(blockingStub.query(any())).thenReturn(response);

        QueryResp resp = client_v2.query(QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .limit(10)
                .build());

        Assertions.assertEquals(321L, resp.getCost());
        Assertions.assertEquals(100L, resp.getSessionTs());
    }

    @Test
    void testQueryCostDefaultsToZeroWhenReportValueMissingOrInvalid() {
        QueryResults absent = QueryResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .build();
        when(blockingStub.query(any())).thenReturn(absent);
        QueryResp respAbsent = client_v2.query(QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .build());
        Assertions.assertEquals(0L, respAbsent.getCost());

        QueryResults invalid = QueryResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).putExtraInfo("report_value", "not-a-number").build())
                .build();
        when(blockingStub.query(any())).thenReturn(invalid);
        QueryResp respInvalid = client_v2.query(QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .build());
        Assertions.assertEquals(0L, respInvalid.getCost());
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
            requestId.set("11111111111111111111111111111111");
            return client_v2.query(request);
        });

        try {
            Assertions.assertTrue(schemaLoadStarted.await(5, TimeUnit.SECONDS));
            requestId.set("22222222222222222222222222222222");
            CompletableFuture<QueryResp> asynchronous = client_v2.queryAsync(request);

            requestId.set("33333333333333333333333333333333");
            releaseSchemaLoad.countDown();

            Assertions.assertNotNull(synchronous.get(1, TimeUnit.SECONDS));
            Assertions.assertNotNull(asynchronous.get(1, TimeUnit.SECONDS));
            verify(futureStub).withOption(
                    ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION,
                    "22222222222222222222222222222222");
            verify(futureStub, never()).withOption(
                    ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION,
                    "11111111111111111111111111111111");
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
            CompletableFuture<QueryResp> first = client_v2.session("cluster-a").queryAsync(request);
            CompletableFuture<QueryResp> second = client_v2.session("cluster-b").queryAsync(request);
            Assertions.assertNull(request.getClusterId());

            request.setDatabaseName("mutated_db");
            request.setCollectionName("mutated_collection");
            request.setClusterId("cluster-c");
            request.setLimit(20);
            outputFields.add("mutated_field");

            schemaFuture.set(describeCollectionResponse());
            first.get(1, TimeUnit.SECONDS);
            second.get(1, TimeUnit.SECONDS);

            ArgumentCaptor<QueryRequest> queryCaptor = ArgumentCaptor.forClass(QueryRequest.class);
            verify(futureStub, times(2)).query(queryCaptor.capture());
            Set<String> clusterIds = new HashSet<>();
            for (QueryRequest rpcRequest : queryCaptor.getAllValues()) {
                Assertions.assertEquals("original_db", rpcRequest.getDbName());
                Assertions.assertEquals("original_collection", rpcRequest.getCollectionName());
                Assertions.assertEquals(Collections.singletonList("original_field"),
                        rpcRequest.getOutputFieldsList());
                Assertions.assertEquals("10", getParam(rpcRequest.getQueryParamsList(), Constant.LIMIT));
                clusterIds.add(getParam(rpcRequest.getQueryParamsList(), Constant.CLUSTER_ID));
            }
            Assertions.assertEquals(new HashSet<>(Arrays.asList("cluster-a", "cluster-b")), clusterIds);
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
    void testSearchWithFunctionChains() {
        FunctionChain chain = FunctionChain.builder()
                .stage(FunctionChainStage.L2_RERANK)
                .name("rerank_chain")
                .map("$score", FunctionChainExpr.builder()
                        .name("num_combine")
                        .arg(FunctionChainArg.col("$score"))
                        .arg(FunctionChainArg.col("freshness"))
                        .arg(FunctionChainArg.literal(0.5))
                        .param("mode", "weighted")
                        .param("weights", Arrays.asList(0.7, 0.2, 0.1))
                        .build())
                .sort("$score", true, "$id")
                .limit(10)
                .build();
        SearchReq request = SearchReq.builder()
                .collectionName("test2")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .functionChains(Collections.singletonList(chain))
                .build();

        client_v2.search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        io.milvus.grpc.FunctionChain grpcChain = captor.getValue().getFunctionChains(0);
        Assertions.assertEquals("rerank_chain", grpcChain.getName());
        Assertions.assertEquals(io.milvus.grpc.FunctionChainStage.FunctionChainStageL2Rerank, grpcChain.getStage());
        Assertions.assertEquals(3, grpcChain.getOpsCount());

        // map op: expr name/args/params plus outputs
        io.milvus.grpc.FunctionChainOp mapOp = grpcChain.getOps(0);
        Assertions.assertEquals("map", mapOp.getOp());
        Assertions.assertEquals("num_combine", mapOp.getExpr().getName());
        Assertions.assertEquals(3, mapOp.getExpr().getArgsCount());
        Assertions.assertEquals("$score", mapOp.getExpr().getArgs(0).getColumn().getName());
        Assertions.assertEquals("freshness", mapOp.getExpr().getArgs(1).getColumn().getName());
        Assertions.assertEquals(0.5, mapOp.getExpr().getArgs(2).getLiteral().getDoubleValue());
        Assertions.assertEquals("weighted", mapOp.getExpr().getParamsOrThrow("mode").getStringValue());
        Assertions.assertEquals(3, mapOp.getExpr().getParamsOrThrow("weights").getArrayValue().getValuesCount());
        Assertions.assertEquals(1, mapOp.getOutputsCount());
        Assertions.assertEquals("$score", mapOp.getOutputs(0));

        // sort op: inputs plus column/desc/tie_break_col params
        io.milvus.grpc.FunctionChainOp sortOp = grpcChain.getOps(1);
        Assertions.assertEquals("sort", sortOp.getOp());
        Assertions.assertEquals(Arrays.asList("$score", "$id"), sortOp.getInputsList());
        Assertions.assertEquals("$score", sortOp.getParamsOrThrow("column").getStringValue());
        Assertions.assertTrue(sortOp.getParamsOrThrow("desc").getBoolValue());
        Assertions.assertEquals("$id", sortOp.getParamsOrThrow("tie_break_col").getStringValue());

        // limit op: limit plus offset params
        io.milvus.grpc.FunctionChainOp limitOp = grpcChain.getOps(2);
        Assertions.assertEquals("limit", limitOp.getOp());
        Assertions.assertEquals(10L, limitOp.getParamsOrThrow("limit").getInt64Value());
        Assertions.assertEquals(0L, limitOp.getParamsOrThrow("offset").getInt64Value());
    }

    @Test
    void testSearchRejectsFunctionChainsWithRanker() {
        SearchReq request = SearchReq.builder()
                .collectionName("test2")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .functionChains(Collections.singletonList(FunctionChain.builder()
                        .stage(FunctionChainStage.L2_RERANK)
                        .build()))
                .ranker(CreateCollectionReq.Function.builder()
                        .name("rrf")
                        .build())
                .build();

        MilvusClientException ex = Assertions.assertThrows(MilvusClientException.class, () -> client_v2.search(request));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, ex.getErrorCode());
    }

    @Test
    void testSearchRejectsUnspecifiedFunctionChainStage() {
        SearchReq request = SearchReq.builder()
                .collectionName("test2")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .functionChains(Collections.singletonList(FunctionChain.builder()
                        .stage(FunctionChainStage.UNSPECIFIED)
                        .build()))
                .build();

        MilvusClientException ex = Assertions.assertThrows(MilvusClientException.class, () -> client_v2.search(request));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, ex.getErrorCode());
    }

    @Test
    void testFunctionParamValueFrom() {
        // scalar types
        io.milvus.grpc.FunctionParamValue boolValue =
                io.milvus.v2.service.vector.request.FunctionParamValue.from(true).toGrpc();
        Assertions.assertEquals(io.milvus.grpc.FunctionParamValue.ValueCase.BOOL_VALUE, boolValue.getValueCase());
        Assertions.assertTrue(boolValue.getBoolValue());

        io.milvus.grpc.FunctionParamValue intValue =
                io.milvus.v2.service.vector.request.FunctionParamValue.from(42).toGrpc();
        Assertions.assertEquals(io.milvus.grpc.FunctionParamValue.ValueCase.INT64_VALUE, intValue.getValueCase());
        Assertions.assertEquals(42L, intValue.getInt64Value());

        Assertions.assertEquals(42L, io.milvus.v2.service.vector.request.FunctionParamValue.from(42L).toGrpc().getInt64Value());
        Assertions.assertEquals(42L, io.milvus.v2.service.vector.request.FunctionParamValue.from((short) 42).toGrpc().getInt64Value());
        Assertions.assertEquals(42L, io.milvus.v2.service.vector.request.FunctionParamValue.from((byte) 42).toGrpc().getInt64Value());

        io.milvus.grpc.FunctionParamValue doubleValue =
                io.milvus.v2.service.vector.request.FunctionParamValue.from(0.5).toGrpc();
        Assertions.assertEquals(io.milvus.grpc.FunctionParamValue.ValueCase.DOUBLE_VALUE, doubleValue.getValueCase());
        Assertions.assertEquals(0.5, doubleValue.getDoubleValue());
        Assertions.assertEquals(0.5, io.milvus.v2.service.vector.request.FunctionParamValue.from(0.5f).toGrpc().getDoubleValue());

        io.milvus.grpc.FunctionParamValue stringValue =
                io.milvus.v2.service.vector.request.FunctionParamValue.from("weighted").toGrpc();
        Assertions.assertEquals(io.milvus.grpc.FunctionParamValue.ValueCase.STRING_VALUE, stringValue.getValueCase());
        Assertions.assertEquals("weighted", stringValue.getStringValue());

        byte[] bytes = new byte[]{1, 2, 3};
        io.milvus.grpc.FunctionParamValue bytesValue =
                io.milvus.v2.service.vector.request.FunctionParamValue.from(bytes).toGrpc();
        Assertions.assertEquals(io.milvus.grpc.FunctionParamValue.ValueCase.BYTES_VALUE, bytesValue.getValueCase());
        Assertions.assertArrayEquals(bytes, bytesValue.getBytesValue().toByteArray());

        // container types, recursively converted
        io.milvus.grpc.FunctionParamValue arrayValue =
                io.milvus.v2.service.vector.request.FunctionParamValue.from(Arrays.asList(1, 2.0, "three")).toGrpc();
        Assertions.assertEquals(io.milvus.grpc.FunctionParamValue.ValueCase.ARRAY_VALUE, arrayValue.getValueCase());
        Assertions.assertEquals(3, arrayValue.getArrayValue().getValuesCount());
        Assertions.assertEquals(1L, arrayValue.getArrayValue().getValues(0).getInt64Value());
        Assertions.assertEquals(2.0, arrayValue.getArrayValue().getValues(1).getDoubleValue());
        Assertions.assertEquals("three", arrayValue.getArrayValue().getValues(2).getStringValue());

        Map<String, Object> objectInput = new LinkedHashMap<>();
        objectInput.put("mode", "sum");
        objectInput.put("weights", Arrays.asList(0.7, 0.3));
        io.milvus.grpc.FunctionParamValue objectValue =
                io.milvus.v2.service.vector.request.FunctionParamValue.from(objectInput).toGrpc();
        Assertions.assertEquals(io.milvus.grpc.FunctionParamValue.ValueCase.OBJECT_VALUE, objectValue.getValueCase());
        Assertions.assertEquals("sum", objectValue.getObjectValue().getFieldsOrThrow("mode").getStringValue());
        Assertions.assertEquals(2, objectValue.getObjectValue().getFieldsOrThrow("weights").getArrayValue().getValuesCount());

        // a FunctionParamValue passes through unchanged
        io.milvus.v2.service.vector.request.FunctionParamValue passthrough =
                io.milvus.v2.service.vector.request.FunctionParamValue.of(7L);
        Assertions.assertSame(passthrough, io.milvus.v2.service.vector.request.FunctionParamValue.from(passthrough));

        // error paths
        MilvusClientException nullEx = Assertions.assertThrows(MilvusClientException.class,
                () -> io.milvus.v2.service.vector.request.FunctionParamValue.from(null));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, nullEx.getErrorCode());

        MilvusClientException unsupportedEx = Assertions.assertThrows(MilvusClientException.class,
                () -> io.milvus.v2.service.vector.request.FunctionParamValue.from(new Object()));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, unsupportedEx.getErrorCode());

        Map<Object, Object> badKeyInput = new HashMap<>();
        badKeyInput.put(1, "x");
        MilvusClientException badKeyEx = Assertions.assertThrows(MilvusClientException.class,
                () -> io.milvus.v2.service.vector.request.FunctionParamValue.from(badKeyInput));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, badKeyEx.getErrorCode());
    }

    @Test
    void testFunctionParamValueFromAdditionalTypes() {
        // BigInteger within int64 range maps to int64
        Assertions.assertEquals(42L, io.milvus.v2.service.vector.request.FunctionParamValue
                .from(java.math.BigInteger.valueOf(42)).toGrpc().getInt64Value());
        // BigInteger out of int64 range is rejected with a clear error
        MilvusClientException overflowEx = Assertions.assertThrows(MilvusClientException.class,
                () -> io.milvus.v2.service.vector.request.FunctionParamValue
                        .from(java.math.BigInteger.valueOf(Long.MAX_VALUE).add(java.math.BigInteger.ONE)));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, overflowEx.getErrorCode());

        // Character maps to a one-char string
        Assertions.assertEquals("x", io.milvus.v2.service.vector.request.FunctionParamValue
                .from('x').toGrpc().getStringValue());

        // char[] maps to UTF-8 bytes
        Assertions.assertArrayEquals("hi".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                io.milvus.v2.service.vector.request.FunctionParamValue
                        .from(new char[]{'h', 'i'}).toGrpc().getBytesValue().toByteArray());
    }

    @Test
    void testFunctionChainFactoryValidation() {
        // map: empty output
        MilvusClientException emptyMapOutput = Assertions.assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().map("", FunctionChainExpr.builder().name("f").build()).build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, emptyMapOutput.getErrorCode());

        // map: null expr
        MilvusClientException nullMapExpr = Assertions.assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().map("$score", null).build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, nullMapExpr.getErrorCode());

        // sort: empty by
        MilvusClientException emptySortBy = Assertions.assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().sort("", true, null).build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, emptySortBy.getErrorCode());

        // limit: non-positive limit
        MilvusClientException nonPositiveLimit = Assertions.assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().limit(0).build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, nonPositiveLimit.getErrorCode());

        // limit: negative offset
        MilvusClientException negativeOffset = Assertions.assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().limit(10, -1).build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, negativeOffset.getErrorCode());
    }

    @Test
    void testFunctionChainBuilderNullGuards() {
        // null stage is rejected eagerly instead of failing at serialization
        MilvusClientException nullStage = Assertions.assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().stage(null).build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, nullStage.getErrorCode());

        // null op params are rejected with a typed error
        MilvusClientException nullParams = Assertions.assertThrows(MilvusClientException.class,
                () -> FunctionChainOp.builder().op("map").params(null).build());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, nullParams.getErrorCode());
    }

    @Test
    void testSearchRejectsNullFunctionChainElement() {
        // addFunctionChain(null) is rejected eagerly
        MilvusClientException nullAdd = Assertions.assertThrows(MilvusClientException.class,
                () -> SearchReq.builder().addFunctionChain(null));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, nullAdd.getErrorCode());

        // a null element inside the functionChains list is rejected at conversion time
        FunctionChain chain = FunctionChain.builder().stage(FunctionChainStage.L2_RERANK).build();
        SearchReq request = SearchReq.builder()
                .collectionName("test2")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .functionChains(Arrays.asList(chain, null))
                .build();
        MilvusClientException nullElement = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.search(request));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, nullElement.getErrorCode());
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
        requestId.set("44444444444444444444444444444444");
        CompletableFuture<QueryResp> queryFuture = client_v2.queryAsync(queryReq);
        requestId.set("55555555555555555555555555555555");
        queryReq.setCollectionName("query_mutated");
        firstQuery.setException(io.grpc.Status.UNAVAILABLE.asRuntimeException());
        queryFuture.get(1, TimeUnit.SECONDS);
        ArgumentCaptor<QueryRequest> queryCaptor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(futureStub, times(2)).query(queryCaptor.capture());
        queryCaptor.getAllValues().forEach(rpcRequest ->
                Assertions.assertEquals("query_original", rpcRequest.getCollectionName()));
        verify(futureStub, times(2)).withOption(
                ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION,
                "44444444444444444444444444444444");

        clearInvocations(futureStub);
        SettableFuture<SearchResults> firstSearch = SettableFuture.create();
        when(futureStub.search(any())).thenReturn(firstSearch,
                Futures.immediateFuture(SearchResults.newBuilder().build()));
        SearchReq searchReq = SearchReq.builder()
                .collectionName("search_original")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        requestId.set("66666666666666666666666666666666");
        CompletableFuture<SearchResp> searchFuture = client_v2.searchAsync(searchReq);
        requestId.set("77777777777777777777777777777777");
        searchReq.setCollectionName("search_mutated");
        firstSearch.setException(io.grpc.Status.UNAVAILABLE.asRuntimeException());
        searchFuture.get(1, TimeUnit.SECONDS);
        ArgumentCaptor<SearchRequest> searchCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(futureStub, times(2)).search(searchCaptor.capture());
        searchCaptor.getAllValues().forEach(rpcRequest ->
                Assertions.assertEquals("search_original", rpcRequest.getCollectionName()));
        verify(futureStub, times(2)).withOption(
                ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION,
                "66666666666666666666666666666666");

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
        requestId.set("88888888888888888888888888888888");
        CompletableFuture<SearchResp> hybridFuture = client_v2.hybridSearchAsync(hybridSearchReq);
        requestId.set("99999999999999999999999999999999");
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
                ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION,
                "88888888888888888888888888888888");
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

    private static <T> void assertAsyncCallFailsAfterClose(CompletableFuture<T> future) {
        ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                () -> future.get(1, TimeUnit.SECONDS));
        Assertions.assertTrue(exception.getCause() instanceof MilvusClientException);
        Assertions.assertEquals(ErrorCode.CLIENT_ERROR,
                ((MilvusClientException) exception.getCause()).getErrorCode());
        Assertions.assertEquals("MilvusClient is closed", exception.getCause().getMessage());
    }

    @Test
    void testQueryOrderByFieldsSerialization() {
        QueryReq request = QueryReq.builder()
                .collectionName("test")
                .filter("id > 0")
                .orderByFields(Arrays.asList(
                        OrderByField.builder().fieldName("price").build(),
                        OrderByField.builder().fieldName("rating").direction(AggDirection.DESC).build()))
                .build();

        client_v2.query(request);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        Assertions.assertEquals("price:asc,rating:desc", getParam(captor.getValue().getQueryParamsList(), Constant.ORDER_BY_FIELDS));
    }

    @Test
    void testSearchOrderByFieldsSerialization() {
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .orderByFields(Arrays.asList(
                        OrderByField.builder().fieldName("price").direction(AggDirection.ASC).build(),
                        OrderByField.builder().fieldName("rating").direction(AggDirection.DESC).build()))
                .build();

        client_v2.search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        Assertions.assertEquals("price:asc,rating:desc", getParam(captor.getValue().getSearchParamsList(), Constant.ORDER_BY_FIELDS));
    }

    @Test
    void testSessionReturnsClusterId() {
        Assertions.assertEquals("cluster-a", client_v2.session("cluster-a").getClusterId());
    }

    @Test
    void testSessionSearchPassesClusterId() {
        List<Float> vectorList = Arrays.asList(1.0f, 2.0f);
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(vectorList)))
                .limit(10)
                .build();

        client_v2.session("cluster-a").search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getSearchParamsList(), Constant.CLUSTER_ID));
        Assertions.assertNull(request.getClusterId());
    }

    @Test
    void testSessionQueryPassesClusterId() {
        QueryReq request = QueryReq.builder()
                .collectionName("test")
                .clusterId("cluster-b")
                .filter("id > 0")
                .build();

        client_v2.session("cluster-a").query(request);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getQueryParamsList(), Constant.CLUSTER_ID));
        Assertions.assertEquals(1, captor.getValue().getQueryParamsList().stream()
                .filter(param -> Constant.CLUSTER_ID.equals(param.getKey())).count());
        Assertions.assertEquals("cluster-b", request.getClusterId());
    }

    @Test
    void testSessionGetPassesClusterId() {
        GetReq request = GetReq.builder()
                .collectionName("test")
                .ids(Collections.singletonList(1L))
                .build();

        client_v2.session("cluster-a").get(request);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getQueryParamsList(), Constant.CLUSTER_ID));
        Assertions.assertNull(request.getClusterId());
    }

    @Test
    void testSessionHybridSearchPassesClusterId() {
        List<Float> vectorList = Arrays.asList(1.0f, 2.0f);
        AnnSearchReq annSearchReq = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(new FloatVec(vectorList)))
                .limit(10)
                .build();
        HybridSearchReq request = HybridSearchReq.builder()
                .collectionName("test")
                .clusterId("cluster-b")
                .searchRequests(Collections.singletonList(annSearchReq))
                .limit(10)
                .build();

        client_v2.session("cluster-a").hybridSearch(request);

        ArgumentCaptor<HybridSearchRequest> captor = ArgumentCaptor.forClass(HybridSearchRequest.class);
        verify(blockingStub).hybridSearch(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getRankParamsList(), Constant.CLUSTER_ID));
        Assertions.assertEquals(1, captor.getValue().getRankParamsList().stream()
                .filter(param -> Constant.CLUSTER_ID.equals(param.getKey())).count());
        Assertions.assertEquals("cluster-b", request.getClusterId());
    }

    @Test
    void testSessionAsyncOperationsPassClusterId() throws Exception {
        SearchReq searchRequest = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        QueryReq queryRequest = QueryReq.builder()
                .collectionName("test")
                .filter("id > 0")
                .build();
        AnnSearchReq annSearchReq = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        HybridSearchReq hybridSearchRequest = HybridSearchReq.builder()
                .collectionName("test")
                .searchRequests(Collections.singletonList(annSearchReq))
                .limit(10)
                .build();
        GetReq getRequest = GetReq.builder()
                .collectionName("test")
                .ids(Collections.singletonList(1L))
                .build();

        client_v2.session("cluster-a").searchAsync(searchRequest).get(1, TimeUnit.SECONDS);
        client_v2.session("cluster-a").queryAsync(queryRequest).get(1, TimeUnit.SECONDS);
        client_v2.session("cluster-a").hybridSearchAsync(hybridSearchRequest).get(1, TimeUnit.SECONDS);
        client_v2.session("cluster-a").getAsync(getRequest).get(1, TimeUnit.SECONDS);

        Assertions.assertNull(searchRequest.getClusterId());
        Assertions.assertNull(queryRequest.getClusterId());
        Assertions.assertNull(hybridSearchRequest.getClusterId());
        Assertions.assertNull(getRequest.getClusterId());

        ArgumentCaptor<SearchRequest> searchCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(futureStub).search(searchCaptor.capture());
        Assertions.assertEquals("cluster-a",
                getParam(searchCaptor.getValue().getSearchParamsList(), Constant.CLUSTER_ID));

        ArgumentCaptor<QueryRequest> queryCaptor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(futureStub, times(2)).query(queryCaptor.capture());
        for (QueryRequest rpcRequest : queryCaptor.getAllValues()) {
            Assertions.assertEquals("cluster-a",
                    getParam(rpcRequest.getQueryParamsList(), Constant.CLUSTER_ID));
        }

        ArgumentCaptor<HybridSearchRequest> hybridCaptor = ArgumentCaptor.forClass(HybridSearchRequest.class);
        verify(futureStub).hybridSearch(hybridCaptor.capture());
        Assertions.assertEquals("cluster-a",
                getParam(hybridCaptor.getValue().getRankParamsList(), Constant.CLUSTER_ID));
    }

    @Test
    void testSessionQueryIteratorPassesClusterId() throws ReflectiveOperationException {
        setIteratorConnectConfig();
        QueryIteratorReq request = QueryIteratorReq.builder()
                .collectionName("test")
                .expr("id > 0")
                .batchSize(10)
                .build();

        Assertions.assertNotNull(client_v2.session("cluster-a").queryIterator(request));

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getQueryParamsList(), Constant.CLUSTER_ID));
        Assertions.assertNull(request.getClusterId());
    }

    @Test
    void testSessionSearchIteratorPassesClusterId() throws ReflectiveOperationException {
        setIteratorConnectConfig();
        SearchIteratorReq request = SearchIteratorReq.builder()
                .collectionName("test")
                .vectorFieldName("vector")
                .metricType(IndexParam.MetricType.L2)
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .batchSize(10)
                .build();

        Assertions.assertNotNull(client_v2.session("cluster-a").searchIterator(request));

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getSearchParamsList(), Constant.CLUSTER_ID));
        Assertions.assertNull(request.getClusterId());
    }

    @Test
    void testSessionSearchIteratorV2PassesClusterId() throws ReflectiveOperationException {
        setIteratorConnectConfig();
        SearchResults iteratorResponse = SearchResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .setResults(SearchResultData.newBuilder()
                        .setSearchIteratorV2Results(SearchIteratorV2Results.newBuilder().setToken("token").build())
                        .build())
                .build();
        when(blockingStub.search(any(SearchRequest.class))).thenReturn(iteratorResponse);

        SearchIteratorReqV2 request = SearchIteratorReqV2.builder()
                .collectionName("test")
                .vectorFieldName("vector")
                .metricType(IndexParam.MetricType.L2)
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .batchSize(10)
                .build();

        Assertions.assertNotNull(client_v2.session("cluster-a").searchIteratorV2(request));

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getSearchParamsList(), Constant.CLUSTER_ID));
        Assertions.assertNull(request.getClusterId());
    }

    @Test
    void testSessionOverridesRequestClusterIdAndRejectsClosed() {
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .clusterId("cluster-b")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();

        client_v2.session("cluster-a").search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getSearchParamsList(), Constant.CLUSTER_ID));
        Assertions.assertEquals(1, captor.getValue().getSearchParamsList().stream()
                .filter(param -> Constant.CLUSTER_ID.equals(param.getKey())).count());
        Assertions.assertEquals("cluster-b", request.getClusterId());

        io.milvus.v2.client.MilvusClientV2Session session = client_v2.session("cluster-a");
        session.close();
        SearchReq closedRequest = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        MilvusClientException closedException = Assertions.assertThrows(MilvusClientException.class,
                () -> session.search(closedRequest));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, closedException.getErrorCode());
        Assertions.assertNull(closedRequest.getClusterId());
    }

    @Test
    void testSearchSessionDeduplicatesRawClusterId() {
        Map<String, Object> rawParams = new HashMap<>();
        rawParams.put("nprobe", 10);
        rawParams.put(Constant.CLUSTER_ID, "user-cluster");
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .searchParams(rawParams)
                .build();

        client_v2.session("cluster-a").search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        List<KeyValuePair> searchParamsList = captor.getValue().getSearchParamsList();
        Assertions.assertEquals(1, searchParamsList.stream()
                .filter(param -> Constant.CLUSTER_ID.equals(param.getKey())).count());
        Assertions.assertEquals("cluster-a", getParam(searchParamsList, Constant.CLUSTER_ID));
        Assertions.assertEquals("10", getParam(searchParamsList, "nprobe"));
    }

    @Test
    void testSearchPreservesRawClusterIdWithoutSession() {
        Map<String, Object> rawParams = new HashMap<>();
        rawParams.put("nprobe", 10);
        rawParams.put(Constant.CLUSTER_ID, "user-cluster");
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .searchParams(rawParams)
                .build();

        client_v2.search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        List<KeyValuePair> searchParamsList = captor.getValue().getSearchParamsList();
        Assertions.assertEquals(1, searchParamsList.stream()
                .filter(param -> Constant.CLUSTER_ID.equals(param.getKey())).count());
        Assertions.assertEquals("user-cluster", getParam(searchParamsList, Constant.CLUSTER_ID));
    }

    @Test
    void testSearchAsyncSessionDeduplicatesRawClusterId() throws Exception {
        Map<String, Object> rawParams = new HashMap<>();
        rawParams.put("nprobe", 10);
        rawParams.put(Constant.CLUSTER_ID, "user-cluster");
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .searchParams(rawParams)
                .build();

        client_v2.session("cluster-a").searchAsync(request).get(1, TimeUnit.SECONDS);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(futureStub).search(captor.capture());
        List<KeyValuePair> searchParamsList = captor.getValue().getSearchParamsList();
        Assertions.assertEquals(1, searchParamsList.stream()
                .filter(param -> Constant.CLUSTER_ID.equals(param.getKey())).count());
        Assertions.assertEquals("cluster-a", getParam(searchParamsList, Constant.CLUSTER_ID));
        Assertions.assertEquals("10", getParam(searchParamsList, "nprobe"));
    }

    @Test
    void testOrderByFieldDefaultsToAsc() {
        OrderByField orderByField = OrderByField.builder()
                .fieldName("price")
                .build();

        Assertions.assertEquals(AggDirection.ASC, orderByField.getDirection());
    }

    @Test
    void testSearchAggregationSerialization() {
        SearchAggregation requestAggregation = buildAggregation();
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .searchAggregation(requestAggregation)
                .build();

        client_v2.search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        SearchAggregationSpec aggregation = captor.getValue().getSearchAggregation();
        Assertions.assertEquals(Arrays.asList("category", "region"), aggregation.getFieldsList());
        Assertions.assertEquals(5, aggregation.getSize());
        Assertions.assertEquals("count", aggregation.getMetricsOrThrow("doc_count").getOp());
        Assertions.assertEquals("*", aggregation.getMetricsOrThrow("doc_count").getFieldName());
        Assertions.assertEquals("avg", aggregation.getMetricsOrThrow("avg_score").getOp());
        Assertions.assertEquals("score", aggregation.getMetricsOrThrow("avg_score").getFieldName());
        Assertions.assertEquals("doc_count", aggregation.getOrder(0).getKey());
        Assertions.assertEquals("desc", aggregation.getOrder(0).getDirection());
        Assertions.assertEquals(2, aggregation.getTopHits().getSize());
        Assertions.assertEquals("_score", aggregation.getTopHits().getSort(0).getFieldName());
        Assertions.assertEquals("desc", aggregation.getTopHits().getSort(0).getDirection());
        Assertions.assertEquals(3, aggregation.getSubAggregation().getSize());
        Assertions.assertEquals(Collections.singletonList("brand"), aggregation.getSubAggregation().getFieldsList());
    }

    @Test
    void testSearchAggregationRejectsGroupBy() {
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .groupByFieldName("category")
                .searchAggregation(buildAggregation())
                .build();

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.search(request));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testSessionSearchPreservesAggregationAndClusterId() {
        SearchAggregation requestAggregation = buildAggregation();
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .searchAggregation(requestAggregation)
                .build();

        client_v2.session("cluster-a").search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getSearchParamsList(), Constant.CLUSTER_ID));
        Assertions.assertEquals(Arrays.asList("category", "region"), captor.getValue().getSearchAggregation().getFieldsList());
        Assertions.assertNull(request.getClusterId());
        Assertions.assertSame(requestAggregation, request.getSearchAggregation());
    }

    @Test
    void testSearchWithoutAggregationKeepsAggregationBucketsEmpty() {
        SearchResults response = SearchResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .setResults(SearchResultData.newBuilder()
                        .setTopK(1)
                        .addTopks(1)
                        .setNumQueries(1)
                        .setIds(IDs.newBuilder().setIntId(LongArray.newBuilder().addData(10L).build()).build())
                        .addScores(0.9f)
                        .build())
                .build();
        when(blockingStub.search(any())).thenReturn(response);

        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        SearchResp searchResp = client_v2.search(request);

        Assertions.assertTrue(searchResp.getAggregationBuckets().isEmpty());
    }

    @Test
    void testSearchAggregationWithoutAggTopksFailsForMultiQuery() {
        SearchResults response = SearchResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .setResults(SearchResultData.newBuilder()
                        .setTopK(1)
                        .addTopks(1)
                        .addTopks(1)
                        .setNumQueries(2)
                        .setIds(IDs.newBuilder().setIntId(LongArray.newBuilder().addData(10L).addData(11L).build()).build())
                        .addScores(0.9f)
                        .addScores(0.8f)
                        .addAggBuckets(AggBucket.newBuilder()
                                .addKey(BucketKeyEntry.newBuilder().setFieldId(101).setFieldName("category").setStringVal("books").build())
                                .setCount(7)
                                .build())
                        .addAggBuckets(AggBucket.newBuilder()
                                .addKey(BucketKeyEntry.newBuilder().setFieldId(102).setFieldName("category").setStringVal("games").build())
                                .setCount(2)
                                .build())
                        .build())
                .build();
        when(blockingStub.search(any())).thenReturn(response);

        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Arrays.asList(
                        new FloatVec(Arrays.asList(1.0f, 2.0f)),
                        new FloatVec(Arrays.asList(3.0f, 4.0f))))
                .limit(10)
                .searchAggregation(buildAggregation())
                .build();
        MilvusClientException ex = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.search(request));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, ex.getErrorCode());
        Assertions.assertTrue(ex.getMessage().contains("without aggTopks"));
        Assertions.assertTrue(ex.getMessage().contains("multi-query search"));
    }

    @Test
    void testSearchAggregationResponseParsing() {
        SearchResults response = SearchResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0)
                        .putExtraInfo("report_value", "123")
                        .putExtraInfo("scanned_remote_bytes", "456")
                        .putExtraInfo("scanned_total_bytes", "789")
                        .putExtraInfo("cache_hit_ratio", "0.5")
                        .build())
                .setResults(SearchResultData.newBuilder()
                        .setTopK(1)
                        .addTopks(1)
                        .setNumQueries(1)
                        .setIds(IDs.newBuilder().setIntId(LongArray.newBuilder().addData(10L).build()).build())
                        .addScores(0.9f)
                        .addFieldsData(FieldData.newBuilder()
                                .setFieldName("label")
                                .setType(DataType.VarChar)
                                .setScalars(ScalarField.newBuilder()
                                        .setStringData(StringArray.newBuilder().addData("doc-1").build())
                                        .build())
                                .build())
                        .addAggBuckets(AggBucket.newBuilder()
                                .addKey(BucketKeyEntry.newBuilder().setFieldId(101).setFieldName("category").setStringVal("books").build())
                                .setCount(7)
                                .putMetrics("doc_count", MetricValue.newBuilder().setIntVal(7L).build())
                                .putMetrics("avg_score", MetricValue.newBuilder().setDoubleVal(0.88d).build())
                                .addHits(AggHit.newBuilder()
                                        .setIntPk(10L)
                                        .setScore(0.91f)
                                        .addFields(AggHitField.newBuilder().setFieldId(201).setFieldName("title").setStringVal("Book A").build())
                                        .addFields(AggHitField.newBuilder().setFieldId(202).setFieldName("payload").setBytesVal(com.google.protobuf.ByteString.copyFrom(new byte[]{1, 2, 3})).build())
                                        .build())
                                .addSubGroups(AggBucket.newBuilder()
                                        .addKey(BucketKeyEntry.newBuilder().setFieldId(102).setFieldName("brand").setStringVal("acme").build())
                                        .setCount(3)
                                        .build())
                                .build())
                        .addAggBuckets(AggBucket.newBuilder()
                                .addKey(BucketKeyEntry.newBuilder().setFieldId(103).setFieldName("category").setStringVal("games").build())
                                .setCount(2)
                                .build())
                        .addAggTopks(1)
                        .addAggTopks(1)
                        .build())
                .build();
        when(blockingStub.search(any())).thenReturn(response);

        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .searchAggregation(buildAggregation())
                .build();
        SearchResp searchResp = client_v2.search(request);

        Assertions.assertEquals(1, searchResp.getSearchResults().size());
        Assertions.assertEquals(1, searchResp.getSearchResults().get(0).size());
        Assertions.assertEquals(10L, searchResp.getSearchResults().get(0).get(0).getId());
        Assertions.assertEquals(0.9f, searchResp.getSearchResults().get(0).get(0).getScore());
        Assertions.assertEquals(2, searchResp.getAggregationBuckets().size());
        Assertions.assertEquals(1, searchResp.getAggregationBuckets().get(0).size());
        Assertions.assertEquals(1, searchResp.getAggregationBuckets().get(1).size());
        AggregationBucket bucket = searchResp.getAggregationBuckets().get(0).get(0);
        Assertions.assertEquals(7L, bucket.getCount());
        Assertions.assertEquals("books", bucket.getKey().get(0).getValue());
        Assertions.assertEquals(7L, bucket.getMetrics().get("doc_count"));
        Assertions.assertEquals(0.88d, bucket.getMetrics().get("avg_score"));
        Assertions.assertEquals(1, bucket.getHits().size());
        AggregationHit hit = bucket.getHits().get(0);
        Assertions.assertEquals(10L, hit.getId());
        Assertions.assertEquals(0.91f, hit.getScore());
        Assertions.assertEquals("Book A", hit.getFields().get("title"));
        Assertions.assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) hit.getFields().get("payload"));
        Assertions.assertEquals(201L, hit.getFieldIds().get("title"));
        Assertions.assertEquals(202L, hit.getFieldIds().get("payload"));
        Assertions.assertEquals(1, bucket.getSubGroups().size());
        Assertions.assertEquals("acme", bucket.getSubGroups().get(0).getKey().get(0).getValue());
        Assertions.assertEquals("books", searchResp.getAggregationBuckets().get(0).get(0).getKey().get(0).getValue());
        Assertions.assertEquals("games", searchResp.getAggregationBuckets().get(1).get(0).getKey().get(0).getValue());
    }

    private SearchAggregation buildAggregation() {
        return SearchAggregation.builder()
                .fields(Arrays.asList("category", "region"))
                .size(5)
                .addMetric("doc_count", MetricSpec.builder().op(MetricOps.COUNT).fieldName("*").build())
                .addMetric("avg_score", MetricSpec.builder().op(MetricOps.AVG).fieldName("score").build())
                .addOrder(OrderSpec.builder().key("doc_count").direction(AggDirection.DESC).build())
                .topHits(TopHitsSpec.builder()
                        .size(2)
                        .addSort(SortSpec.builder().fieldName("_score").direction(AggDirection.DESC).build())
                        .build())
                .subAggregation(SearchAggregation.builder()
                        .addField("brand")
                        .size(3)
                        .build())
                .build();
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

    private void setIteratorConnectConfig() throws ReflectiveOperationException {
        // Iterator creation reads the RPC deadline and cache endpoint initialized by a real
        // MilvusClientV2 connection. BaseTest constructs the client without connecting, so inject
        // both values for these mocked iterator tests.
        ConnectConfig config = ConnectConfig.builder().uri("http://localhost:19530").build();
        Field connectConfig = MilvusClientV2.class.getDeclaredField("connectConfig");
        connectConfig.setAccessible(true);
        connectConfig.set(client_v2, config);

        Field cacheEndpoint = MilvusClientV2.class.getDeclaredField("cacheEndpoint");
        cacheEndpoint.setAccessible(true);
        cacheEndpoint.set(client_v2, config.getHost() + ":" + config.getPort());
    }

    private String getParam(List<KeyValuePair> params, String key) {
        return params.stream()
                .filter(param -> key.equals(param.getKey()))
                .map(KeyValuePair::getValue)
                .findFirst()
                .orElse(null);
    }
}
