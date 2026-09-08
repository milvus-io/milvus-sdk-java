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

import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("integration")
class VectorInsertUpsertTest extends BaseTest {

    Logger logger = LoggerFactory.getLogger(VectorInsertUpsertTest.class);

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
    void testInsertWithEmptyDataReturnsEmptyWithoutRpc() {
        InsertResp resp = client_v2.insert(InsertReq.builder()
                .collectionName("test")
                .data(Collections.emptyList())
                .build());

        Assertions.assertEquals(0L, resp.getInsertCnt());
        Assertions.assertTrue(resp.getPrimaryKeys().isEmpty());
        Assertions.assertEquals(0L, resp.getCost());
        verify(blockingStub, never()).insert(any(InsertRequest.class));
        verify(blockingStub, never()).describeCollection(any());
    }

    @Test
    void testInsertWithNullDataRejectsWithoutRpc() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.insert(InsertReq.builder()
                        .collectionName("test")
                        .data(null)
                        .build()));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("data cannot be null"));
        verify(blockingStub, never()).insert(any(InsertRequest.class));
        verify(blockingStub, never()).describeCollection(any());
    }

    @Test
    void testUpsertWithEmptyDataReturnsEmptyWithoutRpc() {
        UpsertResp resp = client_v2.upsert(UpsertReq.builder()
                .collectionName("test")
                .data(Collections.emptyList())
                .build());

        Assertions.assertEquals(0L, resp.getUpsertCnt());
        Assertions.assertTrue(resp.getPrimaryKeys().isEmpty());
        Assertions.assertEquals(0L, resp.getCost());
        verify(blockingStub, never()).upsert(any(UpsertRequest.class));
        verify(blockingStub, never()).describeCollection(any());
    }

    @Test
    void testUpsertWithNullDataRejectsWithoutRpc() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.upsert(UpsertReq.builder()
                        .collectionName("test")
                        .data(null)
                        .build()));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("data cannot be null"));
        verify(blockingStub, never()).upsert(any(UpsertRequest.class));
        verify(blockingStub, never()).describeCollection(any());
    }

    @Test
    void testDeleteExposesCost() {
        MutationResult result = MutationResult.newBuilder()
                .setDeleteCnt(2L)
                .setIDs(IDs.newBuilder()
                        .setIntId(LongArray.newBuilder()
                                .addData(10L)
                                .addData(20L)
                                .build()))
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
        Assertions.assertEquals(Arrays.asList(10L, 20L), resp.getPrimaryKeys());
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
}
