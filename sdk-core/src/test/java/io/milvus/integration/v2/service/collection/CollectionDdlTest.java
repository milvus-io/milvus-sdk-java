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

package io.milvus.integration.v2.service.collection;

import io.milvus.grpc.AddCollectionFunctionRequest;
import io.milvus.grpc.AlterCollectionFieldRequest;
import io.milvus.grpc.AlterCollectionFunctionRequest;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.LoadCollectionRequest;
import io.milvus.support.v2.BaseTest;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("integration")
class CollectionDdlTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(CollectionDdlTest.class);

    @Test
    void testDropCollection() {
        DropCollectionReq req = DropCollectionReq.builder()
                .collectionName("test")
                .async(Boolean.FALSE)
                .build();
        client_v2.dropCollection(req);
    }

    @Test
    void testTruncateCollection() {
        TruncateCollectionReq req = TruncateCollectionReq.builder()
                .collectionName("test")
                .build();
        client_v2.truncateCollection(req);
    }

    @Test
    void testHasCollection() {
        HasCollectionReq req = HasCollectionReq.builder()
                .collectionName("test")
                .build();
        Boolean resp = client_v2.hasCollection(req);
    }

    @Test
    void testRenameCollection() {
        RenameCollectionReq req = RenameCollectionReq.builder()
                .collectionName("test2")
                .newCollectionName("test")
                .build();
        client_v2.renameCollection(req);
    }

    @Test
    void testLoadCollection() {
        LoadCollectionReq req = LoadCollectionReq.builder()
                .collectionName("test")
                .build();
        client_v2.loadCollection(req);

    }

    @Test
    void testReleaseCollection() {
        ReleaseCollectionReq req = ReleaseCollectionReq.builder()
                .collectionName("test")
                .async(Boolean.FALSE)
                .build();
        client_v2.releaseCollection(req);
    }

    @Test
    void testGetLoadState() {
        GetLoadStateReq req = GetLoadStateReq.builder()
                .collectionName("test")
                .build();
        Boolean resp = client_v2.getLoadState(req);
        logger.info("resp: {}", resp);
    }

    @Test
    void testGetCollectionStats() {
        GetCollectionStatsReq req = GetCollectionStatsReq.builder()
                .collectionName("test")
                .build();
        GetCollectionStatsResp resp = client_v2.getCollectionStats(req);
        Assertions.assertEquals(10L, resp.getNumOfEntities());
        Assertions.assertEquals("10", resp.getStats().get("row_count"));
    }

    @Test
    void testGetCollectionStatsMissingRowCountReturnsZero() {
        when(blockingStub.getCollectionStatistics(any())).thenReturn(
                GetCollectionStatisticsResponse.newBuilder()
                        .addStats(KeyValuePair.newBuilder().setKey("other_stat").setValue("1").build())
                        .setStatus(io.milvus.grpc.Status.newBuilder().setCode(0).build())
                        .build());

        GetCollectionStatsReq req = GetCollectionStatsReq.builder()
                .collectionName("test")
                .build();
        GetCollectionStatsResp resp = client_v2.getCollectionStats(req);
        Assertions.assertEquals(0L, resp.getNumOfEntities());
        Assertions.assertTrue(resp.getStats().containsKey("other_stat"));
    }

    @Test
    void testDropCollectionFieldProperties() {
        when(blockingStub.alterCollectionField(any())).thenReturn(
                io.milvus.grpc.Status.newBuilder().setCode(0).build());

        DropCollectionFieldPropertiesReq req = DropCollectionFieldPropertiesReq.builder()
                .databaseName("default")
                .collectionName("test")
                .fieldName("vector")
                .propertyKeys(Arrays.asList("index_type", "dim"))
                .build();
        client_v2.dropCollectionFieldProperties(req);

        ArgumentCaptor<AlterCollectionFieldRequest> captor =
                ArgumentCaptor.forClass(AlterCollectionFieldRequest.class);
        verify(blockingStub).alterCollectionField(captor.capture());
        AlterCollectionFieldRequest rpcRequest = captor.getValue();
        Assertions.assertEquals("default", rpcRequest.getDbName());
        Assertions.assertEquals("test", rpcRequest.getCollectionName());
        Assertions.assertEquals("vector", rpcRequest.getFieldName());
        Assertions.assertEquals(Arrays.asList("index_type", "dim"), rpcRequest.getDeleteKeysList());
    }

    @Test
    void testRefreshLoad() {
        RefreshLoadReq req = RefreshLoadReq.builder()
                .databaseName("default")
                .collectionName("test")
                .sync(Boolean.FALSE)
                .build();
        client_v2.refreshLoad(req);

        ArgumentCaptor<LoadCollectionRequest> captor =
                ArgumentCaptor.forClass(LoadCollectionRequest.class);
        verify(blockingStub).loadCollection(captor.capture());
        LoadCollectionRequest rpcRequest = captor.getValue();
        Assertions.assertEquals("default", rpcRequest.getDbName());
        Assertions.assertEquals("test", rpcRequest.getCollectionName());
        Assertions.assertTrue(rpcRequest.getRefresh());
    }

    @Test
    void testAddCollectionFunction() {
        when(blockingStub.addCollectionFunction(any())).thenReturn(
                io.milvus.grpc.Status.newBuilder().setCode(0).build());

        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder()
                .name("bm25")
                .functionType(io.milvus.common.clientenum.FunctionType.BM25)
                .inputFieldNames(Arrays.asList("text"))
                .outputFieldNames(Arrays.asList("sparse"))
                .build();
        AddCollectionFunctionReq req = AddCollectionFunctionReq.builder()
                .databaseName("default")
                .collectionName("test")
                .function(function)
                .build();
        client_v2.addCollectionFunction(req);

        ArgumentCaptor<AddCollectionFunctionRequest> captor =
                ArgumentCaptor.forClass(AddCollectionFunctionRequest.class);
        verify(blockingStub).addCollectionFunction(captor.capture());
        AddCollectionFunctionRequest rpcRequest = captor.getValue();
        Assertions.assertEquals("default", rpcRequest.getDbName());
        Assertions.assertEquals("test", rpcRequest.getCollectionName());
        Assertions.assertEquals("bm25", rpcRequest.getFunctionSchema().getName());
        Assertions.assertEquals(io.milvus.grpc.FunctionType.BM25, rpcRequest.getFunctionSchema().getType());
        Assertions.assertEquals("text", rpcRequest.getFunctionSchema().getInputFieldNames(0));
        Assertions.assertEquals("sparse", rpcRequest.getFunctionSchema().getOutputFieldNames(0));
    }

    @Test
    void testAddCollectionFunctionRejectsNullFunction() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.addCollectionFunction(AddCollectionFunctionReq.builder()
                        .collectionName("test")
                        .build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        verify(blockingStub, never()).addCollectionFunction(any());
    }

    @Test
    void testAlterCollectionFunction() {
        when(blockingStub.alterCollectionFunction(any())).thenReturn(
                io.milvus.grpc.Status.newBuilder().setCode(0).build());

        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder()
                .name("bm25")
                .functionType(io.milvus.common.clientenum.FunctionType.BM25)
                .inputFieldNames(Arrays.asList("text"))
                .outputFieldNames(Arrays.asList("sparse"))
                .build();
        AlterCollectionFunctionReq req = AlterCollectionFunctionReq.builder()
                .databaseName("default")
                .collectionName("test")
                .function(function)
                .build();
        client_v2.alterCollectionFunction(req);

        ArgumentCaptor<AlterCollectionFunctionRequest> captor =
                ArgumentCaptor.forClass(AlterCollectionFunctionRequest.class);
        verify(blockingStub).alterCollectionFunction(captor.capture());
        AlterCollectionFunctionRequest rpcRequest = captor.getValue();
        Assertions.assertEquals("default", rpcRequest.getDbName());
        Assertions.assertEquals("test", rpcRequest.getCollectionName());
        Assertions.assertEquals("bm25", rpcRequest.getFunctionName());
        Assertions.assertEquals("bm25", rpcRequest.getFunctionSchema().getName());
        Assertions.assertEquals(io.milvus.grpc.FunctionType.BM25, rpcRequest.getFunctionSchema().getType());
        Assertions.assertEquals("text", rpcRequest.getFunctionSchema().getInputFieldNames(0));
        Assertions.assertEquals("sparse", rpcRequest.getFunctionSchema().getOutputFieldNames(0));
    }

    @Test
    void testAlterCollectionFunctionRejectsNullFunction() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.alterCollectionFunction(AlterCollectionFunctionReq.builder()
                        .collectionName("test")
                        .build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        verify(blockingStub, never()).alterCollectionFunction(any());
    }
}
