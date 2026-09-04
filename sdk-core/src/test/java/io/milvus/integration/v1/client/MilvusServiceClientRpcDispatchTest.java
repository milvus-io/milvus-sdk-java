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

package io.milvus.integration.v1.client;

import com.google.common.util.concurrent.ListenableFuture;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.alias.*;
import io.milvus.param.collection.*;
import io.milvus.param.control.*;
import io.milvus.param.credential.CreateCredentialParam;
import io.milvus.param.credential.DeleteCredentialParam;
import io.milvus.param.credential.ListCredUsersParam;
import io.milvus.param.credential.UpdateCredentialParam;
import io.milvus.param.dml.*;
import io.milvus.param.dml.ranker.RRFRanker;
import io.milvus.param.index.*;
import io.milvus.param.partition.*;
import io.milvus.support.server.MockMilvusServer;
import io.milvus.support.server.MockMilvusServerImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class MilvusServiceClientRpcDispatchTest {
    private final int testPort = 53019;
    private MockMilvusServerImpl mockServerImpl;

    private MockMilvusServer startServer() {
        mockServerImpl = new MockMilvusServerImpl();
        MockMilvusServer mockServer = new MockMilvusServer(testPort, mockServerImpl);
        mockServer.start();
        return mockServer;
    }

    private MilvusClient startClient() {
        String testHost = "localhost";
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost(testHost)
                .withPort(testPort)
                .build();
        RetryParam retryParam = RetryParam.newBuilder()
                .withMaxRetryTimes(2)
                .build();
        return new MilvusServiceClient(connectParam).withRetry(retryParam);
    }

    @SuppressWarnings("unchecked")
    private <T, P> void invokeFunc(Method testFunc, MilvusClient client, T param, int ret, boolean equalRet) {
        try {
            R<P> resp = (R<P>) testFunc.invoke(client, param);
            if (equalRet) {
                assertEquals(ret, resp.getStatus());
            } else {
                assertNotEquals(ret, resp.getStatus());
            }
        } catch (IllegalArgumentException | InvocationTargetException | IllegalAccessException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            fail();
        }
    }

    private <T, P> void testFuncByName(String funcName, T param) {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusClient client = startClient();
        try {
            Class<?> clientClass = MilvusServiceClient.class;
            Method testFunc = clientClass.getMethod(funcName, param.getClass());

            // test return ok with correct input
            invokeFunc(testFunc, client, param, R.Status.Success.getCode(), true);

            // stop mock server
            server.stop();

            // test return error without server
            invokeFunc(testFunc, client, param, R.Status.Success.getCode(), false);

            // test return error when client channel is shutdown
            client.close();
            invokeFunc(testFunc, client, param, R.Status.ClientNotConnected.getCode(), true);
        } catch (NoSuchMethodException | SecurityException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            fail();
        } finally {
            server.stop();
            client.close();
        }
    }

    @SuppressWarnings("unchecked")
    private <T, P> void testAsyncFuncByName(String funcName, T param) {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusClient client = startClient();

        try {
            Class<?> clientClass = MilvusServiceClient.class;
            Method testFunc = clientClass.getMethod(funcName, param.getClass());

            // test return ok with correct input
            try {
                ListenableFuture<R<P>> respFuture = (ListenableFuture<R<P>>) testFunc.invoke(client, param);
                R<P> response = respFuture.get();
                assertEquals(R.Status.Success.getCode(), response.getStatus());
            } catch (IllegalArgumentException | InvocationTargetException | IllegalAccessException |
                     InterruptedException | ExecutionException e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
                fail();
            }

            // stop mock server
            server.stop();

            // test return error without server
            assertThrows(ExecutionException.class, () -> {
                ListenableFuture<R<P>> respFuture = (ListenableFuture<R<P>>) testFunc.invoke(client, param);
                R<P> response = respFuture.get();
                assertNotEquals(R.Status.Success.getCode(), response.getStatus());
            });

            // test return error when client channel is shutdown
            client.close();
            try {
                ListenableFuture<R<P>> respFuture = (ListenableFuture<R<P>>) testFunc.invoke(client, param);
                R<P> response = respFuture.get();
                assertEquals(R.Status.ClientNotConnected.getCode(), response.getStatus());
            } catch (IllegalArgumentException | InvocationTargetException | IllegalAccessException |
                     InterruptedException | ExecutionException e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
                fail();
            }
        } catch (NoSuchMethodException | SecurityException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            fail();
        } finally {
            server.stop();
            client.close();
        }
    }

    @Test
    void createCollectionParam() {
        // test throw exception with illegal input for FieldType
        assertThrows(ParamException.class, () ->
                FieldType.newBuilder()
                        .withName("")
                        .withDataType(DataType.Int64)
                        .build()
        );

        assertThrows(ParamException.class, () ->
                FieldType.newBuilder()
                        .withName("userID")
                        .build()
        );

        assertThrows(ParamException.class, () ->
                FieldType.newBuilder()
                        .withName("userID")
                        .withDataType(DataType.FloatVector)
                        .build()
        );

        assertThrows(ParamException.class, () ->
                FieldType.newBuilder()
                        .withName("userID")
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withPartitionKey(true)
                        .build()
        );

        assertThrows(ParamException.class, () ->
                FieldType.newBuilder()
                        .withName("userID")
                        .withDataType(DataType.FloatVector)
                        .withPartitionKey(true)
                        .build()
        );

        assertDoesNotThrow(() ->
                FieldType.newBuilder()
                        .withName("partitionKey")
                        .withDataType(DataType.Int64)
                        .withPartitionKey(true)
                        .build()
        );

        assertDoesNotThrow(() ->
                FieldType.newBuilder()
                        .withName("partitionKey")
                        .withDataType(DataType.VarChar)
                        .withMaxLength(120)
                        .withPartitionKey(true)
                        .build()
        );

        Map<String, String> params = new HashMap<>();
        params.put("1", "1");
        assertThrows(ParamException.class, () ->
                FieldType.newBuilder()
                        .withName("vec")
                        .withDescription("desc")
                        .withDataType(DataType.FloatVector)
                        .withTypeParams(params)
                        .addTypeParam("2", "2")
                        .withDimension(-1)
                        .build()
        );

        // test throw exception with illegal input for CreateCollectionParam
        assertThrows(ParamException.class, () ->
                CreateCollectionParam
                        .newBuilder()
                        .withCollectionName("collection1")
                        .withShardsNum(2)
                        .build()
        );

        FieldType fieldType1 = FieldType.newBuilder()
                .withName("userID")
                .withDescription("userId")
                .withDataType(DataType.Int64)
                .withAutoID(true)
                .withPrimaryKey(true)
                .build();

        assertThrows(ParamException.class, () ->
                CreateCollectionParam
                        .newBuilder()
                        .withCollectionName("")
                        .withShardsNum(2)
                        .addFieldType(fieldType1)
                        .build()
        );

        assertThrows(ParamException.class, () ->
                CreateCollectionParam
                        .newBuilder()
                        .withCollectionName("collection1")
                        .withShardsNum(-1)
                        .addFieldType(fieldType1)
                        .build()
        );

        List<FieldType> fields = Collections.singletonList(null);
        assertThrows(ParamException.class, () ->
                CreateCollectionParam
                        .newBuilder()
                        .withCollectionName("collection1")
                        .withShardsNum(2)
                        .withFieldTypes(fields)
                        .build()
        );

        assertThrows(ParamException.class, () ->
                CreateCollectionParam
                        .newBuilder()
                        .withCollectionName("collection1")
                        .withShardsNum(0)
                        .withPartitionsNum(10)
                        .addFieldType(fieldType1)
                        .build()
        );

        FieldType fieldType2 = FieldType.newBuilder()
                .withName("partitionKey")
                .withDataType(DataType.Int64)
                .withPartitionKey(true)
                .build();

        assertDoesNotThrow(() ->
                CreateCollectionParam
                        .newBuilder()
                        .withCollectionName("collection1")
                        .addFieldType(fieldType1)
                        .addFieldType(fieldType2)
                        .build()
        );

        assertDoesNotThrow(() ->
                CreateCollectionParam
                        .newBuilder()
                        .withCollectionName("collection1")
                        .withPartitionsNum(100)
                        .addFieldType(fieldType1)
                        .addFieldType(fieldType2)
                        .build()
        );
    }


    @Test
    void createCollection() {
        FieldType fieldType1 = FieldType.newBuilder()
                .withName("userID")
                .withDescription("userId")
                .withDataType(DataType.Int64)
                .withAutoID(true)
                .withPrimaryKey(true)
                .build();

        CreateCollectionParam param = CreateCollectionParam
                .newBuilder()
                .withCollectionName("collection1")
                .withDescription("desc")
                .withShardsNum(2)
                .addFieldType(fieldType1)
                .build();

        testFuncByName("createCollection", param);
    }

    @Test
    void describeCollectionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () ->
                DescribeCollectionParam.newBuilder()
                        .withCollectionName("")
                        .build()
        );
    }

    @Test
    void describeCollection() {
        DescribeCollectionParam param = DescribeCollectionParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        testFuncByName("describeCollection", param);
    }

    @Test
    void dropCollectionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () ->
                DropCollectionParam.newBuilder()
                        .withCollectionName("")
                        .build()
        );
    }

    @Test
    void dropCollection() {
        DropCollectionParam param = DropCollectionParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        testFuncByName("dropCollection", param);
    }

    @Test
    void getCollectionStatisticsParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () ->
                GetCollectionStatisticsParam.newBuilder()
                        .withCollectionName("")
                        .build()
        );
    }

    @Test
    void getCollectionStatistics() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusClient client = startClient();

        try {
            final String collectionName = "collection1";
            GetCollectionStatisticsParam param = GetCollectionStatisticsParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withFlush(Boolean.TRUE)
                    .build();

            // test return ok with correct input
            final long segmentID = 2021L;
            mockServerImpl.setFlushResponse(FlushResponse.newBuilder()
                    .putCollSegIDs(collectionName, LongArray.newBuilder().addData(segmentID).build())
                    .putCollFlushTs(collectionName, 200L)
                    .build());
            mockServerImpl.setGetFlushStateResponse(GetFlushStateResponse.newBuilder()
                    .setFlushed(false)
                    .build());

            new Thread(() -> {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
                mockServerImpl.setGetFlushStateResponse(GetFlushStateResponse.newBuilder()
                        .setFlushed(true)
                        .build());
            }, "RefreshFlushState").start();

            R<GetCollectionStatisticsResponse> resp = client.getCollectionStatistics(param);
            assertEquals(R.Status.Success.getCode(), resp.getStatus());

            // stop mock server
            server.stop();

            // test return error without server
            resp = client.getCollectionStatistics(param);
            assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

            // test return error when client channel is shutdown
            client.close();
            resp = client.getCollectionStatistics(param);
            assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            fail();
        } finally {
            server.stop();
            client.close();
        }
    }

    @Test
    void hasCollectionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () ->
                HasCollectionParam.newBuilder()
                        .withCollectionName("")
                        .build()
        );
    }

    @Test
    void hasCollection() {
        HasCollectionParam param = HasCollectionParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        testFuncByName("hasCollection", param);
    }

    @Test
    void loadCollectionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () ->
                LoadCollectionParam.newBuilder()
                        .withCollectionName("")
                        .build()
        );

        assertThrows(ParamException.class, () ->
                LoadCollectionParam.newBuilder()
                        .withCollectionName("collection1")
                        .withSyncLoad(Boolean.TRUE)
                        .withSyncLoadWaitingInterval(0L)
                        .build()
        );

        assertThrows(ParamException.class, () ->
                LoadCollectionParam.newBuilder()
                        .withCollectionName("collection1")
                        .withSyncLoad(Boolean.TRUE)
                        .withSyncLoadWaitingInterval(-1L)
                        .build()
        );

        assertThrows(ParamException.class, () ->
                LoadCollectionParam.newBuilder()
                        .withCollectionName("collection1")
                        .withSyncLoad(Boolean.TRUE)
                        .withSyncLoadWaitingInterval(Constant.MAX_WAITING_LOADING_INTERVAL + 1)
                        .build()
        );

        assertThrows(ParamException.class, () ->
                LoadCollectionParam.newBuilder()
                        .withCollectionName("collection1")
                        .withSyncLoad(Boolean.TRUE)
                        .withSyncLoadWaitingTimeout(0L)
                        .build()
        );

        assertThrows(ParamException.class, () ->
                LoadCollectionParam.newBuilder()
                        .withCollectionName("collection1")
                        .withSyncLoad(Boolean.TRUE)
                        .withSyncLoadWaitingTimeout(-1L)
                        .build()
        );

        assertThrows(ParamException.class, () ->
                LoadCollectionParam.newBuilder()
                        .withCollectionName("collection1")
                        .withSyncLoad(Boolean.TRUE)
                        .withSyncLoadWaitingTimeout(Constant.MAX_WAITING_LOADING_TIMEOUT + 1)
                        .build()
        );
    }

    @Test
    void loadCollection() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusClient client = startClient();

        String collectionName = "collection1";
        LoadCollectionParam param = LoadCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .withSyncLoad(Boolean.FALSE)
                .build();

        // test return ok with correct input
        R<RpcStatus> resp = client.loadCollection(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return ok for sync mode loading
        mockServerImpl.setShowCollectionsResponse(ShowCollectionsResponse.newBuilder()
                .addCollectionNames(collectionName)
                .addInMemoryPercentages(0)
                .build());

        new Thread(() -> {
            try {
                for (int i = 0; i <= 10; ++i) {
                    TimeUnit.MILLISECONDS.sleep(100);
                    mockServerImpl.setShowCollectionsResponse(ShowCollectionsResponse.newBuilder()
                            .addCollectionNames(collectionName)
                            .addInMemoryPercentages(i * 10)
                            .build());
                }
            } catch (InterruptedException e) {
                mockServerImpl.setShowCollectionsResponse(ShowCollectionsResponse.newBuilder()
                        .addCollectionNames(collectionName)
                        .addInMemoryPercentages(100)
                        .build());
            }
        }, "RefreshMemState").start();

        param = LoadCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .withSyncLoad(Boolean.TRUE)
                .withSyncLoadWaitingInterval(100L)
                .build();
        resp = client.loadCollection(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.loadCollection(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.loadCollection(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void releaseCollectionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () ->
                ReleaseCollectionParam.newBuilder()
                        .withCollectionName("")
                        .build()
        );
    }

    @Test
    void releaseCollection() {
        ReleaseCollectionParam param = ReleaseCollectionParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        testFuncByName("releaseCollection", param);
    }

    @Test
    void showCollectionsParam() {
        // test throw exception with illegal input
        List<String> names = new ArrayList<>();
        names.add(null);
        assertThrows(IllegalArgumentException.class, () ->
                ShowCollectionsParam.newBuilder()
                        .withCollectionNames(names)
                        .build()
        );

        assertThrows(ParamException.class, () ->
                ShowCollectionsParam.newBuilder()
                        .addCollectionName("")
                        .build()
        );

        // verify internal param
        ShowCollectionsParam param = ShowCollectionsParam.newBuilder()
                .build();
        assertEquals(ShowType.All, param.getShowType());

        param = ShowCollectionsParam.newBuilder()
                .addCollectionName("collection1")
                .build();
        assertEquals(ShowType.InMemory, param.getShowType());
    }

    @Test
    void showCollections() {
        ShowCollectionsParam param = ShowCollectionsParam.newBuilder()
                .addCollectionName("collection1")
                .addCollectionName("collection2")
                .build();

        testFuncByName("showCollections", param);
    }

    @Test
    void alterCollectionParam() {
        AlterCollectionParam param = AlterCollectionParam.newBuilder()
                .withCollectionName("collection1")
                .build();
        assertEquals("collection1", param.getCollectionName());
        assertTrue(param.getProperties().isEmpty());

        param = AlterCollectionParam.newBuilder()
                .withCollectionName("collection1")
                .withTTL(100)
                .build();
        assertTrue(param.getProperties().containsKey(Constant.TTL_SECONDS));
        assertEquals("100", param.getProperties().get(Constant.TTL_SECONDS));

        assertThrows(ParamException.class, () ->
                AlterCollectionParam.newBuilder()
                        .withCollectionName("")
                        .build()
        );

        assertThrows(ParamException.class, () ->
                AlterCollectionParam.newBuilder()
                        .withCollectionName("collection1")
                        .withTTL(-5)
                        .build()
        );
    }

    @Test
    void alterCollection() {
        AlterCollectionParam param = AlterCollectionParam.newBuilder()
                .withCollectionName("collection1")
                .withTTL(100)
                .build();

        testFuncByName("alterCollection", param);
    }

    @Test
    void flushParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> FlushParam.newBuilder()
                .build()
        );

        assertThrows(ParamException.class, () -> FlushParam.newBuilder()
                .addCollectionName("")
                .build()
        );

        assertThrows(ParamException.class, () -> FlushParam.newBuilder()
                .addCollectionName("collection1")
                .withSyncFlush(Boolean.TRUE)
                .withSyncFlushWaitingInterval(0L)
                .build()
        );

        assertThrows(ParamException.class, () -> FlushParam.newBuilder()
                .addCollectionName("collection1")
                .withSyncFlush(Boolean.TRUE)
                .withSyncFlushWaitingInterval(-1L)
                .build()
        );

        assertThrows(ParamException.class, () -> FlushParam.newBuilder()
                .withCollectionNames(Collections.singletonList("collection1"))
                .withSyncFlush(Boolean.TRUE)
                .withSyncFlushWaitingInterval(Constant.MAX_WAITING_FLUSHING_INTERVAL + 1)
                .build()
        );

        assertThrows(ParamException.class, () -> FlushParam.newBuilder()
                .addCollectionName("collection1")
                .withSyncFlush(Boolean.TRUE)
                .withSyncFlushWaitingTimeout(0L)
                .build()
        );

        assertThrows(ParamException.class, () -> FlushParam.newBuilder()
                .addCollectionName("collection1")
                .withSyncFlush(Boolean.TRUE)
                .withSyncFlushWaitingTimeout(-1L)
                .build()
        );

        assertThrows(ParamException.class, () -> FlushParam.newBuilder()
                .addCollectionName("collection1")
                .withSyncFlush(Boolean.TRUE)
                .withSyncFlushWaitingTimeout(Constant.MAX_WAITING_FLUSHING_TIMEOUT + 1)
                .build()
        );
    }

    @Test
    void flush() {
        FlushParam param = FlushParam.newBuilder()
                .addCollectionName("collection1")
                .withSyncFlush(Boolean.TRUE)
                .withSyncFlushWaitingTimeout(1L)
                .build();

        testFuncByName("flush", param);
    }

    @Test
    void createPartitionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> CreatePartitionParam.newBuilder()
                .withCollectionName("")
                .withPartitionName("partition1")
                .build()
        );

        assertThrows(ParamException.class, () -> CreatePartitionParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("")
                .build()
        );
    }

    @Test
    void createPartition() {
        CreatePartitionParam param = CreatePartitionParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build();

        testFuncByName("createPartition", param);
    }

    @Test
    void dropPartitionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> DropPartitionParam.newBuilder()
                .withCollectionName("")
                .withPartitionName("partition1")
                .build()
        );

        assertThrows(ParamException.class, () -> DropPartitionParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("")
                .build()
        );
    }

    @Test
    void dropPartition() {
        DropPartitionParam param = DropPartitionParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build();

        testFuncByName("dropPartition", param);
    }

    @Test
    void hasPartitionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> HasPartitionParam.newBuilder()
                .withCollectionName("")
                .withPartitionName("partition1")
                .build()
        );

        assertThrows(ParamException.class, () -> HasPartitionParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("")
                .build()
        );
    }

    @Test
    void hasPartition() {
        HasPartitionParam param = HasPartitionParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build();

        testFuncByName("hasPartition", param);
    }

    @Test
    void loadPartitionsParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
                .withCollectionName("")
                .addPartitionName("partition1")
                .build()
        );

        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .addPartitionName("")
                .build()
        );

        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .build()
        );

        List<String> names = new ArrayList<>();
        names.add(null);
        assertThrows(IllegalArgumentException.class, () -> LoadPartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(names)
                .build()
        );

        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .addPartitionName("partition1")
                .withSyncLoad(Boolean.TRUE)
                .withSyncLoadWaitingInterval(0L)
                .build()
        );

        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .addPartitionName("partition1")
                .withSyncLoad(Boolean.TRUE)
                .withSyncLoadWaitingInterval(-1L)
                .build()
        );

        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .addPartitionName("partition1")
                .withSyncLoad(Boolean.TRUE)
                .withSyncLoadWaitingInterval(Constant.MAX_WAITING_LOADING_INTERVAL + 1)
                .build()
        );

        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .addPartitionName("partition1")
                .withSyncLoad(Boolean.TRUE)
                .withSyncLoadWaitingTimeout(0L)
                .build()
        );

        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .addPartitionName("partition1")
                .withSyncLoad(Boolean.TRUE)
                .withSyncLoadWaitingTimeout(-1L)
                .build()
        );

        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .addPartitionName("partition1")
                .withSyncLoad(Boolean.TRUE)
                .withSyncLoadWaitingTimeout(Constant.MAX_WAITING_LOADING_TIMEOUT + 1)
                .build()
        );
    }

    @Test
    void loadPartitions() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusClient client = startClient();

        String collectionName = "collection1";
        String partitionName = "partition1";
        LoadPartitionsParam param = LoadPartitionsParam.newBuilder()
                .withCollectionName(collectionName)
                .addPartitionName(partitionName)
                .withSyncLoad(Boolean.FALSE)
                .build();

        // test return ok with correct input
        R<RpcStatus> resp = client.loadPartitions(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return ok for sync mode loading
        mockServerImpl.setShowPartitionsResponse(ShowPartitionsResponse.newBuilder()
                .addPartitionNames(partitionName)
                .addInMemoryPercentages(0)
                .build());

        new Thread(() -> {
            try {
                for (int i = 0; i <= 10; ++i) {
                    TimeUnit.MILLISECONDS.sleep(100);
                    mockServerImpl.setShowPartitionsResponse(ShowPartitionsResponse.newBuilder()
                            .addPartitionNames(partitionName)
                            .addInMemoryPercentages(i * 10)
                            .build());
                }
            } catch (InterruptedException e) {
                mockServerImpl.setShowPartitionsResponse(ShowPartitionsResponse.newBuilder()
                        .addPartitionNames(partitionName)
                        .addInMemoryPercentages(100)
                        .build());
            }
        }, "RefreshMemState").start();

        param = LoadPartitionsParam.newBuilder()
                .withCollectionName(collectionName)
                .addPartitionName(partitionName)
                .withSyncLoad(Boolean.TRUE)
                .withSyncLoadWaitingInterval(100L)
                .build();
        resp = client.loadPartitions(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.loadPartitions(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.loadPartitions(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void releasePartitionsParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> ReleasePartitionsParam.newBuilder()
                .withCollectionName("")
                .addPartitionName("partition1")
                .build()
        );

        assertThrows(ParamException.class, () -> ReleasePartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .addPartitionName("")
                .build()
        );

        assertThrows(ParamException.class, () -> ReleasePartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .build()
        );

        List<String> names = new ArrayList<>();
        names.add(null);
        assertThrows(IllegalArgumentException.class, () -> ReleasePartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(names)
                .build()
        );
    }

    @Test
    void releasePartitions() {
        ReleasePartitionsParam param = ReleasePartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .addPartitionName("partition1")
                .build();

        testFuncByName("releasePartitions", param);
    }

    @Test
    void getPartitionStatisticsParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> GetPartitionStatisticsParam.newBuilder()
                .withCollectionName("")
                .withPartitionName("partition1")
                .withFlush(true)
                .build()
        );

        assertThrows(ParamException.class, () -> GetPartitionStatisticsParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("")
                .withFlush(false)
                .build()
        );
    }

    @Test
    void getPartitionStatistics() {
        GetPartitionStatisticsParam param = GetPartitionStatisticsParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build();

        testFuncByName("getPartitionStatistics", param);
    }

    @Test
    void showPartitionsParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> ShowPartitionsParam.newBuilder()
                .withCollectionName("")
                .addPartitionName("partition1")
                .build()
        );

        assertThrows(ParamException.class, () -> ShowPartitionsParam.newBuilder()
                .withCollectionName("collection1`")
                .addPartitionName("")
                .build()
        );

        List<String> names = new ArrayList<>();
        names.add(null);
        assertThrows(IllegalArgumentException.class, () -> ShowPartitionsParam.newBuilder()
                .withCollectionName("collection1`")
                .withPartitionNames(names)
                .build()
        );

        // verify internal param
        ShowPartitionsParam param = ShowPartitionsParam.newBuilder()
                .withCollectionName("collection1`")
                .build();
        assertEquals(ShowType.All, param.getShowType());

        param = ShowPartitionsParam.newBuilder()
                .withCollectionName("collection1`")
                .addPartitionName("partition1")
                .build();
        assertEquals(ShowType.InMemory, param.getShowType());
    }

    @Test
    void showPartitions() {
        ShowPartitionsParam param = ShowPartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        testFuncByName("showPartitions", param);
    }

    @Test
    void createAliasParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> CreateAliasParam.newBuilder()
                .withCollectionName("")
                .withAlias("alias1")
                .build()
        );

        assertThrows(ParamException.class, () -> CreateAliasParam.newBuilder()
                .withCollectionName("collection1")
                .withAlias("")
                .build()
        );

        CreateAliasParam param = CreateAliasParam.newBuilder()
                .withCollectionName("collection1")
                .withAlias("alias1")
                .withDatabaseName("db1")
                .build();
    }

    @Test
    void createAlias() {
        CreateAliasParam param = CreateAliasParam.newBuilder()
                .withCollectionName("collection1")
                .withAlias("alias1")
                .build();

        testFuncByName("createAlias", param);
    }

    @Test
    void dropAliasParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> DropAliasParam.newBuilder()
                .withAlias("")
                .build()
        );
    }

    @Test
    void dropAlias() {
        DropAliasParam param = DropAliasParam.newBuilder()
                .withAlias("alias1")
                .withDatabaseName("db1")
                .build();

        testFuncByName("dropAlias", param);
    }

    @Test
    void alterAliasParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> AlterAliasParam.newBuilder()
                .withCollectionName("")
                .withAlias("alias1")
                .build()
        );

        assertThrows(ParamException.class, () -> AlterAliasParam.newBuilder()
                .withCollectionName("collection1")
                .withAlias("")
                .build()
        );

        AlterAliasParam param = AlterAliasParam.newBuilder()
                .withCollectionName("collection1")
                .withAlias("alias1")
                .withDatabaseName("db1")
                .build();
    }

    @Test
    void alterAlias() {
        AlterAliasParam param = AlterAliasParam.newBuilder()
                .withCollectionName("collection1")
                .withAlias("alias1")
                .build();

        testFuncByName("alterAlias", param);
    }

    @Test
    void listAliasesParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> ListAliasesParam.newBuilder()
                .withCollectionName("")
                .withDatabaseName("")
                .build()
        );

        ListAliasesParam param = ListAliasesParam.newBuilder()
                .withCollectionName("collection1")
                .withDatabaseName("")
                .build();
    }

    @Test
    void listAliases() {
        ListAliasesParam param = ListAliasesParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        testFuncByName("listAliases", param);
    }

    @Test
    void createIndexParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> CreateIndexParam.newBuilder()
                .withCollectionName("")
                .withFieldName("field1")
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("dummy")
                .build()
        );

        assertThrows(ParamException.class, () -> CreateIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("")
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("dummy")
                .build()
        );

        assertThrows(ParamException.class, () -> CreateIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.L2)
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(-1L)
                .build()
        );

        assertThrows(ParamException.class, () -> CreateIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.L2)
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(Constant.MAX_WAITING_INDEX_INTERVAL + 1L)
                .build()
        );

        assertThrows(ParamException.class, () -> CreateIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.L2)
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingTimeout(0L)
                .build()
        );
    }

    @Test
    void createIndex() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusClient client = startClient();

        // createIndex() calls describeCollection() to check input
        CollectionSchema schema = CollectionSchema.newBuilder()
                .addFields(FieldSchema.newBuilder()
                        .setName("field1")
                        .setDataType(DataType.FloatVector)
                        .addTypeParams(KeyValuePair.newBuilder().setKey(Constant.VECTOR_DIM).setValue("256").build())
                        .build())
                .build();
        mockServerImpl.setDescribeCollectionResponse(DescribeCollectionResponse.newBuilder().setSchema(schema).build());

        // test return ok for sync mode loading
        mockServerImpl.setDescribeIndexResponse(DescribeIndexResponse.newBuilder()
                .addIndexDescriptions(IndexDescription.newBuilder().setState(IndexState.InProgress).build())
                .build());

        // field doesn't exist
        CreateIndexParam param = CreateIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("aaa")
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("{\"dummy\": 0}")
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(2L)
                .build();

        R<RpcStatus> resp = client.createIndex(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // index type doesn't match with data type
        param = CreateIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .withIndexType(IndexType.BIN_IVF_FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("{\"dummy\": 1}")
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(2L)
                .build();

        resp = client.createIndex(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(1);
                mockServerImpl.setDescribeIndexResponse(DescribeIndexResponse.newBuilder()
                        .addIndexDescriptions(IndexDescription.newBuilder().setState(IndexState.Finished).build())
                        .build());
            } catch (InterruptedException e) {
                mockServerImpl.setDescribeIndexResponse(DescribeIndexResponse.newBuilder()
                        .addIndexDescriptions(IndexDescription.newBuilder().setState(IndexState.Finished).build())
                        .build());
            }
        }, "RefreshIndexState").start();

        param = CreateIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("{\"dummy\": 2}")
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(2L)
                .build();

        // test return ok with correct input
        resp = client.createIndex(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.createIndex(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.createIndex(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void describeIndexParam() {
        DescribeIndexParam param = DescribeIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withIndexName("dummy")
                .build();
        assertEquals("dummy", param.getIndexName());

        assertThrows(ParamException.class, () -> DescribeIndexParam.newBuilder()
                .withCollectionName("")
                .withIndexName("field1")
                .build()
        );
    }

    @Test
    void describeIndex() {
        DescribeIndexParam param = DescribeIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withIndexName("idx")
                .build();

        testFuncByName("describeIndex", param);
    }

    @Test
    void getIndexStateParam() {
        // test throw exception with illegal input
        GetIndexStateParam param = GetIndexStateParam.newBuilder()
                .withCollectionName("collection1")
                .withIndexName("")
                .build();
        assertEquals(Constant.DEFAULT_INDEX_NAME, param.getIndexName());

        param = GetIndexStateParam.newBuilder()
                .withCollectionName("collection1")
                .withIndexName("dummy")
                .build();
        assertEquals("dummy", param.getIndexName());

        assertThrows(ParamException.class, () -> GetIndexStateParam.newBuilder()
                .withCollectionName("")
                .withIndexName("field1")
                .build()
        );
    }

    @Test
    void getIndexState() {
        GetIndexStateParam param = GetIndexStateParam.newBuilder()
                .withCollectionName("collection1")
                .withIndexName("idx")
                .build();

        testFuncByName("getIndexState", param);
    }

    @Test
    void getIndexBuildProgressParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> GetIndexBuildProgressParam.newBuilder()
                .withCollectionName("")
                .build()
        );
    }

    @Test
    void getIndexBuildProgress() {
        GetIndexBuildProgressParam param = GetIndexBuildProgressParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        testFuncByName("getIndexBuildProgress", param);
    }

    @Test
    void dropIndexParam() {
        // test throw exception with illegal input
        DropIndexParam param = DropIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withIndexName("")
                .build();
        assertEquals(Constant.DEFAULT_INDEX_NAME, param.getIndexName());

        param = DropIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withIndexName("dummy")
                .build();
        assertEquals("dummy", param.getIndexName());

        assertThrows(ParamException.class, () -> DropIndexParam.newBuilder()
                .withCollectionName("")
                .withIndexName("field1")
                .build()
        );
    }

    @Test
    void dropIndex() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusClient client = startClient();

        DropIndexParam param = DropIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withIndexName("idx")
                .build();

        // test return ok with correct input
        mockServerImpl.setDescribeIndexResponse(DescribeIndexResponse.newBuilder()
                .addIndexDescriptions(IndexDescription.newBuilder()
                        .setIndexName(param.getIndexName())
                        .setFieldName("fff")
                        .build())
                .build());

        R<RpcStatus> resp = client.dropIndex(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.dropIndex(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.dropIndex(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void insertParam() {
        // test throw exception with illegal input
        List<InsertParam.Field> fields = new ArrayList<>();

        // collection is empty
        assertThrows(ParamException.class, () -> InsertParam.newBuilder()
                .withCollectionName("")
                .withFields(fields)
                .build()
        );

        // fields is empty
        assertThrows(ParamException.class, () -> InsertParam.newBuilder()
                .withCollectionName("collection1")
                .withFields(fields)
                .build()
        );

        // field is null
        fields.add(null);
        assertThrows(ParamException.class, () -> InsertParam.newBuilder()
                .withCollectionName("collection1")
                .withFields(fields)
                .build()
        );

        // field name is empty
        fields.clear();
        List<Long> ids = new ArrayList<>();
        fields.add(new InsertParam.Field("", ids));
        assertThrows(ParamException.class, () -> InsertParam.newBuilder()
                .withCollectionName("collection1")
                .withFields(fields)
                .build()
        );

        // field row count is 0
        fields.clear();
        fields.add(new InsertParam.Field("field1", ids));
        assertThrows(ParamException.class, () -> InsertParam.newBuilder()
                .withCollectionName("collection1")
                .withFields(fields)
                .build()
        );

        // field row count not equal
        fields.clear();
        List<Long> ages = Arrays.asList(1L, 2L);
        fields.add(new InsertParam.Field("field1", ages));
        List<Integer> ports = Arrays.asList(1, 2, 3);
        fields.add(new InsertParam.Field("field2", ports));
        assertThrows(ParamException.class, () -> InsertParam.newBuilder()
                .withCollectionName("collection1")
                .withFields(fields)
                .build()
        );
    }

    @Test
    void insert() {
        // prepare schema
        HashMap<String, FieldSchema> fieldsSchema = new HashMap<>();
        fieldsSchema.put("field0", ParamUtils.ConvertField(FieldType.newBuilder()
                .withName("field0")
                .withDataType(DataType.Int64)
                .withAutoID(false)
                .withPrimaryKey(true)
                .build()));

        fieldsSchema.put("field1", ParamUtils.ConvertField(FieldType.newBuilder()
                .withName("field1")
                .withDataType(DataType.Int32)
                .build()));

        fieldsSchema.put("field2", ParamUtils.ConvertField(FieldType.newBuilder()
                .withName("field2")
                .withDataType(DataType.Int16)
                .build()));

        fieldsSchema.put("field3", ParamUtils.ConvertField(FieldType.newBuilder()
                .withName("field3")
                .withDataType(DataType.Int8)
                .build()));

        fieldsSchema.put("field4", ParamUtils.ConvertField(FieldType.newBuilder()
                .withName("field4")
                .withDataType(DataType.Bool)
                .build()));

        fieldsSchema.put("field5", ParamUtils.ConvertField(FieldType.newBuilder()
                .withName("field5")
                .withDataType(DataType.Float)
                .build()));

        fieldsSchema.put("field6", ParamUtils.ConvertField(FieldType.newBuilder()
                .withName("field6")
                .withDataType(DataType.Double)
                .build()));

        fieldsSchema.put("field7", ParamUtils.ConvertField(FieldType.newBuilder()
                .withName("field7")
                .withDataType(DataType.VarChar)
                .withMaxLength(20)
                .build()));

        fieldsSchema.put("field8", ParamUtils.ConvertField(FieldType.newBuilder()
                .withName("field8")
                .withDataType(DataType.FloatVector)
                .withDimension(2)
                .build()));

        fieldsSchema.put("field9", ParamUtils.ConvertField(FieldType.newBuilder()
                .withName("field9")
                .withDataType(DataType.BinaryVector)
                .withDimension(16)
                .build()));

        // prepare raw data
        List<Long> ids = new ArrayList<>();
        List<Integer> nVal = new ArrayList<>();
        List<Boolean> bVal = new ArrayList<>();
        List<Float> fVal = new ArrayList<>();
        List<Double> dVal = new ArrayList<>();
        List<String> sVal = new ArrayList<>();
        List<ByteBuffer> bVectors = new ArrayList<>();
        List<List<Float>> fVectors = new ArrayList<>();
        int rowCount = 3;
        for (int i = 0; i < rowCount; ++i) {
            ids.add((long) i);
            nVal.add(i);
            bVal.add(Boolean.TRUE);
            fVal.add(0.5f);
            dVal.add(1.0);
            sVal.add(String.valueOf(i));
            ByteBuffer buf = ByteBuffer.allocate(2);
            buf.put((byte) 1);
            buf.put((byte) 2);
            bVectors.add(buf);
            List<Float> vec = Arrays.asList(0.1f, 0.2f);
            fVectors.add(vec);
        }

        CollectionSchema.Builder colBuilder = CollectionSchema.newBuilder();
        colBuilder.addFields(fieldsSchema.get("field0"));
        colBuilder.addFields(fieldsSchema.get("field1"));
        colBuilder.addFields(fieldsSchema.get("field2"));
        colBuilder.addFields(fieldsSchema.get("field3"));
        colBuilder.addFields(fieldsSchema.get("field4"));
        colBuilder.addFields(fieldsSchema.get("field5"));
        colBuilder.addFields(fieldsSchema.get("field6"));
        colBuilder.addFields(fieldsSchema.get("field7"));
        colBuilder.addFields(fieldsSchema.get("field8"));
        colBuilder.addFields(fieldsSchema.get("field9"));

        List<InsertParam.Field> fields = new ArrayList<>();
        fields.add(new InsertParam.Field("field0", ids));
        fields.add(new InsertParam.Field("field1", nVal));
        fields.add(new InsertParam.Field("field2", nVal));
        fields.add(new InsertParam.Field("field3", nVal));
        fields.add(new InsertParam.Field("field4", bVal));
        fields.add(new InsertParam.Field("field5", fVal));
        fields.add(new InsertParam.Field("field6", dVal));
        fields.add(new InsertParam.Field("field7", sVal));
        fields.add(new InsertParam.Field("field8", fVectors));
        fields.add(new InsertParam.Field("field9", bVectors));

        InsertParam param = InsertParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .withFields(fields)
                .build();

        {
            // start mock server
            MockMilvusServer server = startServer();
            MilvusClient client = startClient();

            // test return ok with correct input
            mockServerImpl.setDescribeCollectionResponse(DescribeCollectionResponse.newBuilder()
                    .setCollectionID(1L)
                    .setShardsNum(2)
                    .setSchema(colBuilder.build())
                    .build());

            R<MutationResult> resp = client.insert(param);
            assertEquals(R.Status.Success.getCode(), resp.getStatus());

            // test return error with wrong int64 type
            InsertParam.Builder paramBuilder = InsertParam.newBuilder();
            paramBuilder.withCollectionName("collection1");

            fields.set(0, new InsertParam.Field("field0", nVal));
            resp = client.insert(paramBuilder.withFields(fields).build());
            assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

            // test return error with wrong int32 type
            fields.set(0, new InsertParam.Field("field0", ids));
            fields.set(1, new InsertParam.Field("field1", ids));
            resp = client.insert(paramBuilder.withFields(fields).build());
            assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

            // test return error with wrong int16 type
            fields.set(1, new InsertParam.Field("field1", nVal));
            fields.set(2, new InsertParam.Field("field2", ids));
            resp = client.insert(paramBuilder.withFields(fields).build());
            assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

            // test return error with wrong int8 type
            fields.set(2, new InsertParam.Field("field2", nVal));
            fields.set(3, new InsertParam.Field("field3", ids));
            resp = client.insert(paramBuilder.withFields(fields).build());
            assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

            // test return error with wrong bool type
            fields.set(3, new InsertParam.Field("field3", nVal));
            fields.set(4, new InsertParam.Field("field4", nVal));
            resp = client.insert(paramBuilder.withFields(fields).build());
            assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

            // test return error with wrong float type
            fields.set(4, new InsertParam.Field("field4", bVal));
            fields.set(5, new InsertParam.Field("field5", bVal));
            resp = client.insert(paramBuilder.withFields(fields).build());
            assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

            // test return error with wrong double type
            fields.set(5, new InsertParam.Field("field5", fVal));
            fields.set(6, new InsertParam.Field("field6", nVal));
            resp = client.insert(paramBuilder.withFields(fields).build());
            assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

            // test return error with wrong varchar type
            fields.set(6, new InsertParam.Field("field6", dVal));
            fields.set(7, new InsertParam.Field("field7", nVal));
            resp = client.insert(paramBuilder.withFields(fields).build());
            assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

            // test return error with wrong float vector type
            fields.set(7, new InsertParam.Field("field7", sVal));
            fields.set(8, new InsertParam.Field("field8", nVal));
            resp = client.insert(paramBuilder.withFields(fields).build());
            assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

            // test throw exception when row count is not equal
            List<List<Long>> fakeList = new ArrayList<>();
            fakeList.add(ids);
            fields.set(8, new InsertParam.Field("field8", fakeList));
            assertThrows(ParamException.class, () -> paramBuilder.withFields(fields).build());

            // test return error with wrong dimension of float vector
            fVectors.remove(rowCount - 1);
            fVectors.add(fVal);
            fields.set(8, new InsertParam.Field("field8", fVectors));
            resp = client.insert(paramBuilder.withFields(fields).build());
            assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

            // test return error with wrong binary vector type
            fVectors.remove(rowCount - 1);
            fVectors.add(Arrays.asList(0.1f, 0.2f));
            fields.set(8, new InsertParam.Field("field8", fVectors));
            fields.set(9, new InsertParam.Field("field9", nVal));
            resp = client.insert(paramBuilder.withFields(fields).build());
            assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

            // test throw exception when row count is not equal
            List<ByteBuffer> fakeList2 = new ArrayList<>();
            fakeList2.add(ByteBuffer.allocate(2));
            fields.set(9, new InsertParam.Field("field9", fakeList2));
            assertThrows(ParamException.class, () -> paramBuilder.withFields(fields).build());

            // test return error with wrong dimension of binary vector
            bVectors.remove(rowCount - 1);
            bVectors.add(ByteBuffer.allocate(1));
            fields.set(9, new InsertParam.Field("field9", bVectors));
            resp = client.insert(paramBuilder.withFields(fields).build());
            assertNotEquals(R.Status.Success.getCode(), resp.getStatus());


            // test return error without server
            server.stop();
            resp = client.insert(param);
            assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

            // test return error when client channel is shutdown
            client.close();
            resp = client.insert(param);
            assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());

            // stop mock server
            server.stop();
        }

        {
            // start mock server
            MockMilvusServer server = startServer();
            MilvusClient client = startClient();

            // test return ok with insertAsync
            try {
                ListenableFuture<R<MutationResult>> respFuture = client.insertAsync(param);
                R<MutationResult> response = respFuture.get();
                assertEquals(R.Status.Success.getCode(), response.getStatus());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            // stop mock server
            server.stop();

            // test return error without server
            try {
                ListenableFuture<R<MutationResult>> respFuture = client.insertAsync(param);
                R<MutationResult> response = respFuture.get();
                assertNotEquals(R.Status.Success.getCode(), response.getStatus());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            // test return error when client channel is shutdown
            client.close();
            try {
                ListenableFuture<R<MutationResult>> respFuture = client.insertAsync(param);
                R<MutationResult> response = respFuture.get();
                assertNotEquals(R.Status.Success.getCode(), response.getStatus());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            // stop mock server
            server.stop();
        }
    }

    @Test
    void deleteParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> DeleteParam.newBuilder()
                .withCollectionName("")
                .withExpr("dummy")
                .build()
        );

        assertThrows(ParamException.class, () -> DeleteParam.newBuilder()
                .withCollectionName("collection1")
                .withExpr("")
                .build()
        );
    }

    @Test
    void delete() {
        DeleteParam param = DeleteParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .withExpr("dummy")
                .build();

        testFuncByName("delete", param);
    }

    @Test
    void searchParam() {
        // test throw exception with illegal input
        List<String> partitions = Collections.singletonList("partition1");
        List<String> outputFields = Collections.singletonList("field1");
        List<List<Float>> vectors = new ArrayList<>();

        // target vector is empty
        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .addPartitionName("p2")
                .withParams("{}")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(vectors)
                .withExpr("dummy")
                .build()
        );

        // collection name is empty
        List<Float> vector1 = Collections.singletonList(0.1F);
        vectors.add(vector1);
        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("")
                .withPartitionNames(partitions)
                .withParams("{}")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(vectors)
                .withExpr("dummy")
                .build()
        );

        // target field name is empty
        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("{}")
                .withOutFields(outputFields)
                .withVectorFieldName("")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(vectors)
                .withExpr("dummy")
                .build()
        );

        // illegal topk value
        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("{}")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.IP)
                .withTopK(0)
                .withVectors(vectors)
                .withExpr("dummy")
                .build()
        );

        // target vector type must be Lst<Float> or ByteBuffer
        List<String> fakeVectors1 = Collections.singletonList("fake");
        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("{}")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(fakeVectors1)
                .withExpr("dummy")
                .build()
        );

        // float vector field's value must be Lst<Float>
        List<List<String>> fakeVectors2 = Collections.singletonList(fakeVectors1);
        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("{}")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(fakeVectors2)
                .withExpr("dummy")
                .build()
        );

        // float vector dimension not equal
        List<Float> vector2 = Arrays.asList(0.1F, 0.2F);
        vectors.add(vector2);
        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("{}")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(vectors)
                .withExpr("dummy")
                .build()
        );

        // binary vector dimension not equal
        ByteBuffer buf1 = ByteBuffer.allocate(1);
        buf1.put((byte) 1);
        ByteBuffer buf2 = ByteBuffer.allocate(2);
        buf2.put((byte) 1);
        buf2.put((byte) 2);
        List<ByteBuffer> binVectors = Arrays.asList(buf1, buf2);
        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("{}")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.HAMMING)
                .withTopK(5)
                .withVectors(binVectors)
                .withExpr("dummy")
                .build()
        );

        // succeed float vector case
        List<List<Float>> vectors2 = Collections.singletonList(vector2);
        assertDoesNotThrow(() -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("{}")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.L2)
                .withTopK(5)
                .withVectors(vectors2)
                .withExpr("dummy")
                .withIgnoreGrowing(Boolean.TRUE)
                .build()
        );

        // succeed binary vector case
        List<ByteBuffer> binVectors2 = Collections.singletonList(buf2);
        assertDoesNotThrow(() -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("{}")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.HAMMING)
                .withTopK(5)
                .withVectors(binVectors2)
                .withExpr("dummy")
                .build()
        );

        // param is not json format
        assertDoesNotThrow(() -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("dummy")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.L2)
                .withTopK(5)
                .withVectors(vectors2)
                .withExpr("dummy")
                .build()
        );
    }

    @Test
    void search() {
        List<String> partitions = Collections.singletonList("partition1");
        List<String> outputFields = Collections.singletonList("field2");

        List<List<Float>> vectors = new ArrayList<>();
        List<Float> vector1 = Arrays.asList(0.1f, 0.2f);
        vectors.add(vector1);
        SearchParam param = SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("{}")
                .withOutFields(outputFields)
                .addOutField("f2")
                .withVectorFieldName("field1")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(vectors)
                .withExpr("dummy")
                .withRoundDecimal(5)
                .withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
                .build();
        testFuncByName("search", param);

        List<ByteBuffer> bVectors = new ArrayList<>();
        ByteBuffer buf = ByteBuffer.allocate(2);
        buf.put((byte) 1);
        buf.put((byte) 2);
        bVectors.add(buf);
        param = SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("{}")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.HAMMING)
                .withTopK(5)
                .withVectors(bVectors)
                .withExpr("dummy")
                .build();

        testFuncByName("search", param);
        testAsyncFuncByName("searchAsync", param);
    }

    @Test
    void hybridSearch() {
        List<List<Float>> vectors = new ArrayList<>();
        List<Float> vector1 = Arrays.asList(0.1f, 0.2f);
        vectors.add(vector1);
        AnnSearchParam param1 = AnnSearchParam.newBuilder()
                .withParams("{}")
                .withVectorFieldName("field1")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withFloatVectors(vectors)
                .withExpr("dummy")
                .build();

        List<ByteBuffer> bVectors = new ArrayList<>();
        ByteBuffer buf = ByteBuffer.allocate(2);
        buf.put((byte) 1);
        buf.put((byte) 2);
        bVectors.add(buf);
        AnnSearchParam param2 = AnnSearchParam.newBuilder()
                .withParams("{}")
                .withVectorFieldName("field2")
                .withMetricType(MetricType.HAMMING)
                .withTopK(5)
                .withBinaryVectors(bVectors)
                .withExpr("dummy")
                .build();

        List<String> partitions = Collections.singletonList("partition1");
        List<String> outputFields = Collections.singletonList("field2");
        HybridSearchParam param = HybridSearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withOutFields(outputFields)
                .addOutField("f2")
                .addSearchRequest(param1)
                .addSearchRequest(param2)
                .withTopK(15)
                .withRoundDecimal(5)
                .withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
                .withRanker(RRFRanker.newBuilder().withK(10).build())
                .build();

        testFuncByName("hybridSearch", param);
        testAsyncFuncByName("hybridSearchAsync", param);
    }

    @Test
    void queryParam() {
        // test throw exception with illegal input
        List<String> partitions = Collections.singletonList("partition1");
        List<String> outputFields = Collections.singletonList("field1");

        // empty collection name
        assertThrows(ParamException.class, () -> QueryParam.newBuilder()
                .withCollectionName("")
                .withPartitionNames(partitions)
                .addPartitionName("p2")
                .withOutFields(outputFields)
                .addOutField("f2")
                .withExpr("dummy")
                .build()
        );

        // negative topk
        assertThrows(ParamException.class, () -> QueryParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withOutFields(outputFields)
                .withExpr("dummy")
                .withLimit(-1L)
                .build()
        );

        // negative offset
        assertThrows(ParamException.class, () -> QueryParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withOutFields(outputFields)
                .withExpr("dummy")
                .withOffset(-1L)
                .build()
        );

        // success
        assertDoesNotThrow(() -> QueryParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withOutFields(outputFields)
                .withExpr("dummy")
                .withOffset(1L)
                .withLimit(1L)
                .withIgnoreGrowing(Boolean.TRUE)
                .build()
        );
    }

    @Test
    void query() {
        List<String> partitions = Collections.singletonList("partition1");
        List<String> outputFields = Collections.singletonList("field2");
        QueryParam param = QueryParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withOutFields(outputFields)
                .addOutField("d1")
                .withExpr("dummy")
                .withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
                .build();

        testFuncByName("query", param);
        testAsyncFuncByName("queryAsync", param);
    }

    @Test
    void getMetricsParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> GetMetricsParam.newBuilder()
                .withRequest("")
                .build()
        );
    }

    @Test
    void getMetrics() {
        GetMetricsParam param = GetMetricsParam.newBuilder()
                .withRequest("{}")
                .build();

        testFuncByName("getMetrics", param);
    }

    @Test
    void getFlushStateParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> GetFlushStateParam.newBuilder()
                .build()
        );
    }

    @Test
    void getFlushState() {
        List<Long> ids = Arrays.asList(1L, 2L);
        GetFlushStateParam param = GetFlushStateParam.newBuilder()
                .withCollectionName("dummy")
                .withFlushTs(100L)
                .build();

        testFuncByName("getFlushState", param);
    }

    @Test
    void getPersistentSegmentInfoParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> GetPersistentSegmentInfoParam.newBuilder()
                .withCollectionName("")
                .build()
        );
    }

    @Test
    void getPersistentSegmentInfo() {
        GetPersistentSegmentInfoParam param = GetPersistentSegmentInfoParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        testFuncByName("getPersistentSegmentInfo", param);
    }

    @Test
    void getQuerySegmentInfoParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> GetQuerySegmentInfoParam
                .newBuilder()
                .withCollectionName("")
                .build()
        );
    }

    @Test
    void getQuerySegmentInfo() {
        GetQuerySegmentInfoParam param = GetQuerySegmentInfoParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        testFuncByName("getQuerySegmentInfo", param);
    }

    @Test
    void getReplicasParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> GetQuerySegmentInfoParam
                .newBuilder()
                .withCollectionName("")
                .build()
        );
    }

    @Test
    void getReplicas() {
        GetReplicasParam param = GetReplicasParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        testFuncByName("getReplicas", param);
    }

    @Test
    void loadBalanceParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> LoadBalanceParam
                .newBuilder()
                .withSourceNodeID(1L)
                .withDestinationNodeID(Arrays.asList(2L, 3L))
                .addDestinationNodeID(4L)
                .build()
        );

        assertThrows(ParamException.class, () -> LoadBalanceParam
                .newBuilder()
                .withSourceNodeID(1L)
                .withSegmentIDs(Arrays.asList(2L, 3L))
                .addSegmentID(4L)
                .build()
        );
    }

    @Test
    void loadBalance() {
        LoadBalanceParam param = LoadBalanceParam.newBuilder()
                .withSourceNodeID(1L)
                .addDestinationNodeID(2L)
                .addSegmentID(3L)
                .build();

        testFuncByName("loadBalance", param);
    }

    @Test
    void getCompactionState() {
        GetCompactionStateParam param = GetCompactionStateParam.newBuilder()
                .withCompactionID(1L)
                .build();

        testFuncByName("getCompactionState", param);
    }

    @Test
    void manualCompact() {
        ManualCompactParam param = ManualCompactParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        testFuncByName("manualCompact", param);
    }

    @Test
    void getCompactionStateWithPlans() {
        GetCompactionPlansParam param = GetCompactionPlansParam.newBuilder()
                .withCompactionID(1L)
                .build();

        testFuncByName("getCompactionStateWithPlans", param);
    }

    @Test
    void createCredentialParam() {
        assertThrows(ParamException.class, () -> CreateCredentialParam
                .newBuilder()
                .withUsername(" ")
                .withPassword("password")
                .build()
        );

        assertThrows(ParamException.class, () -> CreateCredentialParam
                .newBuilder()
                .withUsername("username")
                .withPassword(" ")
                .build()
        );
    }

    @Test
    void createCredential() {
        testFuncByName("createCredential", CreateCredentialParam
                .newBuilder()
                .withUsername("username")
                .withPassword("password")
                .build()
        );
    }

    @Test
    void updateCredentialParam() {
        assertThrows(ParamException.class, () -> UpdateCredentialParam
                .newBuilder()
                .withUsername("  ")
                .withOldPassword("oldPassword")
                .withNewPassword("newPassword")
                .build()
        );

        assertDoesNotThrow(() -> UpdateCredentialParam
                .newBuilder()
                .withUsername("username")
                .withOldPassword("")
                .withNewPassword("newPassword")
                .build()
        );

        assertThrows(ParamException.class, () -> UpdateCredentialParam
                .newBuilder()
                .withUsername("username")
                .withOldPassword("oldPassword")
                .withNewPassword("  ")
                .build()
        );
    }

    @Test
    void updateCredential() {
        testFuncByName("updateCredential", UpdateCredentialParam
                .newBuilder()
                .withUsername("username")
                .withOldPassword("oldPassword")
                .withNewPassword("newPassword")
                .build()
        );
    }

    @Test
    void deleteCredentialParam() {
        assertThrows(ParamException.class, () -> DeleteCredentialParam
                .newBuilder()
                .withUsername(" ")
                .build()
        );
    }

    @Test
    void deleteCredential() {
        testFuncByName("deleteCredential", DeleteCredentialParam
                .newBuilder()
                .withUsername("username")
                .build()
        );
    }

    @Test
    void listCredUsers() {
        testFuncByName("listCredUsers", ListCredUsersParam
                .newBuilder()
                .build()
        );
    }

    @Test
    void getLoadingProgress() {
        List<String> partitions = Collections.singletonList("partition1");
        testFuncByName("getLoadingProgress", GetLoadingProgressParam
                .newBuilder()
                .withCollectionName("dummy")
                .withPartitionNames(partitions)
                .build()
        );
    }

    @Test
    void getLoadState() {
        List<String> partitions = Collections.singletonList("partition1");
        testFuncByName("getLoadState", GetLoadStateParam
                .newBuilder()
                .withCollectionName("dummy")
                .withPartitionNames(partitions)
                .build()
        );
    }
}
