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

package io.milvus.client;

import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.alias.AlterAliasParam;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.alias.DropAliasParam;
import io.milvus.param.collection.*;
import io.milvus.param.control.GetMetricsParam;
import io.milvus.param.control.GetPersistentSegmentInfoParam;
import io.milvus.param.control.GetQuerySegmentInfoParam;
import io.milvus.param.dml.*;
import io.milvus.param.index.*;
import io.milvus.param.partition.*;
import io.milvus.server.MockMilvusServer;
import io.milvus.server.MockMilvusServerImpl;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MilvusServiceClientTest {
    private final String testHost = "localhost";
    private final int testPort = 53019;
    private MockMilvusServerImpl mockServerImpl;

    private MockMilvusServer startServer() {
        mockServerImpl = new MockMilvusServerImpl();
        MockMilvusServer mockServer = new MockMilvusServer(testPort, mockServerImpl);
        mockServer.start();
        return mockServer;
    }

    private MilvusServiceClient startClient() {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost(testHost)
                .withPort(testPort)
                .build();
        return new MilvusServiceClient(connectParam);
    }

    @Test
    void connectParam() {
        System.out.println(System.getProperty("os.name"));
        System.out.println(System.getProperty("os.arch"));

        String host = "dummyHost";
        int port = 100;
        long connectTimeoutMs = 1;
        long keepAliveTimeMs = 2;
        long keepAliveTimeoutMs = 3;
        long idleTimeoutMs = 5;
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost(host)
                .withPort(port)
                .withConnectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                .withKeepAliveTime(keepAliveTimeMs, TimeUnit.MILLISECONDS)
                .withKeepAliveTimeout(keepAliveTimeoutMs, TimeUnit.NANOSECONDS)
                .keepAliveWithoutCalls(true)
                .withIdleTimeout(idleTimeoutMs, TimeUnit.MILLISECONDS)
                .build();
        assertEquals(host.compareTo(connectParam.getHost()), 0);
        assertEquals(connectParam.getPort(), port);
        assertEquals(connectParam.getConnectTimeoutMs(), connectTimeoutMs);
        assertEquals(connectParam.getKeepAliveTimeMs(), keepAliveTimeMs);
        assertEquals(connectParam.getKeepAliveTimeoutMs(), keepAliveTimeoutMs);
        assertTrue(connectParam.isKeepAliveWithoutCalls());
        assertEquals(connectParam.getIdleTimeoutMs(), idleTimeoutMs);
    }

    @Test
    void createCollectionParam() {
        // test throw exception with illegal input
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
                        .withShardsNum(0)
                        .addFieldType(fieldType1)
                    .build()
        );

        List<FieldType> fields = Collections.singletonList(null);
        assertThrows(ParamException.class, () ->
                CreateCollectionParam
                        .newBuilder()
                        .withCollectionName("collection1")
                        .withShardsNum(0)
                        .withFieldTypes(fields)
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
                .withShardsNum(2)
                .addFieldType(fieldType1)
                .build();

        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        // test return ok with correct input
        R<RpcStatus> resp = client.createCollection(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.createCollection(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.createCollection(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        DescribeCollectionParam param = DescribeCollectionParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        // test return ok with correct input
        R<DescribeCollectionResponse> resp = client.describeCollection(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.describeCollection(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.describeCollection(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        DropCollectionParam param = DropCollectionParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        // test return ok with correct input
        R<RpcStatus> resp = client.dropCollection(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.dropCollection(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.dropCollection(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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
        MilvusServiceClient client = startClient();

        GetCollectionStatisticsParam param = GetCollectionStatisticsParam.newBuilder()
                .withCollectionName("collection1")
                .withFlush(Boolean.TRUE)
                .build();

        // test return ok with correct input
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
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        HasCollectionParam param = HasCollectionParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        // test return ok with correct input
        R<Boolean> resp = client.hasCollection(param);
        assertFalse(resp.getData());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.hasCollection(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.hasCollection(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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
    }

    @Test
    void loadCollection() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        String collectionName = "collection1";
        LoadCollectionParam param = LoadCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .withSyncLoad(Boolean.FALSE)
                .build();

        // test return ok with correct input
        R<RpcStatus> resp = client.loadCollection(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return ok for sync mode loading
        ShowCollectionsResponse showResponse = ShowCollectionsResponse.newBuilder()
                .addCollectionNames(collectionName)
                .addInMemoryPercentages(100)
                .build();
        mockServerImpl.setShowCollectionsResponse(showResponse);

        param = LoadCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .withSyncLoad(Boolean.TRUE)
                .withSyncLoadWaitingInterval(10L)
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
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        ReleaseCollectionParam param = ReleaseCollectionParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        // test return ok with correct input
        R<RpcStatus> resp = client.releaseCollection(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.releaseCollection(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.releaseCollection(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void showCollectionsParam() {
        // test throw exception with illegal input
        List<String> names = new ArrayList<>();
        names.add(null);
        assertThrows(ParamException.class, () ->
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
        assertEquals(param.getShowType(), ShowType.All);

        param = ShowCollectionsParam.newBuilder()
                .addCollectionName("collection1")
                .build();
        assertEquals(param.getShowType(), ShowType.InMemory);
    }

    @Test
    void showCollections() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        ShowCollectionsParam param = ShowCollectionsParam.newBuilder()
                .addCollectionName("collection1")
                .addCollectionName("collection2")
                .build();

        // test return ok with correct input
        R<ShowCollectionsResponse> resp = client.showCollections(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.showCollections(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.showCollections(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void flushParam() {
        // test throw exception with illegal input
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
                .addCollectionName("collection1")
                .withSyncFlush(Boolean.TRUE)
                .withSyncFlushWaitingInterval(Constant.MAX_WAITING_FLUSHING_INTERVAL + 1)
                .build()
        );
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
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        CreatePartitionParam param = CreatePartitionParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build();

        // test return ok with correct input
        R<RpcStatus> resp = client.createPartition(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.createPartition(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.createPartition(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        DropPartitionParam param = DropPartitionParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build();

        // test return ok with correct input
        R<RpcStatus> resp = client.dropPartition(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.dropPartition(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.dropPartition(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        HasPartitionParam param = HasPartitionParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build();

        // test return ok with correct input
        R<Boolean> resp = client.hasPartition(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.hasPartition(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.hasPartition(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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

        List<String> names = new ArrayList<>();
        names.add(null);
        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
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
    }

    @Test
    void loadPartitions() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

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
        ShowPartitionsResponse showResponse = ShowPartitionsResponse.newBuilder()
                .addPartitionNames(partitionName)
                .addInMemoryPercentages(100)
                .build();
        mockServerImpl.setShowPartitionsResponse(showResponse);

        param = LoadPartitionsParam.newBuilder()
                .withCollectionName(collectionName)
                .addPartitionName(partitionName)
                .withSyncLoad(Boolean.TRUE)
                .withSyncLoadWaitingInterval(10L)
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

        List<String> names = new ArrayList<>();
        names.add(null);
        assertThrows(ParamException.class, () -> ReleasePartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(names)
                .build()
        );
    }

    @Test
    void releasePartitions() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        ReleasePartitionsParam param = ReleasePartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .addPartitionName("partition1")
                .build();

        // test return ok with correct input
        R<RpcStatus> resp = client.releasePartitions(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.releasePartitions(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.releasePartitions(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void getPartitionStatisticsParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> GetPartitionStatisticsParam.newBuilder()
                .withCollectionName("")
                .withPartitionName("partition1")
                .build()
        );

        assertThrows(ParamException.class, () -> GetPartitionStatisticsParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("")
                .build()
        );
    }

    @Test
    void getPartitionStatistics() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        GetPartitionStatisticsParam param = GetPartitionStatisticsParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build();

        // test return ok with correct input
        R<GetPartitionStatisticsResponse> resp = client.getPartitionStatistics(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.getPartitionStatistics(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.getPartitionStatistics(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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
        assertThrows(ParamException.class, () -> ShowPartitionsParam.newBuilder()
                .withCollectionName("collection1`")
                .withPartitionNames(names)
                .build()
        );

        // verify internal param
        ShowPartitionsParam param = ShowPartitionsParam.newBuilder()
                .withCollectionName("collection1`")
                .build();
        assertEquals(param.getShowType(), ShowType.All);

        param = ShowPartitionsParam.newBuilder()
                .withCollectionName("collection1`")
                .addPartitionName("partition1")
                .build();
        assertEquals(param.getShowType(), ShowType.InMemory);
    }

    @Test
    void showPartitions() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        ShowPartitionsParam param = ShowPartitionsParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        // test return ok with correct input
        R<ShowPartitionsResponse> resp = client.showPartitions(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.showPartitions(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.showPartitions(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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
    }

    @Test
    void createAlias() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        CreateAliasParam param = CreateAliasParam.newBuilder()
                .withCollectionName("collection1")
                .withAlias("alias1")
                .build();

        // test return ok with correct input
        R<RpcStatus> resp = client.createAlias(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.createAlias(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.createAlias(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        DropAliasParam param = DropAliasParam.newBuilder()
                .withAlias("alias1")
                .build();

        // test return ok with correct input
        R<RpcStatus> resp = client.dropAlias(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.dropAlias(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.dropAlias(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void alterAliasParam() {
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
    }

    @Test
    void alterAlias() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        AlterAliasParam param = AlterAliasParam.newBuilder()
                .withCollectionName("collection1")
                .withAlias("alias1")
                .build();

        // test return ok with correct input
        R<RpcStatus> resp = client.alterAlias(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.alterAlias(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.alterAlias(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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
                .withIndexType(IndexType.INVALID)
                .withMetricType(MetricType.L2)
                .withExtraParam("dummy")
                .build()
        );

        assertThrows(ParamException.class, () -> CreateIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.INVALID)
                .withExtraParam("dummy")
                .build()
        );

        assertThrows(ParamException.class, () -> CreateIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("")
                .build()
        );
    }

    @Test
    void createIndex() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        CreateIndexParam param = CreateIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("dummy")
                .build();

        // test return ok with correct input
        R<RpcStatus> resp = client.createIndex(param);
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
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> DescribeIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("")
                .build()
        );

        assertThrows(ParamException.class, () -> DescribeIndexParam.newBuilder()
                .withCollectionName("")
                .withFieldName("field1")
                .build()
        );
    }

    @Test
    void describeIndex() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        DescribeIndexParam param = DescribeIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .build();

        // test return ok with correct input
        R<DescribeIndexResponse> resp = client.describeIndex(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.describeIndex(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.describeIndex(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void getIndexStateParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> GetIndexStateParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("")
                .build()
        );

        assertThrows(ParamException.class, () -> GetIndexStateParam.newBuilder()
                .withCollectionName("")
                .withFieldName("field1")
                .build()
        );
    }

    @Test
    void getIndexState() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        GetIndexStateParam param = GetIndexStateParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .build();

        // test return ok with correct input
        R<GetIndexStateResponse> resp = client.getIndexState(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.getIndexState(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.getIndexState(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        GetIndexBuildProgressParam param = GetIndexBuildProgressParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        // test return ok with correct input
        R<GetIndexBuildProgressResponse> resp = client.getIndexBuildProgress(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.getIndexBuildProgress(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.getIndexBuildProgress(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void dropIndexParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> DropIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("")
                .build()
        );

        assertThrows(ParamException.class, () -> DropIndexParam.newBuilder()
                .withCollectionName("")
                .withFieldName("field1")
                .build()
        );
    }

    @Test
    void dropIndex() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        DropIndexParam param = DropIndexParam.newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .build();
        // test return ok with correct input
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
        fields.add(new InsertParam.Field("", DataType.Int64, ids));
        assertThrows(ParamException.class, () -> InsertParam.newBuilder()
                .withCollectionName("collection1")
                .withFields(fields)
                .build()
        );

        // field row count is 0
        fields.clear();
        fields.add(new InsertParam.Field("field1", DataType.Int64, ids));
        assertThrows(ParamException.class, () -> InsertParam.newBuilder()
                .withCollectionName("collection1")
                .withFields(fields)
                .build()
        );

        // field row count not equal
        fields.clear();
        ids.add(1L);
        fields.add(new InsertParam.Field("field1", DataType.Int64, ids));
        List<List<Float>> vectors = new ArrayList<>();
        fields.add(new InsertParam.Field("field2", DataType.FloatVector, vectors));
        assertThrows(ParamException.class, () -> InsertParam.newBuilder()
                .withCollectionName("collection1")
                .withFields(fields)
                .build()
        );

        // wrong type
        fields.clear();
        List<String> fakeVectors1 = new ArrayList<>();
        fields.add(new InsertParam.Field("field2", DataType.FloatVector, fakeVectors1));
        assertThrows(ParamException.class, () -> InsertParam.newBuilder()
                .withCollectionName("collection1")
                .withFields(fields)
                .build()
        );

        fields.clear();
        List<List<String>> fakeVectors2 = new ArrayList<>();
        fields.add(new InsertParam.Field("field2", DataType.FloatVector, fakeVectors2));
        assertThrows(ParamException.class, () -> InsertParam.newBuilder()
                .withCollectionName("collection1")
                .withFields(fields)
                .build()
        );

        fields.clear();
        fields.add(new InsertParam.Field("field2", DataType.BinaryVector, fakeVectors1));
        assertThrows(ParamException.class, () -> InsertParam.newBuilder()
                .withCollectionName("collection1")
                .withFields(fields)
                .build()
        );

        // vector dimension not equal
        fields.clear();
        List<Float> vector1 = Arrays.asList(0.1F, 0.2F, 0.3F);
        List<Float> vector2 = Arrays.asList(0.1F, 0.2F);
        vectors.add(vector1);
        vectors.add(vector2);
        fields.add(new InsertParam.Field("field1", DataType.FloatVector, vectors));
        assertThrows(ParamException.class, () -> InsertParam.newBuilder()
                .withCollectionName("collection1")
                .withFields(fields)
                .build()
        );

        fields.clear();
        ByteBuffer buf1 = ByteBuffer.allocate(1);
        buf1.put((byte) 1);
        ByteBuffer buf2 = ByteBuffer.allocate(2);
        buf2.put((byte) 1);
        buf2.put((byte) 2);
        List<ByteBuffer> binVectors = Arrays.asList(buf1, buf2);
        fields.add(new InsertParam.Field("field2", DataType.BinaryVector, binVectors));
        assertThrows(ParamException.class, () -> InsertParam.newBuilder()
                .withCollectionName("collection1")
                .withFields(fields)
                .build()
        );
    }

    @Test
    void insert() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        List<InsertParam.Field> fields = new ArrayList<>();
        List<Long> ids = Collections.singletonList(1L);
        fields.add(new InsertParam.Field("field1", DataType.Int64, ids));
        InsertParam param = InsertParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .withFields(fields)
                .build();

        // test return ok with correct input
        R<MutationResult> resp = client.insert(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.insert(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.insert(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        DeleteParam param = DeleteParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .withExpr("dummy")
                .build();

        // test return ok with correct input
        R<MutationResult> resp = client.delete(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.delete(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.delete(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void searchParam() {
        // test throw exception with illegal input
        List<String> partitions = Collections.singletonList("partition1");
        List<String> outputFields = Collections.singletonList("field2");
        List<List<Float>> vectors = new ArrayList<>();
        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("dummy")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(vectors)
                .withExpr("dummy")
                .build()
        );

        List<Float> vector1 = Collections.singletonList(0.1F);
        vectors.add(vector1);
        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("")
                .withPartitionNames(partitions)
                .withParams("dummy")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(vectors)
                .withExpr("dummy")
                .build()
        );

        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("dummy")
                .withOutFields(outputFields)
                .withVectorFieldName("")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(vectors)
                .withExpr("dummy")
                .build()
        );

        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("dummy")
                .withOutFields(outputFields)
                .withVectorFieldName("")
                .withMetricType(MetricType.INVALID)
                .withTopK(5)
                .withVectors(vectors)
                .withExpr("dummy")
                .build()
        );

        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("dummy")
                .withOutFields(outputFields)
                .withVectorFieldName("")
                .withMetricType(MetricType.IP)
                .withTopK(0)
                .withVectors(vectors)
                .withExpr("dummy")
                .build()
        );

        // vector type illegal
        List<String> fakeVectors1 = Collections.singletonList("fake");
        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("dummy")
                .withOutFields(outputFields)
                .withVectorFieldName("")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(fakeVectors1)
                .withExpr("dummy")
                .build()
        );

        List<List<String>> fakeVectors2 = Collections.singletonList(fakeVectors1);
        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("dummy")
                .withOutFields(outputFields)
                .withVectorFieldName("")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(fakeVectors2)
                .withExpr("dummy")
                .build()
        );

        // vector dimension not equal
        List<Float> vector2 = Arrays.asList(0.1F, 0.2F);
        vectors.add(vector2);
        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("dummy")
                .withOutFields(outputFields)
                .withVectorFieldName("")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(vectors)
                .withExpr("dummy")
                .build()
        );

        ByteBuffer buf1 = ByteBuffer.allocate(1);
        buf1.put((byte) 1);
        ByteBuffer buf2 = ByteBuffer.allocate(2);
        buf2.put((byte) 1);
        buf2.put((byte) 2);
        List<ByteBuffer> binVectors = Arrays.asList(buf1, buf2);
        assertThrows(ParamException.class, () -> SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("dummy")
                .withOutFields(outputFields)
                .withVectorFieldName("")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(binVectors)
                .withExpr("dummy")
                .build()
        );
    }

    @Test
    void search() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        List<String> partitions = Collections.singletonList("partition1");
        List<String> outputFields = Collections.singletonList("field2");
        List<List<Float>> vectors = new ArrayList<>();
        List<Float> vector1 = Collections.singletonList(0.1F);
        vectors.add(vector1);
        SearchParam param = SearchParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("dummy")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(vectors)
                .withExpr("dummy")
                .build();

        // test return ok with correct input
        R<SearchResults> resp = client.search(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.search(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.search(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void queryParam() {
        // test throw exception with illegal input
        List<String> partitions = Collections.singletonList("partition1");
        List<String> outputFields = Collections.singletonList("field2");
        assertThrows(ParamException.class, () -> QueryParam.newBuilder()
                .withCollectionName("")
                .withPartitionNames(partitions)
                .withOutFields(outputFields)
                .withExpr("dummy")
                .build()
        );

        assertThrows(ParamException.class, () -> QueryParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withOutFields(outputFields)
                .withExpr("")
                .build()
        );
    }

    @Test
    void query() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        List<String> partitions = Collections.singletonList("partition1");
        List<String> outputFields = Collections.singletonList("field2");
        QueryParam param = QueryParam.newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withOutFields(outputFields)
                .withExpr("dummy")
                .build();
        // test return ok with correct input
        R<QueryResults> resp = client.query(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.query(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.query(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void calcDistanceParam() {
        // test throw exception with illegal input
        List<List<Float>> vectorsLeft = new ArrayList<>();
        List<List<Float>> vectorsRight = new ArrayList<>();
        List<Float> vector1 = Collections.singletonList(0.1F);
        List<Float> vector2 = Collections.singletonList(0.1F);
        vectorsLeft.add(vector1);
        vectorsRight.add(vector2);

        assertThrows(ParamException.class, () -> CalcDistanceParam.newBuilder()
                .withVectorsLeft(vectorsLeft)
                .withVectorsRight(vectorsRight)
                .withMetricType(MetricType.INVALID)
                .build()
        );

        vectorsLeft.clear();
        assertThrows(ParamException.class, () -> CalcDistanceParam.newBuilder()
                .withVectorsLeft(vectorsLeft)
                .withVectorsRight(vectorsRight)
                .withMetricType(MetricType.IP)
                .build()
        );

        vectorsLeft.add(vector1);
        vectorsRight.clear();
        assertThrows(ParamException.class, () -> CalcDistanceParam.newBuilder()
                .withVectorsLeft(vectorsLeft)
                .withVectorsRight(vectorsRight)
                .withMetricType(MetricType.IP)
                .build()
        );

        // vector dimension not equal
        vectorsRight.add(vector2);
        List<Float> vector3 = Arrays.asList(0.1F, 0.2F);
        vectorsLeft.add(vector3);
        assertThrows(ParamException.class, () -> CalcDistanceParam.newBuilder()
                .withVectorsLeft(vectorsLeft)
                .withVectorsRight(vectorsRight)
                .withMetricType(MetricType.IP)
                .build()
        );

        vectorsLeft.clear();
        vectorsLeft.add(vector1);
        List<Float> vector4 = Arrays.asList(0.1F, 0.2F);
        vectorsRight.add(vector4);
        assertThrows(ParamException.class, () -> CalcDistanceParam.newBuilder()
                .withVectorsLeft(vectorsLeft)
                .withVectorsRight(vectorsRight)
                .withMetricType(MetricType.IP)
                .build()
        );
    }

    @Test
    void calcDistance() {
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        List<List<Float>> vectorsLeft = new ArrayList<>();
        List<List<Float>> vectorsRight = new ArrayList<>();
        List<Float> vector1 = Collections.singletonList(0.1F);
        List<Float> vector2 = Collections.singletonList(0.1F);
        vectorsLeft.add(vector1);
        vectorsRight.add(vector2);
        CalcDistanceParam param = CalcDistanceParam.newBuilder()
                .withVectorsLeft(vectorsLeft)
                .withVectorsRight(vectorsRight)
                .withMetricType(MetricType.L2)
                .build();

        // test return ok with correct input
        R<CalcDistanceResults> resp = client.calcDistance(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.calcDistance(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.calcDistance(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        GetMetricsParam param = GetMetricsParam.newBuilder()
                .withRequest("{}")
                .build();

        // test return ok with correct input
        R<GetMetricsResponse> resp = client.getMetrics(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.getMetrics(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.getMetrics(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        GetPersistentSegmentInfoParam param = GetPersistentSegmentInfoParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        // test return ok with correct input
        R<GetPersistentSegmentInfoResponse> resp = client.getPersistentSegmentInfo(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.getPersistentSegmentInfo(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.getPersistentSegmentInfo(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
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
        // start mock server
        MockMilvusServer server = startServer();
        MilvusServiceClient client = startClient();

        GetQuerySegmentInfoParam param = GetQuerySegmentInfoParam.newBuilder()
                .withCollectionName("collection1")
                .build();

        // test return ok with correct input
        R<GetQuerySegmentInfoResponse> resp = client.getQuerySegmentInfo(param);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.getQuerySegmentInfo(param);
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.getQuerySegmentInfo(param);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }
}