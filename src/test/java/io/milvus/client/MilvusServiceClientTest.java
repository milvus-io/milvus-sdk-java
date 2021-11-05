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
import io.milvus.param.dml.*;
import io.milvus.param.index.*;
import io.milvus.param.partition.*;
import io.milvus.server.MockMilvusServer;
import io.milvus.server.MockMilvusServerImpl;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MilvusServiceClientTest {
    private String testHost = "localhost";
    private int testPort = 53019;

    private MockMilvusServer startServer() {
        MockMilvusServer mockServer = new MockMilvusServer(testPort, new MockMilvusServerImpl());
        mockServer.start();
        return mockServer;
    }

    private MilvusServiceClient startClient() {
        ConnectParam connectParam = ConnectParam.Builder.newBuilder()
                .withHost(testHost)
                .withPort(testPort)
                .build();
        MilvusServiceClient milvusClient = new MilvusServiceClient(connectParam);
        return milvusClient;
    }

    @Test
    void createCollectionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            FieldType.Builder.newBuilder()
                    .withName("")
                    .withDataType(DataType.Int64)
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            FieldType.Builder.newBuilder()
                    .withName("userID")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            FieldType.Builder.newBuilder()
                    .withName("userID")
                    .withDataType(DataType.FloatVector)
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            CreateCollectionParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withShardsNum(2)
                    .build();
        });

        FieldType[] fieldTypes = new FieldType[1];
        FieldType fieldType1 = FieldType.Builder.newBuilder()
                .withName("userID")
                .withDescription("userId")
                .withDataType(DataType.Int64)
                .withAutoID(true)
                .withPrimaryKey(true)
                .build();
        fieldTypes[0] = fieldType1;

        assertThrows(ParamException.class, () -> {
            CreateCollectionParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withShardsNum(2)
                    .withFieldTypes(fieldTypes)
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            CreateCollectionParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withShardsNum(0)
                    .withFieldTypes(fieldTypes)
                    .build();
        });

        fieldTypes[0] = null;
        assertThrows(ParamException.class, () -> {
            CreateCollectionParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withShardsNum(0)
                    .withFieldTypes(fieldTypes)
                    .build();
        });
    }


    @Test
    void createCollection() {
        MilvusServiceClient client = startClient();

        FieldType[] fieldTypes = new FieldType[1];
        FieldType fieldType1 = FieldType.Builder.newBuilder()
                .withName("userID")
                .withDescription("userId")
                .withDataType(DataType.Int64)
                .withAutoID(true)
                .withPrimaryKey(true)
                .build();
        fieldTypes[0] = fieldType1;

        // start mock server
        MockMilvusServer server = startServer();

        // test return error with null param
        R<RpcStatus> resp = client.createCollection(null);
        assertEquals(R.Status.ParamError.getCode(), resp.getStatus());

        // test return ok with correct input
        resp = client.createCollection(CreateCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withShardsNum(2)
                .withFieldTypes(fieldTypes)
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.createCollection(CreateCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withShardsNum(2)
                .withFieldTypes(fieldTypes)
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.createCollection(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void describeCollectionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            DescribeCollectionParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .build();
        });
    }

    @Test
    void describeCollection() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<DescribeCollectionResponse> resp = client.describeCollection(DescribeCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.describeCollection(DescribeCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.describeCollection(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void dropCollectionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            DropCollectionParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .build();
        });
    }

    @Test
    void dropCollection() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<RpcStatus> resp = client.dropCollection(DropCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.dropCollection(DropCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.dropCollection(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void getCollectionStatisticsParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            GetCollectionStatisticsParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .build();
        });
    }

    @Test
    void getCollectionStatistics() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<GetCollectionStatisticsResponse> resp = client.getCollectionStatistics(GetCollectionStatisticsParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.getCollectionStatistics(GetCollectionStatisticsParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.getCollectionStatistics(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void hasCollectionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            HasCollectionParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .build();
        });
    }

    @Test
    void hasCollection() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<Boolean> resp = client.hasCollection(HasCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertFalse(resp.getData());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.hasCollection(HasCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.hasCollection(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void loadCollectionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            LoadCollectionParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .build();
        });
    }

    @Test
    void loadCollection() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<RpcStatus> resp = client.loadCollection(LoadCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.loadCollection(LoadCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.loadCollection(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void releaseCollectionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            ReleaseCollectionParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .build();
        });
    }

    @Test
    void releaseCollection() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<RpcStatus> resp = client.releaseCollection(ReleaseCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.releaseCollection(ReleaseCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.releaseCollection(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void showCollectionsParam() {
        // test throw exception with illegal input
        String[] names = new String[1];
        names[0] = null;
        assertThrows(ParamException.class, () -> {
            ShowCollectionsParam.Builder
                    .newBuilder()
                    .withCollectionNames(names)
                    .build();
        });

        names[0] = "";
        assertThrows(ParamException.class, () -> {
            ShowCollectionsParam.Builder
                    .newBuilder()
                    .withCollectionNames(names)
                    .build();
        });

        names[0] = "collection1";
        ShowCollectionsParam param = ShowCollectionsParam.Builder
                .newBuilder()
                .withCollectionNames(names)
                .build();
        assertEquals(param.getShowType(), ShowType.InMemory);
    }

    @Test
    void showCollections() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        String[] names = new String[2];
        names[0] = "collection1";
        names[1] = "collection2";
        R<ShowCollectionsResponse> resp = client.showCollections(ShowCollectionsParam.Builder
                .newBuilder()
                .withCollectionNames(names)
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.showCollections(ShowCollectionsParam.Builder
                .newBuilder()
                .withCollectionNames(names)
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.showCollections(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void flush() {
        MilvusServiceClient client = startClient();

        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            client.flush("");
        });

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<FlushResponse> resp = client.flush("collection1");
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        resp = client.flush("collection1", "db1");
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        List<String> collectionNames = new ArrayList<String>();
        collectionNames.add("collection1");
        resp = client.flush(collectionNames);
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.flush("collection1");
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.flush("collection1");
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void createPartitionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            CreatePartitionParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withPartitionName("partition1")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            CreatePartitionParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionName("")
                    .build();
        });
    }

    @Test
    void createPartition() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<RpcStatus> resp = client.createPartition(CreatePartitionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.createPartition(CreatePartitionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.createPartition(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void dropPartitionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            DropPartitionParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withPartitionName("partition1")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            DropPartitionParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionName("")
                    .build();
        });
    }

    @Test
    void dropPartition() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<RpcStatus> resp = client.dropPartition(DropPartitionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.dropPartition(DropPartitionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.dropPartition(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void hasPartitionParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            HasPartitionParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withPartitionName("partition1")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            HasPartitionParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionName("")
                    .build();
        });
    }

    @Test
    void hasPartition() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<Boolean> resp = client.hasPartition(HasPartitionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.hasPartition(HasPartitionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.hasPartition(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void loadPartitionsParam() {
        // test throw exception with illegal input
        String[] partitions = new String[1];
        partitions[0] = "partition1";
        assertThrows(ParamException.class, () -> {
            LoadPartitionsParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withPartitionNames(partitions)
                    .build();
        });

        partitions[0] = "";
        assertThrows(ParamException.class, () -> {
            LoadPartitionsParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionNames(partitions)
                    .build();
        });

        partitions[0] = null;
        assertThrows(ParamException.class, () -> {
            LoadPartitionsParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionNames(partitions)
                    .build();
        });
    }

    @Test
    void loadPartitions() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        String[] partitions = new String[1];
        partitions[0] = "partition1";
        R<RpcStatus> resp = client.loadPartitions(LoadPartitionsParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.loadPartitions(LoadPartitionsParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.loadPartitions(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void releasePartitionsParam() {
        // test throw exception with illegal input
        String[] partitions = new String[1];
        partitions[0] = "partition1";
        assertThrows(ParamException.class, () -> {
            ReleasePartitionsParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withPartitionNames(partitions)
                    .build();
        });

        partitions[0] = "";
        assertThrows(ParamException.class, () -> {
            ReleasePartitionsParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionNames(partitions)
                    .build();
        });

        partitions[0] = null;
        assertThrows(ParamException.class, () -> {
            ReleasePartitionsParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionNames(partitions)
                    .build();
        });
    }

    @Test
    void releasePartitions() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        String[] partitions = new String[1];
        partitions[0] = "partition1";
        R<RpcStatus> resp = client.releasePartitions(ReleasePartitionsParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.releasePartitions(ReleasePartitionsParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.releasePartitions(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void getPartitionStatisticsParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            GetPartitionStatisticsParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withPartitionName("partition1")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            GetPartitionStatisticsParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionName("")
                    .build();
        });
    }

    @Test
    void getPartitionStatistics() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<GetPartitionStatisticsResponse> resp = client.getPartitionStatistics(GetPartitionStatisticsParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.getPartitionStatistics(GetPartitionStatisticsParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionName("partition1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.getPartitionStatistics(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void showPartitionsParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            ShowPartitionsParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .build();
        });
    }

    @Test
    void showPartitions() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<ShowPartitionsResponse> resp = client.showPartitions(ShowPartitionsParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.showPartitions(ShowPartitionsParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.showPartitions(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void createAliasParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            CreateAliasParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withAlias("alias1")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            CreateAliasParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withAlias("")
                    .build();
        });
    }

    @Test
    void createAlias() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<RpcStatus> resp = client.createAlias(CreateAliasParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withAlias("alias1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.createAlias(CreateAliasParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withAlias("alias1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.createAlias(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void dropAliasParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            DropAliasParam.Builder
                    .newBuilder()
                    .withAlias("")
                    .build();
        });
    }

    @Test
    void dropAlias() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<RpcStatus> resp = client.dropAlias(DropAliasParam.Builder
                .newBuilder()
                .withAlias("alias1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.dropAlias(DropAliasParam.Builder
                .newBuilder()
                .withAlias("alias1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.dropAlias(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void alterAliasParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            CreateAliasParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withAlias("alias1")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            CreateAliasParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withAlias("")
                    .build();
        });
    }

    @Test
    void alterAlias() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<RpcStatus> resp = client.alterAlias(AlterAliasParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withAlias("alias1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.alterAlias(AlterAliasParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withAlias("alias1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.alterAlias(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void createIndexParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            CreateIndexParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withFieldName("field1")
                    .withIndexType(IndexType.IVF_FLAT)
                    .withMetricType(MetricType.L2)
                    .withExtraParam("dummy")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            CreateIndexParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFieldName("")
                    .withIndexType(IndexType.IVF_FLAT)
                    .withMetricType(MetricType.L2)
                    .withExtraParam("dummy")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            CreateIndexParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFieldName("field1")
                    .withIndexType(IndexType.INVALID)
                    .withMetricType(MetricType.L2)
                    .withExtraParam("dummy")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            CreateIndexParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFieldName("field1")
                    .withIndexType(IndexType.IVF_FLAT)
                    .withMetricType(MetricType.INVALID)
                    .withExtraParam("dummy")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            CreateIndexParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFieldName("field1")
                    .withIndexType(IndexType.IVF_FLAT)
                    .withMetricType(MetricType.L2)
                    .withExtraParam("")
                    .build();
        });
    }

    @Test
    void createIndex() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<RpcStatus> resp = client.createIndex(CreateIndexParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("dummy")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.createIndex(CreateIndexParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("dummy")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.createIndex(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void describeIndexParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            DescribeIndexParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFieldName("")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            DescribeIndexParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withFieldName("field1")
                    .build();
        });
    }

    @Test
    void describeIndex() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<DescribeIndexResponse> resp = client.describeIndex(DescribeIndexParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.describeIndex(DescribeIndexParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.describeIndex(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void getIndexStateParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            GetIndexStateParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFieldName("")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            GetIndexStateParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withFieldName("field1")
                    .build();
        });
    }

    @Test
    void getIndexState() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<GetIndexStateResponse> resp = client.getIndexState(GetIndexStateParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.getIndexState(GetIndexStateParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.getIndexState(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void getIndexBuildProgressParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            GetIndexBuildProgressParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .build();
        });
    }

    @Test
    void getIndexBuildProgress() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<GetIndexBuildProgressResponse> resp = client.getIndexBuildProgress(GetIndexBuildProgressParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.getIndexBuildProgress(GetIndexBuildProgressParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.getIndexBuildProgress(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void dropIndexParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            DropIndexParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFieldName("")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            DropIndexParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withFieldName("field1")
                    .build();
        });
    }

    @Test
    void dropIndex() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<RpcStatus> resp = client.dropIndex(DropIndexParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.dropIndex(DropIndexParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withFieldName("field1")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.dropIndex(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void insertParam() {
        // test throw exception with illegal input
        List<InsertParam.Field> fields = new ArrayList<>();

        // collection is empty
        assertThrows(ParamException.class, () -> {
            InsertParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withFields(fields)
                    .build();
        });

        // fields is empty
        assertThrows(ParamException.class, () -> {
            InsertParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFields(fields)
                    .build();
        });

        // field is null
        fields.add(null);
        assertThrows(ParamException.class, () -> {
            InsertParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFields(fields)
                    .build();
        });

        // field name is empty
        fields.clear();
        List<Long> ids = new ArrayList<>();
        fields.add(new InsertParam.Field("", DataType.Int64, ids));
        assertThrows(ParamException.class, () -> {
            InsertParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFields(fields)
                    .build();
        });

        // field row count is 0
        fields.clear();
        fields.add(new InsertParam.Field("field1", DataType.Int64, ids));
        assertThrows(ParamException.class, () -> {
            InsertParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFields(fields)
                    .build();
        });

        // field row count not equal
        fields.clear();
        ids.add(1L);
        fields.add(new InsertParam.Field("field1", DataType.Int64, ids));
        List<List<Float>> vectors = new ArrayList<>();
        fields.add(new InsertParam.Field("field2", DataType.FloatVector, vectors));
        assertThrows(ParamException.class, () -> {
            InsertParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFields(fields)
                    .build();
        });

        // wrong type
        fields.clear();
        List<String> fakeVectors1 = new ArrayList<>();
        fields.add(new InsertParam.Field("field2", DataType.FloatVector, fakeVectors1));
        assertThrows(ParamException.class, () -> {
            InsertParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFields(fields)
                    .build();
        });

        fields.clear();
        List<List<String>> fakeVectors2 = new ArrayList<>();
        fields.add(new InsertParam.Field("field2", DataType.FloatVector, fakeVectors2));
        assertThrows(ParamException.class, () -> {
            InsertParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFields(fields)
                    .build();
        });

        fields.clear();
        fields.add(new InsertParam.Field("field2", DataType.BinaryVector, fakeVectors1));
        assertThrows(ParamException.class, () -> {
            InsertParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFields(fields)
                    .build();
        });

        // vector dimension not equal
        fields.clear();
        List<Float> vector1 = Arrays.asList(0.1F);
        List<Float> vector2 = Arrays.asList(0.1F, 0.2F);
        vectors.add(vector1);
        vectors.add(vector2);
        fields.add(new InsertParam.Field("field1", DataType.FloatVector, vectors));
        assertThrows(ParamException.class, () -> {
            InsertParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFields(fields)
                    .build();
        });

        fields.clear();
        ByteBuffer buf1 = ByteBuffer.allocate(1);
        buf1.put((byte) 1);
        ByteBuffer buf2 = ByteBuffer.allocate(2);
        buf2.put((byte) 1);
        buf2.put((byte) 2);
        List<ByteBuffer> bvectors = Arrays.asList(buf1, buf2);
        fields.add(new InsertParam.Field("field2", DataType.BinaryVector, bvectors));
        assertThrows(ParamException.class, () -> {
            InsertParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withFields(fields)
                    .build();
        });
    }

    @Test
    void insert() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        List<InsertParam.Field> fields = new ArrayList<>();
        List<Long> ids = Arrays.asList(1L);
        fields.add(new InsertParam.Field("field1", DataType.Int64, ids));
        R<MutationResult> resp = client.insert(InsertParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withFields(fields)
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.insert(InsertParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withFields(fields)
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.insert(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void deleteParam() {
        // test throw exception with illegal input
        assertThrows(ParamException.class, () -> {
            DeleteParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withExpr("dummy")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            DeleteParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withExpr("")
                    .build();
        });
    }

    @Test
    void delete() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        R<MutationResult> resp = client.delete(DeleteParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withExpr("dummy")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.delete(DeleteParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withExpr("dummy")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.delete(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void searchParam() {
        // test throw exception with illegal input
        List<String> partitions = Arrays.asList("partition1");
        List<String> outputFields = Arrays.asList("field2");
        List<List<Float>> vectors = new ArrayList<>();
        assertThrows(ParamException.class, () -> {
            SearchParam.Builder
                    .newBuilder()
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
        });

        List<Float> vector1 = Arrays.asList(0.1F);
        vectors.add(vector1);
        assertThrows(ParamException.class, () -> {
            SearchParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withPartitionNames(partitions)
                    .withParams("dummy")
                    .withOutFields(outputFields)
                    .withVectorFieldName("field1")
                    .withMetricType(MetricType.IP)
                    .withTopK(5)
                    .withVectors(vectors)
                    .withExpr("dummy")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            SearchParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionNames(partitions)
                    .withParams("dummy")
                    .withOutFields(outputFields)
                    .withVectorFieldName("")
                    .withMetricType(MetricType.IP)
                    .withTopK(5)
                    .withVectors(vectors)
                    .withExpr("dummy")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            SearchParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionNames(partitions)
                    .withParams("dummy")
                    .withOutFields(outputFields)
                    .withVectorFieldName("")
                    .withMetricType(MetricType.INVALID)
                    .withTopK(5)
                    .withVectors(vectors)
                    .withExpr("dummy")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            SearchParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionNames(partitions)
                    .withParams("dummy")
                    .withOutFields(outputFields)
                    .withVectorFieldName("")
                    .withMetricType(MetricType.IP)
                    .withTopK(0)
                    .withVectors(vectors)
                    .withExpr("dummy")
                    .build();
        });

        // vector type illegal
        List<String> fakeVectors1 = Arrays.asList("fake");
        assertThrows(ParamException.class, () -> {
            SearchParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionNames(partitions)
                    .withParams("dummy")
                    .withOutFields(outputFields)
                    .withVectorFieldName("")
                    .withMetricType(MetricType.IP)
                    .withTopK(5)
                    .withVectors(fakeVectors1)
                    .withExpr("dummy")
                    .build();
        });

        List<List<String>> fakeVectors2 = Arrays.asList(fakeVectors1);
        assertThrows(ParamException.class, () -> {
            SearchParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionNames(partitions)
                    .withParams("dummy")
                    .withOutFields(outputFields)
                    .withVectorFieldName("")
                    .withMetricType(MetricType.IP)
                    .withTopK(5)
                    .withVectors(fakeVectors2)
                    .withExpr("dummy")
                    .build();
        });

        // vector dimension not equal
        List<Float> vector2 = Arrays.asList(0.1F, 0.2F);
        vectors.add(vector2);
        assertThrows(ParamException.class, () -> {
            SearchParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionNames(partitions)
                    .withParams("dummy")
                    .withOutFields(outputFields)
                    .withVectorFieldName("")
                    .withMetricType(MetricType.IP)
                    .withTopK(5)
                    .withVectors(vectors)
                    .withExpr("dummy")
                    .build();
        });

        ByteBuffer buf1 = ByteBuffer.allocate(1);
        buf1.put((byte) 1);
        ByteBuffer buf2 = ByteBuffer.allocate(2);
        buf2.put((byte) 1);
        buf2.put((byte) 2);
        List<ByteBuffer> bvectors = Arrays.asList(buf1, buf2);
        assertThrows(ParamException.class, () -> {
            SearchParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionNames(partitions)
                    .withParams("dummy")
                    .withOutFields(outputFields)
                    .withVectorFieldName("")
                    .withMetricType(MetricType.IP)
                    .withTopK(5)
                    .withVectors(bvectors)
                    .withExpr("dummy")
                    .build();
        });
    }

    @Test
    void search() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        List<String> partitions = Arrays.asList("partition1");
        List<String> outputFields = Arrays.asList("field2");
        List<List<Float>> vectors = new ArrayList<>();
        List<Float> vector1 = Arrays.asList(0.1F);
        vectors.add(vector1);

        R<SearchResults> resp = client.search(SearchParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("dummy")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(vectors)
                .withExpr("dummy")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.search(SearchParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withParams("dummy")
                .withOutFields(outputFields)
                .withVectorFieldName("field1")
                .withMetricType(MetricType.IP)
                .withTopK(5)
                .withVectors(vectors)
                .withExpr("dummy")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.search(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void queryParam() {
        // test throw exception with illegal input
        List<String> partitions = Arrays.asList("partition1");
        List<String> outputFields = Arrays.asList("field2");
        assertThrows(ParamException.class, () -> {
            QueryParam.Builder
                    .newBuilder()
                    .withCollectionName("")
                    .withPartitionNames(partitions)
                    .withOutFields(outputFields)
                    .withExpr("dummy")
                    .build();
        });

        assertThrows(ParamException.class, () -> {
            QueryParam.Builder
                    .newBuilder()
                    .withCollectionName("collection1")
                    .withPartitionNames(partitions)
                    .withOutFields(outputFields)
                    .withExpr("")
                    .build();
        });
    }

    @Test
    void query() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        List<String> partitions = Arrays.asList("partition1");
        List<String> outputFields = Arrays.asList("field2");

        R<QueryResults> resp = client.query(QueryParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withOutFields(outputFields)
                .withExpr("dummy")
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.query(QueryParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withPartitionNames(partitions)
                .withOutFields(outputFields)
                .withExpr("dummy")
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.query(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }

    @Test
    void calcDistanceParam() {
        // test throw exception with illegal input
        List<List<Float>> vectorsLeft = new ArrayList<>();
        List<List<Float>> vectorsRight = new ArrayList<>();
        List<Float> vector1 = Arrays.asList(0.1F);
        List<Float> vector2 = Arrays.asList(0.1F);
        vectorsLeft.add(vector1);
        vectorsRight.add(vector2);

        assertThrows(ParamException.class, () -> {
            CalcDistanceParam.Builder
                    .newBuilder()
                    .withVectorsLeft(vectorsLeft)
                    .withVectorsRight(vectorsRight)
                    .withMetricType(MetricType.INVALID)
                    .build();
        });

        vectorsLeft.clear();
        assertThrows(ParamException.class, () -> {
            CalcDistanceParam.Builder
                    .newBuilder()
                    .withVectorsLeft(vectorsLeft)
                    .withVectorsRight(vectorsRight)
                    .withMetricType(MetricType.IP)
                    .build();
        });

        vectorsLeft.add(vector1);
        vectorsRight.clear();
        assertThrows(ParamException.class, () -> {
            CalcDistanceParam.Builder
                    .newBuilder()
                    .withVectorsLeft(vectorsLeft)
                    .withVectorsRight(vectorsRight)
                    .withMetricType(MetricType.IP)
                    .build();
        });

        // vector dimension not equal
        vectorsRight.add(vector2);
        List<Float> vector3 = Arrays.asList(0.1F, 0.2F);
        vectorsLeft.add(vector3);
        assertThrows(ParamException.class, () -> {
            CalcDistanceParam.Builder
                    .newBuilder()
                    .withVectorsLeft(vectorsLeft)
                    .withVectorsRight(vectorsRight)
                    .withMetricType(MetricType.IP)
                    .build();
        });

        vectorsLeft.clear();
        vectorsLeft.add(vector1);
        List<Float> vector4 = Arrays.asList(0.1F, 0.2F);
        vectorsRight.add(vector4);
        assertThrows(ParamException.class, () -> {
            CalcDistanceParam.Builder
                    .newBuilder()
                    .withVectorsLeft(vectorsLeft)
                    .withVectorsRight(vectorsRight)
                    .withMetricType(MetricType.IP)
                    .build();
        });
    }

    @Test
    void calcDistance() {
        MilvusServiceClient client = startClient();

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        List<List<Float>> vectorsLeft = new ArrayList<>();
        List<List<Float>> vectorsRight = new ArrayList<>();
        List<Float> vector1 = Arrays.asList(0.1F);
        List<Float> vector2 = Arrays.asList(0.1F);
        vectorsLeft.add(vector1);
        vectorsRight.add(vector2);

        R<CalcDistanceResults> resp = client.calcDistance(CalcDistanceParam.Builder
                .newBuilder()
                .withVectorsLeft(vectorsLeft)
                .withVectorsRight(vectorsRight)
                .withMetricType(MetricType.L2)
                .build());
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.calcDistance(CalcDistanceParam.Builder
                .newBuilder()
                .withVectorsLeft(vectorsLeft)
                .withVectorsRight(vectorsRight)
                .withMetricType(MetricType.L2)
                .build());
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());

        // test return error when client channel is shutdown
        client.close();
        resp = client.calcDistance(null);
        assertEquals(R.Status.ClientNotConnected.getCode(), resp.getStatus());
    }
}