package io.milvus.client;

import io.milvus.grpc.FlushResponse;
import io.milvus.grpc.DataType;

import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;

import io.milvus.server.MockMilvusServer;
import io.milvus.server.MockMilvusServerImpl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MilvusServiceClientTest {
    private String testHost = "localhost";
    private int testPort = 53019;

    private MockMilvusServer mockServer;

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
    void createCollection() {
        MilvusServiceClient client = startClient();

        FieldType[] fieldTypes = new FieldType[1];
        FieldType fieldType1 = FieldType.Builder.newBuilder()
                .withName("userID")
                .withDescription("userId")
                .withDataType(DataType.Int64)
                .withAutoID(true)
                .withPrimaryKey(true).build();
        fieldTypes[0] = fieldType1;

        // test return error with illegal input
        R<RpcStatus> resp = client.createCollection(CreateCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withShardsNum(2)
                .build());
        assertEquals(R.Status.ParamError.getCode(), resp.getStatus());

        resp = client.createCollection(CreateCollectionParam.Builder
                .newBuilder()
                .withCollectionName("")
                .withShardsNum(2)
                .withFieldTypes(fieldTypes)
                .build());
        assertEquals(R.Status.ParamError.getCode(), resp.getStatus());

        resp = client.createCollection(CreateCollectionParam.Builder
                .newBuilder()
                .withCollectionName("collection1")
                .withShardsNum(0)
                .withFieldTypes(fieldTypes)
                .build());
        assertEquals(R.Status.ParamError.getCode(), resp.getStatus());

        // start mock server
        MockMilvusServer server = startServer();

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
    }

    @Test
    void hasCollection() {
        MilvusServiceClient client = startClient();

        // test return error with illegal input
        R<Boolean> resp = client.hasCollection(HasCollectionParam.Builder
                .newBuilder()
                .withCollectionName("")
                .build());
        assertEquals(R.Status.ParamError.getCode(), resp.getStatus());

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        resp = client.hasCollection(HasCollectionParam.Builder
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
    }

    @Test
    void flush() {
        MilvusServiceClient client = startClient();

        // test return error with illegal input
        R<FlushResponse> resp = client.flush("");
        assertEquals(R.Status.ParamError.getCode(), resp.getStatus());

        // start mock server
        MockMilvusServer server = startServer();

        // test return ok with correct input
        resp = client.flush("collection1");
        assertEquals(R.Status.Success.getCode(), resp.getStatus());

        // stop mock server
        server.stop();

        // test return error without server
        resp = client.flush("collection1");
        assertNotEquals(R.Status.Success.getCode(), resp.getStatus());
    }
}