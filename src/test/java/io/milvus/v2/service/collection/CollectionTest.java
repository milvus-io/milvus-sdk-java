package io.milvus.v2.service.collection;

import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.v2.common.DataType;
import io.milvus.v2.BaseTest;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.common.IndexParam;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;

class CollectionTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(CollectionTest.class);

    @Test
    void testListCollections() {
        R<ListCollectionsResp> a = client_v2.listCollections();

        logger.info("resp: {}", a.getData());
        Assertions.assertEquals(R.Status.Success.getCode(), a.getStatus());
        Assertions.assertEquals("test", a.getData().getCollectionNames().get(0));
    }

    @Test
    void testCreateCollection() {
        CreateCollectionReq req = CreateCollectionReq.builder()
                .collectionName("test2")
                .dimension(2)
                .build();
        R<RpcStatus> resp = client_v2.createCollection(req);
        Assertions.assertEquals(R.Status.Success.getCode(), resp.getStatus());
    }

    @Test
    void testCreateCollectionWithSchema() {
        List<CreateCollectionWithSchemaReq.FieldSchema> fields = new ArrayList<>();
        CreateCollectionWithSchemaReq.FieldSchema idSchema = CreateCollectionWithSchemaReq.FieldSchema.builder()
                .name("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.FALSE)
                .build();
        CreateCollectionWithSchemaReq.FieldSchema metaSchema = CreateCollectionWithSchemaReq.FieldSchema.builder()
                .name("meta")
                .dataType(DataType.VarChar)
                .build();
        CreateCollectionWithSchemaReq.FieldSchema vectorSchema = CreateCollectionWithSchemaReq.FieldSchema.builder()
                .name("vector")
                .dataType(DataType.FloatVector)
                .dimension(2)
                .build();

        fields.add(idSchema);
        fields.add(vectorSchema);
        fields.add(metaSchema);

        CreateCollectionWithSchemaReq.CollectionSchema collectionSchema = CreateCollectionWithSchemaReq.CollectionSchema.builder()
                .fieldSchemaList(fields)
                .enableDynamicField(Boolean.TRUE)
                .build();

        IndexParam indexParam = IndexParam.builder()
                .fieldName("vector")
                .metricType(IndexParam.MetricType.L2)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .build();

        CreateCollectionWithSchemaReq request = CreateCollectionWithSchemaReq.builder()
                .collectionName("test")
                .collectionSchema(collectionSchema)
                .indexParams(Collections.singletonList(indexParam))
                .build();
        R<RpcStatus> resp = client_v2.createCollectionWithSchema(request);
        Assertions.assertEquals(R.Status.Success.getCode(), resp.getStatus());
    }

    @Test
    void testDropCollection() {
        DropCollectionReq req = DropCollectionReq.builder()
                .collectionName("test")
                .build();
        R<RpcStatus> resp = client_v2.dropCollection(req);
        Assertions.assertEquals(R.Status.Success.getCode(), resp.getStatus());
    }

    @Test
    void testHasCollection() {
        HasCollectionReq req = HasCollectionReq.builder()
                .collectionName("test")
                .build();
        R<Boolean> resp = client_v2.hasCollection(req);
        logger.info("resp: {}", resp.getData());
        Assertions.assertEquals(R.Status.Success.getCode(), resp.getStatus());
    }
    @Test
    void testDescribeCollection() {
        DescribeCollectionReq req = DescribeCollectionReq.builder()
                .collectionName("test2")
                .build();
        R<DescribeCollectionResp> resp = client_v2.describeCollection(req);
        logger.info("resp: {}", resp);
        Assertions.assertEquals(R.Status.Success.getCode(), resp.getStatus());
    }

    @Test
    void testRenameCollection() {
        RenameCollectionReq req = RenameCollectionReq.builder()
                .collectionName("test2")
                .newCollectionName("test")
                .build();
        R<RpcStatus> resp = client_v2.renameCollection(req);
        Assertions.assertEquals(R.Status.Success.getCode(), resp.getStatus());
    }

    @Test
    void testLoadCollection() {
        LoadCollectionReq req = LoadCollectionReq.builder()
                .collectionName("test")
                .build();
        R<RpcStatus> resp = client_v2.loadCollection(req);
        Assertions.assertEquals(R.Status.Success.getCode(), resp.getStatus());
    }

    @Test
    void testReleaseCollection() {
        ReleaseCollectionReq req = ReleaseCollectionReq.builder()
                .collectionName("test")
                .build();
        R<RpcStatus> resp = client_v2.releaseCollection(req);
        Assertions.assertEquals(R.Status.Success.getCode(), resp.getStatus());
    }

    @Test
    void testGetLoadState() {
        GetLoadStateReq req = GetLoadStateReq.builder()
                .collectionName("test")
                .build();
        R<Boolean> resp = client_v2.getLoadState(req);
        logger.info("resp: {}", resp.getData());
        Assertions.assertEquals(R.Status.Success.getCode(), resp.getStatus());
    }

//    @Test
//    void testGetCollectionStats() {
//        GetCollectionStatsReq req = GetCollectionStatsReq.builder()
//                .collectionName("test")
//                .build();
//        R<GetCollectionStatsResp> resp = clientv_2.getCollectionStats(req);
//        logger.info("resp: {}", resp);
//        Assertions.assertEquals(R.Status.Success.getCode(), resp.getStatus());
//    }
}