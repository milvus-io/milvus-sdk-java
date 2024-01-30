package io.milvus.v2.service.collection;

import io.milvus.v2.BaseTest;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

class CollectionTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(CollectionTest.class);

    @Test
    void testListCollections() {
        ListCollectionsResp a = client_v2.listCollections();
    }

    @Test
    void testCreateCollection() {
        CreateCollectionReq req = CreateCollectionReq.builder()
                .collectionName("test2")
                .dimension(2)
                .build();
        client_v2.createCollection(req);
    }

    @Test
    void testCreateCollectionWithSchema() {
//        List<CreateCollectionWithSchemaReq.FieldSchema> fields = new ArrayList<>();
//        CreateCollectionWithSchemaReq.FieldSchema idSchema = CreateCollectionWithSchemaReq.FieldSchema.builder()
//                .name("id")
//                .dataType(DataType.Int64)
//                .isPrimaryKey(Boolean.TRUE)
//                .autoID(Boolean.FALSE)
//                .build();
//        CreateCollectionWithSchemaReq.FieldSchema metaSchema = CreateCollectionWithSchemaReq.FieldSchema.builder()
//                .name("meta")
//                .dataType(DataType.VarChar)
//                .build();
//        CreateCollectionWithSchemaReq.FieldSchema vectorSchema = CreateCollectionWithSchemaReq.FieldSchema.builder()
//                .name("vector")
//                .dataType(DataType.FloatVector)
//                .dimension(2)
//                .build();
//
//        fields.add(idSchema);
//        fields.add(vectorSchema);
//        fields.add(metaSchema);

        CreateCollectionWithSchemaReq.CollectionSchema collectionSchema = CreateCollectionWithSchemaReq.CollectionSchema.builder()
                .enableDynamicField(Boolean.TRUE)
                .build();
        collectionSchema.addPrimaryField("id", DataType.Int64, null, Boolean.TRUE, Boolean.FALSE);
        collectionSchema.addVectorField("vector", DataType.FloatVector,8);
        collectionSchema.addScalarField("meta", DataType.VarChar, 100);
        collectionSchema.addScalarField("age", DataType.Int64);

        IndexParam indexParam = IndexParam.builder()
                .fieldName("vector")
                .metricType(IndexParam.MetricType.L2)
                .build();

        CreateCollectionWithSchemaReq request = CreateCollectionWithSchemaReq.builder()
                .collectionName("test")
                .collectionSchema(collectionSchema)
                .indexParams(Collections.singletonList(indexParam))
                .build();
        client_v2.createCollectionWithSchema(request);
    }

    @Test
    void testDropCollection() {
        DropCollectionReq req = DropCollectionReq.builder()
                .collectionName("test")
                .build();
        client_v2.dropCollection(req);
    }

    @Test
    void testHasCollection() {
        HasCollectionReq req = HasCollectionReq.builder()
                .collectionName("test")
                .build();
        Boolean resp = client_v2.hasCollection(req);
    }
    @Test
    void testDescribeCollection() {
        DescribeCollectionReq req = DescribeCollectionReq.builder()
                .collectionName("test2")
                .build();
        DescribeCollectionResp resp = client_v2.describeCollection(req);
        logger.info("resp: {}", resp);
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
    }
}