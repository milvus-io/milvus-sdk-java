package com.zilliz.milvustestv2.datatype;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test cases for Geometry data type (Milvus 2.6 new data type)
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
public class GeometryTest extends BaseTest {

    private String geometryCollectionName;
    private static final String fieldGeometry = "fieldGeometry";
    private static final int DIM = CommonData.dim;
    private static final int INSERT_COUNT = 1000;
    // every 10th row is exactly "POINT (1 1)"
    private static final int EXACT_POINT_COUNT = INSERT_COUNT / 10;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        geometryCollectionName = "Geometry_" + GenerateUtil.getRandomString(6);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (geometryCollectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(geometryCollectionName)
                    .build());
        }
    }

    private static void createGeometryCollection(String collectionName) {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(fieldGeometry)
                .dataType(DataType.Geometry)
                .build());
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());
    }

    private static String buildWkt(int i) {
        if (i % 10 == 0) {
            return "POINT (1 1)";
        } else if (i % 3 == 0) {
            return "POINT (" + i + " " + i + ")";
        } else if (i % 3 == 1) {
            return "LINESTRING (" + i + " " + i + ", " + (i + 10) + " " + (i + 10) + ", " + (i + 20) + " " + (i + 20) + ")";
        } else {
            return "POLYGON ((" + i + " " + i + ", " + (i + 10) + " " + i + ", "
                    + (i + 10) + " " + (i + 10) + ", " + i + " " + (i + 10) + ", " + i + " " + i + "))";
        }
    }

    private static List<JsonObject> generateGeometryData(int count) {
        List<JsonObject> dataList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            JsonArray floatVector = new JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                floatVector.add(v);
            }
            row.add(CommonData.fieldFloatVector, floatVector);
            row.addProperty(fieldGeometry, buildWkt(i));
            dataList.add(row);
        }
        return dataList;
    }

    // ==================== Positive Tests ====================

    @Test(description = "Create collection with Geometry field and verify schema", groups = {"Smoke"})
    public void createGeometryCollectionSuccess() {
        createGeometryCollection(geometryCollectionName);

        DescribeCollectionResp descResp = milvusClientV2.describeCollection(
                DescribeCollectionReq.builder().collectionName(geometryCollectionName).build());
        Assert.assertNotNull(descResp.getCollectionSchema());
        boolean fieldFound = false;
        for (CreateCollectionReq.FieldSchema fieldSchema : descResp.getCollectionSchema().getFieldSchemaList()) {
            if (fieldGeometry.equals(fieldSchema.getName())) {
                Assert.assertEquals(fieldSchema.getDataType(), DataType.Geometry,
                        "Field type should be Geometry");
                fieldFound = true;
            }
        }
        Assert.assertTrue(fieldFound, "Geometry field should exist in collection schema");
    }

    @Test(description = "Insert 1000 rows with mixed POINT/LINESTRING/POLYGON WKT strings", groups = {"Smoke"}, dependsOnMethods = {"createGeometryCollectionSuccess"})
    public void insertGeometryDataSuccess() {
        List<JsonObject> data = generateGeometryData(INSERT_COUNT);
        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(geometryCollectionName)
                .data(data)
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), INSERT_COUNT,
                "Insert count should match");
    }

    @Test(description = "Create RTREE index on geometry field, HNSW on vector and load", groups = {"Smoke"}, dependsOnMethods = {"insertGeometryDataSuccess"})
    public void createIndexAndLoadSuccess() {
        IndexParam geometryIndexParam = IndexParam.builder()
                .fieldName(fieldGeometry)
                .indexType(IndexParam.IndexType.RTREE)
                .build();
        IndexParam vectorIndexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(geometryCollectionName)
                .indexParams(Arrays.asList(geometryIndexParam, vectorIndexParam))
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(geometryCollectionName)
                .build());
    }

    @Test(description = "Query geometry field and verify returned value is non-empty WKT String", groups = {"Smoke"}, dependsOnMethods = {"createIndexAndLoadSuccess"})
    public void queryGeometryReadbackSuccess() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(geometryCollectionName)
                .filter(CommonData.fieldInt64 + " < 10")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldGeometry))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(queryResp);
        Assert.assertEquals(queryResp.getQueryResults().size(), 10);
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Object geometryValue = result.getEntity().get(fieldGeometry);
            Assert.assertTrue(geometryValue instanceof String,
                    "Geometry value should be returned as String");
            Assert.assertFalse(((String) geometryValue).isEmpty(),
                    "Geometry WKT value should not be empty");
        }
    }

    @Test(description = "Query with ST_EQUALS filter should hit exact points", groups = {"Smoke"}, dependsOnMethods = {"createIndexAndLoadSuccess"})
    public void queryWithStEqualsSuccess() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(geometryCollectionName)
                .filter("ST_EQUALS(" + fieldGeometry + ", 'POINT (1 1)')")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldGeometry))
                .limit(INSERT_COUNT)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(queryResp);
        Assert.assertEquals(queryResp.getQueryResults().size(), EXACT_POINT_COUNT,
                "ST_EQUALS should hit exactly " + EXACT_POINT_COUNT + " rows of POINT (1 1)");
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Assert.assertTrue(result.getEntity().get(fieldGeometry) instanceof String);
        }
    }

    @Test(description = "Query with ST_CONTAINS filter", groups = {"Smoke"}, dependsOnMethods = {"createIndexAndLoadSuccess"})
    public void queryWithStContainsSuccess() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(geometryCollectionName)
                .filter("ST_CONTAINS(" + fieldGeometry + ", 'POINT (500 500)')")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldGeometry))
                .limit(INSERT_COUNT)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(queryResp);
        Assert.assertFalse(queryResp.getQueryResults().isEmpty(),
                "ST_CONTAINS should hit polygons containing POINT (500 500)");
    }

    @Test(description = "Query with ST_WITHIN filter", groups = {"Smoke"}, dependsOnMethods = {"createIndexAndLoadSuccess"})
    public void queryWithStWithinSuccess() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(geometryCollectionName)
                .filter("ST_WITHIN(" + fieldGeometry + ", 'POLYGON ((-100 -100, 2000 -100, 2000 2000, -100 2000, -100 -100))')")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldGeometry))
                .limit(INSERT_COUNT)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(queryResp);
        Assert.assertFalse(queryResp.getQueryResults().isEmpty(),
                "ST_WITHIN should hit geometries inside the big polygon");
    }

    @Test(description = "Query with ST_TOUCHES filter", groups = {"Smoke"}, dependsOnMethods = {"createIndexAndLoadSuccess"})
    public void queryWithStTouchesSuccess() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(geometryCollectionName)
                .filter("ST_TOUCHES(" + fieldGeometry + ", 'POINT (10 10)')")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldGeometry))
                .limit(INSERT_COUNT)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(queryResp);
        Assert.assertNotNull(queryResp.getQueryResults(),
                "ST_TOUCHES query should return normally");
    }

    @Test(description = "Query with ST_CROSSES filter", groups = {"Smoke"}, dependsOnMethods = {"createIndexAndLoadSuccess"})
    public void queryWithStCrossesSuccess() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(geometryCollectionName)
                .filter("ST_CROSSES(" + fieldGeometry + ", 'LINESTRING (0 500, 1000 500)')")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldGeometry))
                .limit(INSERT_COUNT)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(queryResp);
        Assert.assertNotNull(queryResp.getQueryResults(),
                "ST_CROSSES query should return normally");
    }

    @Test(description = "Vector search combined with geometry filter", groups = {"Smoke"}, dependsOnMethods = {"createIndexAndLoadSuccess"})
    public void searchWithGeometryFilterSuccess() {
        List<Float> queryVector = GenerateUtil.generateFloatVector(1, 6, DIM).get(0);
        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(geometryCollectionName)
                .annsField(CommonData.fieldFloatVector)
                .data(Collections.singletonList(new FloatVec(queryVector)))
                .topK(10)
                .filter("ST_WITHIN(" + fieldGeometry + ", 'POLYGON ((-100 -100, 2000 -100, 2000 2000, -100 2000, -100 -100))')")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldGeometry))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        Assert.assertFalse(searchResp.getSearchResults().get(0).isEmpty(),
                "Search with geometry filter should return results");
    }

    // ==================== Negative Tests ====================

    @Test(description = "Insert invalid WKT string should fail", groups = {"Smoke"}, dependsOnMethods = {"createGeometryCollectionSuccess"})
    public void insertInvalidWktShouldFail() {
        List<JsonObject> dataList = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 100001L);
        JsonArray floatVector = new JsonArray();
        for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
            floatVector.add(v);
        }
        row.add(CommonData.fieldFloatVector, floatVector);
        row.addProperty(fieldGeometry, "POINT (abc def)");
        dataList.add(row);

        try {
            milvusClientV2.insert(InsertReq.builder()
                    .collectionName(geometryCollectionName)
                    .data(dataList)
                    .build());
            Assert.fail("Should throw exception when inserting invalid WKT string");
        } catch (Exception e) {
            // Expected exception
        }
    }

    @Test(description = "Create HNSW index on geometry field should fail", groups = {"Smoke"})
    public void createHnswOnGeometryShouldFail() {
        String collectionName = "GeometryHnsw_" + GenerateUtil.getRandomString(6);
        createGeometryCollection(collectionName);
        try {
            IndexParam indexParam = IndexParam.builder()
                    .fieldName(fieldGeometry)
                    .indexType(IndexParam.IndexType.HNSW)
                    .metricType(IndexParam.MetricType.L2)
                    .build();
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(collectionName)
                    .indexParams(Collections.singletonList(indexParam))
                    .build());
            Assert.fail("Should throw exception when creating HNSW index on geometry field");
        } catch (Exception e) {
            // Expected exception
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    @Test(description = "Create RTREE index on float vector field should fail", groups = {"Smoke"})
    public void createRtreeOnFloatVectorShouldFail() {
        String collectionName = "VectorRtree_" + GenerateUtil.getRandomString(6);
        createGeometryCollection(collectionName);
        try {
            IndexParam indexParam = IndexParam.builder()
                    .fieldName(CommonData.fieldFloatVector)
                    .indexType(IndexParam.IndexType.RTREE)
                    .build();
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(collectionName)
                    .indexParams(Collections.singletonList(indexParam))
                    .build());
            Assert.fail("Should throw exception when creating RTREE index on float vector field");
        } catch (Exception e) {
            // Expected exception
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    @Test(description = "Query with ST_WITHIN invalid WKT argument should fail", groups = {"Smoke"}, dependsOnMethods = {"createIndexAndLoadSuccess"})
    public void queryStWithinInvalidWktShouldFail() {
        try {
            milvusClientV2.query(QueryReq.builder()
                    .collectionName(geometryCollectionName)
                    .filter("ST_WITHIN(" + fieldGeometry + ", 'NOT A WKT')")
                    .outputFields(Arrays.asList(CommonData.fieldInt64, fieldGeometry))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            Assert.fail("Should throw exception when ST_WITHIN gets invalid WKT argument");
        } catch (Exception e) {
            // Expected exception
        }
    }
}
