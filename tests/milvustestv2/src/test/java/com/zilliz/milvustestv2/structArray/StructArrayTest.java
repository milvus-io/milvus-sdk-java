package com.zilliz.milvustestv2.structArray;

import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
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
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.EmbeddingList;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

/**
 * Test cases for Struct Array feature including Array of Vector
 *
 * @Author yongpeng.li
 * @Date 2024
 */
public class StructArrayTest extends BaseTest {

    private String structCollectionName;
    private static final int DIM = CommonData.structVectorDim;
    private static final int INSERT_COUNT = 1000;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        structCollectionName = "StructArrayTest_" + GenerateUtil.getRandomString(6);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (structCollectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(structCollectionName)
                    .build());
        }
    }

    // ==================== Create Collection Tests ====================

    @Test(description = "Create collection with struct array field containing vectors", groups = {"Smoke"})
    public void createStructCollectionSuccess() {
        // Create collection with struct field
        String collectionName = CommonFunction.createStructCollection(structCollectionName, DIM);

        // Verify collection exists
        ListCollectionsResp listResp = milvusClientV2.listCollections();
        Assert.assertTrue(listResp.getCollectionNames().contains(collectionName),
                "Collection should be created successfully");

        // Verify collection schema
        DescribeCollectionResp descResp = milvusClientV2.describeCollection(
                DescribeCollectionReq.builder().collectionName(collectionName).build());
        Assert.assertNotNull(descResp.getCollectionSchema());
    }

    @Test(description = "Create collection with multiple struct array fields", groups = {"Smoke"})
    public void createCollectionWithMultipleStructFields() {
        String collectionName = "MultiStructCollection_" + GenerateUtil.getRandomString(6);

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();

        // Primary key
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());

        // First struct field
        schema.addField(AddFieldReq.builder()
                .fieldName("clips")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(100)
                .addStructField(AddFieldReq.builder()
                        .fieldName("clip_vector")
                        .dataType(DataType.FloatVector)
                        .dimension(DIM)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("clip_desc")
                        .dataType(DataType.VarChar)
                        .maxLength(512)
                        .build())
                .build());

        // Second struct field (simplified version)
        schema.addField(AddFieldReq.builder()
                .fieldName("simplify_clips")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(50)
                .addStructField(AddFieldReq.builder()
                        .fieldName("simple_vector")
                        .dataType(DataType.FloatVector)
                        .dimension(32)
                        .build())
                .build());

        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        // Verify
        ListCollectionsResp listResp = milvusClientV2.listCollections();
        Assert.assertTrue(listResp.getCollectionNames().contains(collectionName));

        // Cleanup
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }

    @Test(description = "Create struct field with scalar types only (no vector)", groups = {"Smoke"})
    public void createStructWithScalarOnly() {
        String collectionName = "ScalarStructCollection_" + GenerateUtil.getRandomString(6);

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());

        // Struct with only scalar fields
        schema.addField(AddFieldReq.builder()
                .fieldName("metadata")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(20)
                .addStructField(AddFieldReq.builder()
                        .fieldName("key")
                        .dataType(DataType.VarChar)
                        .maxLength(256)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("value")
                        .dataType(DataType.Int64)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("score")
                        .dataType(DataType.Float)
                        .build())
                .build());

        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        ListCollectionsResp listResp = milvusClientV2.listCollections();
        Assert.assertTrue(listResp.getCollectionNames().contains(collectionName));

        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }

    // ==================== Insert Data Tests ====================

    @Test(description = "Insert data into struct collection", groups = {"Smoke"}, dependsOnMethods = {"createStructCollectionSuccess"})
    public void insertStructDataSuccess() {
        List<JsonObject> data = CommonFunction.generateStructData(0, INSERT_COUNT, DIM);

        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(structCollectionName)
                .data(data)
                .build());

        Assert.assertEquals(insertResp.getInsertCnt(), INSERT_COUNT,
                "Insert count should match");
    }

    @Test(description = "Insert struct data with varying array lengths", groups = {"Smoke"}, dependsOnMethods = {"createStructCollectionSuccess"})
    public void insertStructWithVaryingArrayLength() {
        String collectionName = "VaryingStructCollection_" + GenerateUtil.getRandomString(6);
        CommonFunction.createStructCollection(collectionName, DIM);

        List<JsonObject> dataList = new ArrayList<>();
        Random random = new Random();

        // Insert rows with different struct array lengths (1 to 10)
        for (int i = 0; i < 100; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, 10000 + i);

            // Regular vector
            com.google.gson.JsonArray floatVector = new com.google.gson.JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                floatVector.add(v);
            }
            row.add(CommonData.fieldFloatVector, floatVector);

            // Varying length struct array
            int structCount = random.nextInt(10) + 1;
            com.google.gson.JsonArray structArray = new com.google.gson.JsonArray();
            for (int j = 0; j < structCount; j++) {
                JsonObject struct = new JsonObject();
                struct.addProperty(CommonData.structFieldInt32, j);
                struct.addProperty(CommonData.structFieldVarchar, "item_" + j);

                com.google.gson.JsonArray vec1 = new com.google.gson.JsonArray();
                for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                    vec1.add(v);
                }
                struct.add(CommonData.structFieldFloatVector1, vec1);

                com.google.gson.JsonArray vec2 = new com.google.gson.JsonArray();
                for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                    vec2.add(v);
                }
                struct.add(CommonData.structFieldFloatVector2, vec2);

                structArray.add(struct);
            }
            row.add(CommonData.fieldStruct, structArray);
            dataList.add(row);
        }

        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(dataList)
                .build());

        Assert.assertEquals(insertResp.getInsertCnt(), 100);

        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }

    // ==================== Index Tests ====================

    @DataProvider(name = "MetricTypeProvider")
    public Object[][] provideMetricTypes() {
        return new Object[][]{
                {IndexParam.MetricType.MAX_SIM_COSINE},
                {IndexParam.MetricType.MAX_SIM_IP},
                {IndexParam.MetricType.MAX_SIM_L2}
        };
    }

    @Test(description = "Create HNSW index on struct vector field", groups = {"Smoke"}, dependsOnMethods = {"insertStructDataSuccess"})
    public void createStructVectorIndexSuccess() {
        // Create index on first struct vector field
        CommonFunction.createStructVectorIndex(structCollectionName,
                CommonData.fieldStruct,
                CommonData.structFieldFloatVector1,
                "struct_vector_idx_1",
                IndexParam.MetricType.MAX_SIM_COSINE);

        // Create index on second struct vector field
        CommonFunction.createStructVectorIndex(structCollectionName,
                CommonData.fieldStruct,
                CommonData.structFieldFloatVector2,
                "struct_vector_idx_2",
                IndexParam.MetricType.MAX_SIM_IP);

        // Create index on regular float vector
        CommonFunction.createVectorIndex(structCollectionName,
                CommonData.fieldFloatVector,
                IndexParam.IndexType.HNSW,
                IndexParam.MetricType.L2);

        // Load collection
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(structCollectionName)
                .build());
    }

    @Test(description = "Create struct vector index with different metric types", groups = {"Smoke"}, dataProvider = "MetricTypeProvider")
    public void createStructIndexWithDifferentMetricTypes(IndexParam.MetricType metricType) {
        String collectionName = "MetricTypeTest_" + metricType.name() + "_" + GenerateUtil.getRandomString(4);
        CommonFunction.createStructCollection(collectionName, DIM);

        // Insert some data
        List<JsonObject> data = CommonFunction.generateStructData(0, 100, DIM);
        milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(data)
                .build());

        // Create index with specified metric type
        String fieldPath = String.format("%s[%s]", CommonData.fieldStruct, CommonData.structFieldFloatVector1);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(fieldPath)
                .indexName("idx_" + metricType.name())
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(metricType)
                .extraParams(new HashMap<String, Object>() {{
                    put("M", 16);
                    put("efConstruction", 200);
                }})
                .build();

        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());

        // Verify index created
        DescribeIndexResp describeIndexResp = milvusClientV2.describeIndex(
                DescribeIndexReq.builder()
                        .collectionName(collectionName)
                        .fieldName(fieldPath)
                        .build());
        Assert.assertNotNull(describeIndexResp);

        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }

    // ==================== Search Tests ====================

    @Test(description = "Search on struct vector field using EmbeddingList", groups = {"Smoke"}, dependsOnMethods = {"createStructVectorIndexSuccess"})
    public void searchWithEmbeddingListSuccess() {
        // Generate EmbeddingList with multiple vectors
        EmbeddingList embeddingList = CommonFunction.generateRandomEmbeddingList(5, DIM);

        String annsField = String.format("%s[%s]", CommonData.fieldStruct, CommonData.structFieldFloatVector1);

        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(structCollectionName)
                .annsField(annsField)
                .data(Collections.singletonList(embeddingList))
                .topK(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Arrays.asList(CommonData.fieldInt64,
                        String.format("%s[%s]", CommonData.fieldStruct, CommonData.structFieldVarchar)))
                .build());

        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= 10);
    }

    @Test(description = "Search with multiple EmbeddingLists (batch search)", groups = {"Smoke"}, dependsOnMethods = {"createStructVectorIndexSuccess"})
    public void batchSearchWithEmbeddingList() {
        // Create multiple EmbeddingLists
        List<BaseVector> searchData = new ArrayList<>();
        searchData.add(CommonFunction.generateRandomEmbeddingList(3, DIM));
        searchData.add(CommonFunction.generateRandomEmbeddingList(5, DIM));
        searchData.add(CommonFunction.generateRandomEmbeddingList(2, DIM));

        String annsField = String.format("%s[%s]", CommonData.fieldStruct, CommonData.structFieldFloatVector1);

        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(structCollectionName)
                .annsField(annsField)
                .data(searchData)
                .topK(5)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertNotNull(searchResp);
        Assert.assertEquals(searchResp.getSearchResults().size(), 3,
                "Should return results for all 3 embedding lists");
    }

    @Test(description = "Search on second struct vector field", groups = {"Smoke"}, dependsOnMethods = {"createStructVectorIndexSuccess"})
    public void searchOnSecondStructVectorField() {
        EmbeddingList embeddingList = CommonFunction.generateRandomEmbeddingList(4, DIM);

        String annsField = String.format("%s[%s]", CommonData.fieldStruct, CommonData.structFieldFloatVector2);

        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(structCollectionName)
                .annsField(annsField)
                .data(Collections.singletonList(embeddingList))
                .topK(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
    }

    @Test(description = "Search on regular vector field (non-struct)", groups = {"Smoke"}, dependsOnMethods = {"createStructVectorIndexSuccess"})
    public void searchOnRegularVectorField() {
        List<Float> queryVector = GenerateUtil.generateFloatVector(1, 6, DIM).get(0);

        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(structCollectionName)
                .annsField(CommonData.fieldFloatVector)
                .data(Collections.singletonList(new FloatVec(queryVector)))
                .topK(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Collections.singletonList(CommonData.fieldStruct))
                .build());

        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
    }

    @Test(description = "Search with filter expression", groups = {"Smoke"}, dependsOnMethods = {"createStructVectorIndexSuccess"})
    public void searchWithFilterExpression() {
        EmbeddingList embeddingList = CommonFunction.generateRandomEmbeddingList(3, DIM);

        String annsField = String.format("%s[%s]", CommonData.fieldStruct, CommonData.structFieldFloatVector1);

        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(structCollectionName)
                .annsField(annsField)
                .data(Collections.singletonList(embeddingList))
                .limit(10)
                .filter(CommonData.fieldInt64 + " < 500")
                .outputFields(Collections.singletonList(CommonData.fieldInt64))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        // Verify all results satisfy the filter
        if (!searchResp.getSearchResults().get(0).isEmpty()) {
            for (SearchResp.SearchResult result : searchResp.getSearchResults().get(0)) {
                Object idObj = result.getEntity().get(CommonData.fieldInt64);
                if (idObj != null) {
                    Long id = (Long) idObj;
                    Assert.assertTrue(id < 500, "Result should satisfy filter condition");
                }
            }
        }
    }

    // ==================== Query Tests ====================

    @Test(description = "Query struct collection by ID", groups = {"Smoke"}, dependsOnMethods = {"createStructVectorIndexSuccess"})
    public void queryStructByIdSuccess() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(structCollectionName)
                .filter(CommonData.fieldInt64 + " in [1, 5, 10]")
                .outputFields(Arrays.asList(CommonData.fieldInt64, CommonData.fieldStruct))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertNotNull(queryResp);
        Assert.assertEquals(queryResp.getQueryResults().size(), 3);

        // Verify struct field is returned
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Assert.assertTrue(result.getEntity().containsKey(CommonData.fieldStruct));
            Object structData = result.getEntity().get(CommonData.fieldStruct);
            Assert.assertTrue(structData instanceof List);
        }
    }

    @Test(description = "Query specific struct sub-fields", groups = {"Smoke"}, dependsOnMethods = {"createStructVectorIndexSuccess"})
    public void querySpecificStructSubFields() {
        String structVarcharField = String.format("%s[%s]", CommonData.fieldStruct, CommonData.structFieldVarchar);
        String structInt32Field = String.format("%s[%s]", CommonData.fieldStruct, CommonData.structFieldInt32);

        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(structCollectionName)
                .filter(CommonData.fieldInt64 + " < 10")
                .outputFields(Arrays.asList(CommonData.fieldInt64, structVarcharField, structInt32Field))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertNotNull(queryResp);
        Assert.assertFalse(queryResp.getQueryResults().isEmpty());
    }

    @Test(description = "Query and use result for EmbeddingList search", groups = {"Smoke"}, dependsOnMethods = {"createStructVectorIndexSuccess"})
    public void queryAndSearchWithEmbeddingList() {
        // First query to get struct data
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(structCollectionName)
                .filter(CommonData.fieldInt64 + " == 5")
                .outputFields(Collections.singletonList(CommonData.fieldStruct))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().size(), 1);

        // Extract struct data and create EmbeddingList
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> structData = (List<Map<String, Object>>)
                queryResp.getQueryResults().get(0).getEntity().get(CommonData.fieldStruct);

        EmbeddingList embeddingList = CommonFunction.generateEmbeddingListFromStruct(
                structData, CommonData.structFieldFloatVector1);

        // Use EmbeddingList for search
        String annsField = String.format("%s[%s]", CommonData.fieldStruct, CommonData.structFieldFloatVector1);
        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(structCollectionName)
                .annsField(annsField)
                .data(Collections.singletonList(embeddingList))
                .topK(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
    }

    @Test(description = "Query with count(*)", groups = {"Smoke"}, dependsOnMethods = {"createStructVectorIndexSuccess"})
    public void queryCountSuccess() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(structCollectionName)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertNotNull(queryResp);
        Assert.assertFalse(queryResp.getQueryResults().isEmpty());

        Long count = (Long) queryResp.getQueryResults().get(0).getEntity().get("count(*)");
        Assert.assertTrue(count >= INSERT_COUNT, "Count should be at least " + INSERT_COUNT);
    }

    // ==================== Error Cases Tests ====================

    @Test(description = "Create struct with unsupported element type - Struct in Struct", groups = {"Smoke"}, expectedExceptions = Exception.class)
    public void createNestedStructShouldFail() {
        String collectionName = "NestedStructTest_" + GenerateUtil.getRandomString(6);

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());

        // Try to create nested struct (should fail according to doc)
        schema.addField(AddFieldReq.builder()
                .fieldName("outer_struct")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(10)
                .addStructField(AddFieldReq.builder()
                        .fieldName("inner_struct")
                        .dataType(DataType.Array)
                        .elementType(DataType.Struct)
                        .maxCapacity(5)
                        .build())
                .build());

        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());
    }

    @Test(description = "Create struct with Array element type should fail", groups = {"Smoke"}, expectedExceptions = Exception.class)
    public void createStructWithArrayElementShouldFail() {
        String collectionName = "ArrayInStructTest_" + GenerateUtil.getRandomString(6);

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());

        // Try to add Array type in struct (should fail)
        schema.addField(AddFieldReq.builder()
                .fieldName("struct_with_array")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(10)
                .addStructField(AddFieldReq.builder()
                        .fieldName("nested_array")
                        .dataType(DataType.Array)
                        .elementType(DataType.Int64)
                        .maxCapacity(10)
                        .build())
                .build());

        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());
    }

    @Test(description = "Create struct with JSON element type should fail", groups = {"Smoke"}, expectedExceptions = Exception.class)
    public void createStructWithJsonElementShouldFail() {
        String collectionName = "JsonInStructTest_" + GenerateUtil.getRandomString(6);

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());

        // Try to add JSON type in struct (should fail)
        schema.addField(AddFieldReq.builder()
                .fieldName("struct_with_json")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(10)
                .addStructField(AddFieldReq.builder()
                        .fieldName("json_field")
                        .dataType(DataType.JSON)
                        .build())
                .build());

        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());
    }

    @Test(description = "Search with empty EmbeddingList should fail", groups = {"Smoke"}, dependsOnMethods = {"createStructVectorIndexSuccess"}, expectedExceptions = Exception.class)
    public void searchWithEmptyEmbeddingListShouldFail() {
        EmbeddingList emptyList = new EmbeddingList();

        String annsField = String.format("%s[%s]", CommonData.fieldStruct, CommonData.structFieldFloatVector1);

        milvusClientV2.search(SearchReq.builder()
                .collectionName(structCollectionName)
                .annsField(annsField)
                .data(Collections.singletonList(emptyList))
                .topK(10)
                .build());
    }

    @Test(description = "Search with wrong vector dimension in EmbeddingList should fail", groups = {"Smoke"}, dependsOnMethods = {"createStructVectorIndexSuccess"}, expectedExceptions = Exception.class)
    public void searchWithWrongDimensionShouldFail() {
        // Create EmbeddingList with wrong dimension
        EmbeddingList wrongDimList = CommonFunction.generateRandomEmbeddingList(3, DIM + 10);

        String annsField = String.format("%s[%s]", CommonData.fieldStruct, CommonData.structFieldFloatVector1);

        milvusClientV2.search(SearchReq.builder()
                .collectionName(structCollectionName)
                .annsField(annsField)
                .data(Collections.singletonList(wrongDimList))
                .topK(10)
                .build());
    }

    @Test(description = "Use regular L2 metric type for struct vector should fail", groups = {"Smoke"})
    public void createIndexWithRegularMetricShouldFail() {
        String collectionName = "RegularMetricTest_" + GenerateUtil.getRandomString(6);
        CommonFunction.createStructCollection(collectionName, DIM);

        List<JsonObject> data = CommonFunction.generateStructData(0, 10, DIM);
        milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(data)
                .build());

        String fieldPath = String.format("%s[%s]", CommonData.fieldStruct, CommonData.structFieldFloatVector1);

        try {
            // Try to create index with regular L2 metric (should fail for struct vector)
            IndexParam indexParam = IndexParam.builder()
                    .fieldName(fieldPath)
                    .indexType(IndexParam.IndexType.HNSW)
                    .metricType(IndexParam.MetricType.L2)  // Regular L2, not MAX_SIM_L2
                    .build();

            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(collectionName)
                    .indexParams(Collections.singletonList(indexParam))
                    .build());

            // If no exception, the test should still clean up
            Assert.fail("Should throw exception for using regular L2 metric on struct vector");
        } catch (Exception e) {
            // Expected exception
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    // ==================== Boundary Value Tests (Scenario 5) ====================

    @Test(description = "Create struct with maxCapacity = 1", groups = {"Smoke"})
    public void createStructWithMinCapacity() {
        String collectionName = "MinCapacityStruct_" + GenerateUtil.getRandomString(6);

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());

        // Struct with maxCapacity = 1
        schema.addField(AddFieldReq.builder()
                .fieldName("single_struct")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(1)
                .addStructField(AddFieldReq.builder()
                        .fieldName("vec")
                        .dataType(DataType.FloatVector)
                        .dimension(DIM)
                        .build())
                .build());

        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        // Insert data with single struct element
        List<JsonObject> dataList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);

            com.google.gson.JsonArray floatVector = new com.google.gson.JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                floatVector.add(v);
            }
            row.add(CommonData.fieldFloatVector, floatVector);

            // Single struct element
            com.google.gson.JsonArray structArray = new com.google.gson.JsonArray();
            JsonObject struct = new JsonObject();
            com.google.gson.JsonArray vec = new com.google.gson.JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                vec.add(v);
            }
            struct.add("vec", vec);
            structArray.add(struct);
            row.add("single_struct", structArray);

            dataList.add(row);
        }

        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(dataList)
                .build());

        Assert.assertEquals(insertResp.getInsertCnt(), 10);

        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }

    @Test(description = "Create struct with large maxCapacity", groups = {"Smoke"})
    public void createStructWithLargeCapacity() {
        String collectionName = "LargeCapacityStruct_" + GenerateUtil.getRandomString(6);

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());

        // Struct with large maxCapacity = 1000
        schema.addField(AddFieldReq.builder()
                .fieldName("large_struct")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(1000)
                .addStructField(AddFieldReq.builder()
                        .fieldName("idx")
                        .dataType(DataType.Int32)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("vec")
                        .dataType(DataType.FloatVector)
                        .dimension(32)  // Smaller dimension for large capacity
                        .build())
                .build());

        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        // Verify collection created
        ListCollectionsResp listResp = milvusClientV2.listCollections();
        Assert.assertTrue(listResp.getCollectionNames().contains(collectionName));

        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }

    @Test(description = "Insert data with struct array at maxCapacity limit", groups = {"Smoke"})
    public void insertStructAtMaxCapacity() {
        String collectionName = "MaxCapacityInsert_" + GenerateUtil.getRandomString(6);
        int maxCapacity = 50;

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("bounded_struct")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(maxCapacity)
                .addStructField(AddFieldReq.builder()
                        .fieldName("idx")
                        .dataType(DataType.Int32)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("vec")
                        .dataType(DataType.FloatVector)
                        .dimension(32)
                        .build())
                .build());

        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        // Insert data with exactly maxCapacity struct elements
        List<JsonObject> dataList = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 0L);

        com.google.gson.JsonArray floatVector = new com.google.gson.JsonArray();
        for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
            floatVector.add(v);
        }
        row.add(CommonData.fieldFloatVector, floatVector);

        // Fill struct array to maxCapacity
        com.google.gson.JsonArray structArray = new com.google.gson.JsonArray();
        for (int j = 0; j < maxCapacity; j++) {
            JsonObject struct = new JsonObject();
            struct.addProperty("idx", j);
            com.google.gson.JsonArray vec = new com.google.gson.JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, 32).get(0)) {
                vec.add(v);
            }
            struct.add("vec", vec);
            structArray.add(struct);
        }
        row.add("bounded_struct", structArray);
        dataList.add(row);

        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(dataList)
                .build());

        Assert.assertEquals(insertResp.getInsertCnt(), 1);

        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }

    @Test(description = "Insert data exceeding struct maxCapacity should fail", groups = {"Smoke"})
    public void insertStructExceedingCapacityShouldFail() {
        String collectionName = "ExceedCapacityTest_" + GenerateUtil.getRandomString(6);
        int maxCapacity = 10;

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("limited_struct")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(maxCapacity)
                .addStructField(AddFieldReq.builder()
                        .fieldName("val")
                        .dataType(DataType.Int32)
                        .build())
                .build());

        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        try {
            // Try to insert data exceeding maxCapacity
            List<JsonObject> dataList = new ArrayList<>();
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, 0L);

            com.google.gson.JsonArray floatVector = new com.google.gson.JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                floatVector.add(v);
            }
            row.add(CommonData.fieldFloatVector, floatVector);

            // Exceed maxCapacity (insert maxCapacity + 5 elements)
            com.google.gson.JsonArray structArray = new com.google.gson.JsonArray();
            for (int j = 0; j < maxCapacity + 5; j++) {
                JsonObject struct = new JsonObject();
                struct.addProperty("val", j);
                structArray.add(struct);
            }
            row.add("limited_struct", structArray);
            dataList.add(row);

            milvusClientV2.insert(InsertReq.builder()
                    .collectionName(collectionName)
                    .data(dataList)
                    .build());

            Assert.fail("Should throw exception when exceeding maxCapacity");
        } catch (Exception e) {
            // Expected exception
            Assert.assertTrue(e.getMessage().contains("capacity") || e.getMessage().contains("length") || e.getMessage().contains("exceed"),
                    "Exception should mention capacity/length violation: " + e.getMessage());
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    // ==================== Empty Struct Array Tests (Scenario 6) ====================

    @Test(description = "Insert data with empty struct array", groups = {"Smoke"})
    public void insertEmptyStructArray() {
        String collectionName = "EmptyStructArray_" + GenerateUtil.getRandomString(6);

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("optional_struct")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(100)
                .addStructField(AddFieldReq.builder()
                        .fieldName("data")
                        .dataType(DataType.VarChar)
                        .maxLength(256)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("vec")
                        .dataType(DataType.FloatVector)
                        .dimension(DIM)
                        .build())
                .build());

        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        // Insert data with empty struct array
        List<JsonObject> dataList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);

            com.google.gson.JsonArray floatVector = new com.google.gson.JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                floatVector.add(v);
            }
            row.add(CommonData.fieldFloatVector, floatVector);

            // Empty struct array
            com.google.gson.JsonArray emptyStructArray = new com.google.gson.JsonArray();
            row.add("optional_struct", emptyStructArray);

            dataList.add(row);
        }

        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(dataList)
                .build());

        Assert.assertEquals(insertResp.getInsertCnt(), 10);

        // Create index on regular vector field
        CommonFunction.createVectorIndex(collectionName, CommonData.fieldFloatVector,
                IndexParam.IndexType.HNSW, IndexParam.MetricType.L2);

        // Create index on struct vector field (required for loading)
        CommonFunction.createStructVectorIndex(collectionName, "optional_struct", "vec",
                "optional_struct_vec_idx", IndexParam.MetricType.MAX_SIM_COSINE);

        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        // Query to verify empty struct arrays are stored correctly
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter(CommonData.fieldInt64 + " < 5")
                .outputFields(Arrays.asList(CommonData.fieldInt64, "optional_struct"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().size(), 5);
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Object structData = result.getEntity().get("optional_struct");
            Assert.assertNotNull(structData);
            Assert.assertTrue(structData instanceof List);
            Assert.assertTrue(((List<?>) structData).isEmpty(), "Struct array should be empty");
        }

        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }

    @Test(description = "Insert mixed data - some with empty struct, some with data", groups = {"Smoke"})
    public void insertMixedEmptyAndNonEmptyStruct() {
        String collectionName = "MixedStructArray_" + GenerateUtil.getRandomString(6);

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("mixed_struct")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(50)
                .addStructField(AddFieldReq.builder()
                        .fieldName("val")
                        .dataType(DataType.Int32)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("vec")
                        .dataType(DataType.FloatVector)
                        .dimension(32)
                        .build())
                .build());

        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        // Insert mixed data: even IDs have empty struct, odd IDs have data
        List<JsonObject> dataList = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);

            com.google.gson.JsonArray floatVector = new com.google.gson.JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                floatVector.add(v);
            }
            row.add(CommonData.fieldFloatVector, floatVector);

            com.google.gson.JsonArray structArray = new com.google.gson.JsonArray();
            if (i % 2 == 1) {
                // Odd IDs: add struct elements
                for (int j = 0; j < 3; j++) {
                    JsonObject struct = new JsonObject();
                    struct.addProperty("val", i * 10 + j);
                    com.google.gson.JsonArray vec = new com.google.gson.JsonArray();
                    for (Float v : GenerateUtil.generateFloatVector(1, 6, 32).get(0)) {
                        vec.add(v);
                    }
                    struct.add("vec", vec);
                    structArray.add(struct);
                }
            }
            // Even IDs: keep empty struct array
            row.add("mixed_struct", structArray);
            dataList.add(row);
        }

        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(dataList)
                .build());

        Assert.assertEquals(insertResp.getInsertCnt(), 20);

        // Create index on regular vector field
        CommonFunction.createVectorIndex(collectionName, CommonData.fieldFloatVector,
                IndexParam.IndexType.HNSW, IndexParam.MetricType.L2);

        // Create index on struct vector field (required for loading)
        CommonFunction.createStructVectorIndex(collectionName, "mixed_struct", "vec",
                "mixed_struct_vec_idx", IndexParam.MetricType.MAX_SIM_COSINE);

        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        // Query and verify mixed results
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter(CommonData.fieldInt64 + " in [0, 1, 2, 3]")
                .outputFields(Arrays.asList(CommonData.fieldInt64, "mixed_struct"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().size(), 4);

        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Long id = (Long) result.getEntity().get(CommonData.fieldInt64);
            List<?> structData = (List<?>) result.getEntity().get("mixed_struct");

            if (id % 2 == 0) {
                Assert.assertTrue(structData.isEmpty(), "Even ID " + id + " should have empty struct array");
            } else {
                Assert.assertFalse(structData.isEmpty(), "Odd ID " + id + " should have non-empty struct array");
                Assert.assertEquals(structData.size(), 3, "Odd ID should have 3 struct elements");
            }
        }

        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }

    @Test(description = "Search collection with empty struct arrays should work on regular vector", groups = {"Smoke"})
    public void searchCollectionWithEmptyStructArrays() {
        String collectionName = "SearchEmptyStruct_" + GenerateUtil.getRandomString(6);

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("empty_struct")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(10)
                .addStructField(AddFieldReq.builder()
                        .fieldName("vec")
                        .dataType(DataType.FloatVector)
                        .dimension(DIM)
                        .build())
                .build());

        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        // Insert data with empty struct arrays
        List<JsonObject> dataList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);

            com.google.gson.JsonArray floatVector = new com.google.gson.JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                floatVector.add(v);
            }
            row.add(CommonData.fieldFloatVector, floatVector);

            // Empty struct array
            row.add("empty_struct", new com.google.gson.JsonArray());
            dataList.add(row);
        }

        milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(dataList)
                .build());

        // Create index on regular vector field
        CommonFunction.createVectorIndex(collectionName, CommonData.fieldFloatVector,
                IndexParam.IndexType.HNSW, IndexParam.MetricType.L2);

        // Create index on struct vector field (required for loading)
        CommonFunction.createStructVectorIndex(collectionName, "empty_struct", "vec",
                "empty_struct_vec_idx", IndexParam.MetricType.MAX_SIM_COSINE);

        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        // Search on regular vector field should work
        List<Float> queryVector = GenerateUtil.generateFloatVector(1, 6, DIM).get(0);
        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField(CommonData.fieldFloatVector)
                .data(Collections.singletonList(new FloatVec(queryVector)))
                .topK(10)
                .outputFields(Arrays.asList(CommonData.fieldInt64, "empty_struct"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= 10);

        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }
}
