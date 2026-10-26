package com.zilliz.milvustestv2.collection;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddCollectionStructFieldReq;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.AddFunctionFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionFieldReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.DropFunctionFieldReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.EmbeddedText;
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
 * Test cases for Milvus 3.0 schema evolution APIs:
 * addCollectionStructField / addFunctionField / dropCollectionField / dropFunctionField
 *
 * Note: addFunctionField creates the function output field itself (fieldName/dataType in the
 * request describe the output field), so the base collection does NOT pre-create the sparse
 * output field.
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
public class SchemaEvolutionV3Test extends BaseTest {

    private String evolutionCollectionName;
    private static final int DIM = CommonData.dim;
    private static final String fieldText = "text";
    private static final String fieldToDrop = "to_drop";
    private static final String structFieldName = "added_struct";
    private static final String functionName = "bm25_func";
    private static final String sparseFieldName = "sparse";

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        evolutionCollectionName = "SchemaEvolution_" + GenerateUtil.getRandomString(6);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (evolutionCollectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(evolutionCollectionName)
                    .build());
        }
    }

    private static JsonArray floatVectorJson(int dim) {
        JsonArray floatVector = new JsonArray();
        for (Float v : GenerateUtil.generateFloatVector(1, 6, dim).get(0)) {
            floatVector.add(v);
        }
        return floatVector;
    }

    private static DescribeCollectionResp describe(String collectionName) {
        return milvusClientV2.describeCollection(
                DescribeCollectionReq.builder().collectionName(collectionName).build());
    }

    private static boolean functionExists(String collectionName, String name) {
        List<CreateCollectionReq.Function> functionList = describe(collectionName).getCollectionSchema().getFunctionList();
        if (functionList == null) {
            return false;
        }
        for (CreateCollectionReq.Function function : functionList) {
            if (name.equals(function.getName())) {
                return true;
            }
        }
        return false;
    }

    // ==================== Positive Tests (sequential on one collection, kept unloaded until insert verified) ====================

    @Test(description = "Create base collection for schema evolution", groups = {"Smoke", "V3"})
    public void createBaseCollection() {
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
        // analyzer enabled varchar, used as BM25 function input later
        schema.addField(AddFieldReq.builder()
                .fieldName(fieldText)
                .dataType(DataType.VarChar)
                .maxLength(65535)
                .enableAnalyzer(true)
                .build());
        // a plain scalar field to be dropped later
        schema.addField(AddFieldReq.builder()
                .fieldName(fieldToDrop)
                .dataType(DataType.Int32)
                .build());
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(evolutionCollectionName)
                .collectionSchema(schema)
                .build());
        Assert.assertTrue(describe(evolutionCollectionName).getFieldNames().contains(fieldToDrop));
    }

    @Test(description = "Add struct field online via addCollectionStructField", groups = {"Smoke", "V3"}, dependsOnMethods = {"createBaseCollection"})
    public void addStructFieldSuccess() {
        milvusClientV2.addCollectionStructField(AddCollectionStructFieldReq.builder()
                .collectionName(evolutionCollectionName)
                .fieldName(structFieldName)
                .maxCapacity(50)
                .addStructField(AddFieldReq.builder()
                        .fieldName("age")
                        .dataType(DataType.Int32)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("sub_vec")
                        .dataType(DataType.FloatVector)
                        .dimension(32)
                        .build())
                .build());

        CreateCollectionReq.StructFieldSchema structFieldSchema =
                describe(evolutionCollectionName).getCollectionSchema().getStructField(structFieldName);
        Assert.assertNotNull(structFieldSchema, "Struct field should exist after addCollectionStructField");
        Assert.assertEquals(structFieldSchema.getFields().size(), 2, "Struct field should have 2 sub-fields");
    }

    @Test(description = "Add BM25 function field online via addFunctionField", groups = {"Smoke", "V3"}, dependsOnMethods = {"addStructFieldSuccess"})
    public void addFunctionFieldSuccess() {
        milvusClientV2.addFunctionField(AddFunctionFieldReq.builder()
                .collectionName(evolutionCollectionName)
                .fieldName(sparseFieldName)
                .dataType(DataType.SparseFloatVector)
                .function(CreateCollectionReq.Function.builder()
                        .name(functionName)
                        .functionType(FunctionType.BM25)
                        .inputFieldNames(Collections.singletonList(fieldText))
                        .outputFieldNames(Collections.singletonList(sparseFieldName))
                        .build())
                .indexParam(IndexParam.builder()
                        .fieldName(sparseFieldName)
                        .indexName("sparse_idx")
                        .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                        .metricType(IndexParam.MetricType.BM25)
                        .build())
                .build());

        Assert.assertTrue(functionExists(evolutionCollectionName, functionName),
                "functionList should contain the new BM25 function");
        Assert.assertTrue(describe(evolutionCollectionName).getFieldNames().contains(sparseFieldName),
                "Function output field should be created");
    }

    @Test(description = "Drop a plain scalar field via dropCollectionField", groups = {"Smoke", "V3"}, dependsOnMethods = {"addFunctionFieldSuccess"})
    public void dropScalarFieldSuccess() {
        milvusClientV2.dropCollectionField(DropCollectionFieldReq.builder()
                .collectionName(evolutionCollectionName)
                .fieldName(fieldToDrop)
                .build());
        Assert.assertFalse(describe(evolutionCollectionName).getFieldNames().contains(fieldToDrop),
                "Dropped scalar field should not exist in schema");
    }

    @Test(description = "Insert data with text and verify function generated sparse vector", groups = {"Smoke", "V3"}, dependsOnMethods = {"dropScalarFieldSuccess"})
    public void insertAndVerifyFunctionField() {
        List<JsonObject> dataList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            row.add(CommonData.fieldFloatVector, floatVectorJson(DIM));
            row.addProperty(fieldText, "milvus vector database schema evolution test document number " + i);
            // struct field is nullable (addCollectionStructField defaults nullable=true), omit it
            dataList.add(row);
        }
        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(evolutionCollectionName)
                .data(dataList)
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), 10);

        // index all vector fields then load: float vector + struct sub-vector (sparse already indexed by addFunctionField)
        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("%s[%s]", structFieldName, "sub_vec"))
                .indexName("struct_vec_idx")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.MAX_SIM_COSINE)
                .build());
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(evolutionCollectionName)
                .indexParams(indexParams)
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(evolutionCollectionName)
                .build());

        // sparse 向量字段不允许 query 回读 raw data（not allowed to retrieve raw data of field sparse），
        // 改用 BM25 全文检索验证 function 字段已生效
        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(evolutionCollectionName)
                .annsField(sparseFieldName)
                .data(Collections.singletonList(new EmbeddedText("milvus vector database")))
                .limit(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty(),
                "BM25 search on function-generated sparse field should return results");
        Assert.assertFalse(searchResp.getSearchResults().get(0).isEmpty(),
                "BM25 search should hit documents containing 'milvus'");
    }

    @Test(description = "Drop function via dropFunctionField on a standalone collection", groups = {"Smoke", "V3"})
    public void dropFunctionFieldSuccess() {
        String collectionName = "DropFunction_" + GenerateUtil.getRandomString(6);
        try {
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
                    .fieldName(fieldText)
                    .dataType(DataType.VarChar)
                    .maxLength(65535)
                    .enableAnalyzer(true)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName(sparseFieldName)
                    .dataType(DataType.SparseFloatVector)
                    .build());
            schema.addFunction(CreateCollectionReq.Function.builder()
                    .name(functionName)
                    .functionType(FunctionType.BM25)
                    .inputFieldNames(Collections.singletonList(fieldText))
                    .outputFieldNames(Collections.singletonList(sparseFieldName))
                    .build());
            milvusClientV2.createCollection(CreateCollectionReq.builder()
                    .collectionName(collectionName)
                    .collectionSchema(schema)
                    .build());
            Assert.assertTrue(functionExists(collectionName, functionName),
                    "Function should exist before drop");

            milvusClientV2.dropFunctionField(DropFunctionFieldReq.builder()
                    .collectionName(collectionName)
                    .functionName(functionName)
                    .build());

            Assert.assertFalse(functionExists(collectionName, functionName),
                    "Function should be removed from functionList after dropFunctionField");
            // dropFunctionField drops the function together with its output field
            Assert.assertFalse(describe(collectionName).getFieldNames().contains(sparseFieldName),
                    "Function output field should be dropped together with the function");
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    // ==================== Negative Tests ====================

    @Test(description = "addFunctionField without indexParam should fail (client side validation)", groups = {"Smoke", "V3"})
    public void addFunctionFieldWithoutIndexParamShouldFail() {
        String collectionName = "FuncNoIndex_" + GenerateUtil.getRandomString(6);
        try {
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
                    .fieldName(fieldText)
                    .dataType(DataType.VarChar)
                    .maxLength(65535)
                    .enableAnalyzer(true)
                    .build());
            milvusClientV2.createCollection(CreateCollectionReq.builder()
                    .collectionName(collectionName)
                    .collectionSchema(schema)
                    .build());

            milvusClientV2.addFunctionField(AddFunctionFieldReq.builder()
                    .collectionName(collectionName)
                    .fieldName(sparseFieldName)
                    .dataType(DataType.SparseFloatVector)
                    .function(CreateCollectionReq.Function.builder()
                            .name(functionName)
                            .functionType(FunctionType.BM25)
                            .inputFieldNames(Collections.singletonList(fieldText))
                            .outputFieldNames(Collections.singletonList(sparseFieldName))
                            .build())
                    .build());
            Assert.fail("addFunctionField without indexParam should throw exception");
        } catch (Exception e) {
            // expected: client side validation requires non-null indexParam with indexType != None
            System.out.println("Expected exception for addFunctionField without indexParam: "
                    + e.getClass().getName() + ": " + e.getMessage());
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    @Test(description = "addFunctionField with mismatched output type (BM25 -> BinaryVector) should fail", groups = {"Smoke", "V3"})
    public void addFunctionFieldWrongOutputTypeShouldFail() {
        String collectionName = "FuncWrongType_" + GenerateUtil.getRandomString(6);
        try {
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
                    .fieldName(fieldText)
                    .dataType(DataType.VarChar)
                    .maxLength(65535)
                    .enableAnalyzer(true)
                    .build());
            milvusClientV2.createCollection(CreateCollectionReq.builder()
                    .collectionName(collectionName)
                    .collectionSchema(schema)
                    .build());

            // BM25 function output must be SparseFloatVector, BinaryVector should be rejected
            milvusClientV2.addFunctionField(AddFunctionFieldReq.builder()
                    .collectionName(collectionName)
                    .fieldName("sig")
                    .dataType(DataType.BinaryVector)
                    .dimension(512)
                    .function(CreateCollectionReq.Function.builder()
                            .name(functionName)
                            .functionType(FunctionType.BM25)
                            .inputFieldNames(Collections.singletonList(fieldText))
                            .outputFieldNames(Collections.singletonList("sig"))
                            .build())
                    .indexParam(IndexParam.builder()
                            .fieldName("sig")
                            .indexName("sig_idx")
                            .indexType(IndexParam.IndexType.MINHASH_LSH)
                            .metricType(IndexParam.MetricType.MHJACCARD)
                            .build())
                    .build());
            Assert.fail("addFunctionField with BM25 -> BinaryVector output should throw exception");
        } catch (Exception e) {
            // expected: client side validation of function output type
            System.out.println("Expected exception for BM25 with BinaryVector output: "
                    + e.getClass().getName() + ": " + e.getMessage());
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    @Test(description = "Drop primary key field should fail", groups = {"Smoke", "V3"})
    public void dropPrimaryKeyFieldShouldFail() {
        String collectionName = "DropPk_" + GenerateUtil.getRandomString(6);
        try {
            CommonFunction.createSimpleCollection(DIM, collectionName, false);
            milvusClientV2.dropCollectionField(DropCollectionFieldReq.builder()
                    .collectionName(collectionName)
                    .fieldName(CommonData.simplePk)
                    .build());
            Assert.fail("Drop primary key field should throw exception");
        } catch (Exception e) {
            // expected: server rejects dropping the primary key field
            System.out.println("Expected exception for dropping primary key field: "
                    + e.getClass().getName() + ": " + e.getMessage());
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    @Test(description = "Drop a non-existent field should fail", groups = {"Smoke", "V3"})
    public void dropNonExistentFieldShouldFail() {
        String collectionName = "DropNoField_" + GenerateUtil.getRandomString(6);
        try {
            CommonFunction.createSimpleCollection(DIM, collectionName, false);
            milvusClientV2.dropCollectionField(DropCollectionFieldReq.builder()
                    .collectionName(collectionName)
                    .fieldName("not_exist_field")
                    .build());
            Assert.fail("Drop a non-existent field should throw exception");
        } catch (Exception e) {
            // expected: server rejects dropping a field that does not exist
            System.out.println("Expected exception for dropping non-existent field: "
                    + e.getClass().getName() + ": " + e.getMessage());
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }
}
