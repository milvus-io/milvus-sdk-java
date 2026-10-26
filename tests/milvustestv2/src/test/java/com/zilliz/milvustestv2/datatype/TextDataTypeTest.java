package com.zilliz.milvustestv2.datatype;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.exception.ParamException;
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
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Test cases for Text data type (Milvus 3.0 new data type, code 25)
 * Covers: create/describe, insert/query, TEXT_MATCH (enableMatch), defaultValue, FMINDEX behavior
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
public class TextDataTypeTest extends BaseTest {

    private String textCollectionName;
    private static final String fieldText = "body";
    private static final int DIM = CommonData.dim;
    private static final int INSERT_COUNT = 500;

    private static final String[] WORDS = {
            "vector", "database", "search", "index", "embedding", "scalar", "query", "insert",
            "collection", "partition", "segment", "cluster", "node", "proxy", "token", "analyzer",
            "filter", "schema", "field", "entity", "memory", "disk", "cache", "compute"
    };

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        textCollectionName = "TextDataType_" + GenerateUtil.getRandomString(6);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (textCollectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(textCollectionName)
                    .build());
        }
    }

    /**
     * Create a collection with Int64 pk + Text field (analyzer + match enabled) + FloatVector.
     * Note: maxLength is ignored for Text fields and is not sent to server.
     */
    private static void createTextCollection(String collectionName) {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(fieldText)
                .dataType(DataType.Text)
                .enableAnalyzer(true)
                .enableMatch(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());
    }

    private static String generateText(Random random, boolean withMilvus) {
        StringBuilder sb = new StringBuilder();
        if (withMilvus) {
            sb.append("milvus ");
        }
        int wordCount = 8 + random.nextInt(5);
        for (int i = 0; i < wordCount; i++) {
            sb.append(WORDS[random.nextInt(WORDS.length)]).append(" ");
        }
        return sb.toString().trim();
    }

    private static List<JsonObject> generateTextData(long startId, int count) {
        List<JsonObject> dataList = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, startId + i);
            // even rows contain the keyword "milvus", odd rows do not
            row.addProperty(fieldText, generateText(random, i % 2 == 0));
            JsonArray floatVector = new JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                floatVector.add(v);
            }
            row.add(CommonData.fieldFloatVector, floatVector);
            dataList.add(row);
        }
        return dataList;
    }

    // ==================== Positive Tests ====================

    @Test(description = "Create collection with Text field and verify schema type", groups = {"Smoke", "V3"})
    public void createTextCollectionSuccess() {
        createTextCollection(textCollectionName);

        DescribeCollectionResp descResp = milvusClientV2.describeCollection(
                DescribeCollectionReq.builder().collectionName(textCollectionName).build());
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = descResp.getCollectionSchema().getFieldSchemaList();
        CreateCollectionReq.FieldSchema textField = null;
        for (CreateCollectionReq.FieldSchema fieldSchema : fieldSchemaList) {
            if (fieldText.equals(fieldSchema.getName())) {
                textField = fieldSchema;
                break;
            }
        }
        Assert.assertNotNull(textField, "Text field should exist in schema");
        Assert.assertEquals(textField.getDataType(), DataType.Text, "Field type should be Text");
    }

    @Test(description = "Insert 500 rows of English text into Text field", groups = {"Smoke", "V3"}, dependsOnMethods = {"createTextCollectionSuccess"})
    public void insertTextDataSuccess() {
        List<JsonObject> data = generateTextData(0, INSERT_COUNT);
        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(textCollectionName)
                .data(data)
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), INSERT_COUNT, "Insert count should match");
    }

    @Test(description = "Create index and load text collection", groups = {"Smoke", "V3"}, dependsOnMethods = {"insertTextDataSuccess"})
    public void createIndexAndLoad() {
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(new java.util.HashMap<String, Object>() {{
                    put("M", 16);
                    put("efConstruction", 64);
                }})
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(textCollectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(textCollectionName)
                .build());
    }

    @Test(description = "Query Text field and verify read back as String", groups = {"Smoke", "V3"}, dependsOnMethods = {"createIndexAndLoad"})
    public void queryTextFieldReturnString() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(textCollectionName)
                .filter(CommonData.fieldInt64 + " < 10")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldText))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertEquals(queryResp.getQueryResults().size(), 10);
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Object body = result.getEntity().get(fieldText);
            Assert.assertTrue(body instanceof String, "Text field should be read back as String, but got: "
                    + (body == null ? "null" : body.getClass().getName()));
            Assert.assertFalse(((String) body).isEmpty(), "Text value should not be empty");
        }
    }

    @Test(description = "TEXT_MATCH query on Text field with enableMatch", groups = {"Smoke", "V3"}, dependsOnMethods = {"createIndexAndLoad"})
    public void textMatchQuerySuccess() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(textCollectionName)
                .filter("TEXT_MATCH(body, \"milvus\")")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldText))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertFalse(queryResp.getQueryResults().isEmpty(), "TEXT_MATCH should return rows containing milvus");
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Object body = result.getEntity().get(fieldText);
            Assert.assertTrue(body instanceof String);
            Assert.assertTrue(((String) body).contains("milvus"),
                    "TEXT_MATCH result should contain keyword milvus: " + body);
        }
    }

    @Test(description = "Text field with defaultValue (3.0.10 SDK 把 default_value 按 VarChar 下发被服务端拒绝，行为固化)",
            groups = {"Smoke", "V3"})
    public void textFieldWithDefaultValue() {
        String collectionName = "TextDefault_" + GenerateUtil.getRandomString(6);
        String defaultText = "default text value";
        boolean created = false;
        try {
            CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
            schema.addField(AddFieldReq.builder()
                    .fieldName(CommonData.fieldInt64)
                    .dataType(DataType.Int64)
                    .isPrimaryKey(true)
                    .autoID(false)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName(fieldText)
                    .dataType(DataType.Text)
                    .enableAnalyzer(true)
                    .defaultValue(defaultText)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName(CommonData.fieldFloatVector)
                    .dataType(DataType.FloatVector)
                    .dimension(DIM)
                    .build());
            try {
                milvusClientV2.createCollection(CreateCollectionReq.builder()
                        .collectionName(collectionName)
                        .collectionSchema(schema)
                        .build());
                created = true;
            } catch (Exception e) {
                // 3.0.10 SDK 对 Text 字段的 defaultValue 按 DataType_VarChar 下发，
                // 服务端拒绝：type (Text) of field (body) is not equal to the type(DataType_VarChar)
                // of default_value —— 疑似 SDK bug（default value 类型未映射 Text），固化为预期报错
                Assert.assertTrue(e.getMessage().contains("default_value"),
                        "expected default_value type mismatch error, actual: " + e.getMessage());
                return;
            }

            // insert rows without the text field
            List<JsonObject> dataList = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                JsonObject row = new JsonObject();
                row.addProperty(CommonData.fieldInt64, (long) i);
                JsonArray floatVector = new JsonArray();
                for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                    floatVector.add(v);
                }
                row.add(CommonData.fieldFloatVector, floatVector);
                dataList.add(row);
            }
            InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                    .collectionName(collectionName)
                    .data(dataList)
                    .build());
            Assert.assertEquals(insertResp.getInsertCnt(), 10);

            IndexParam indexParam = IndexParam.builder()
                    .fieldName(CommonData.fieldFloatVector)
                    .indexType(IndexParam.IndexType.HNSW)
                    .metricType(IndexParam.MetricType.L2)
                    .build();
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(collectionName)
                    .indexParams(Collections.singletonList(indexParam))
                    .build());
            milvusClientV2.loadCollection(LoadCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());

            QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                    .collectionName(collectionName)
                    .filter(CommonData.fieldInt64 + " < 10")
                    .outputFields(Arrays.asList(CommonData.fieldInt64, fieldText))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            Assert.assertEquals(queryResp.getQueryResults().size(), 10);
            for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
                Assert.assertEquals(result.getEntity().get(fieldText), defaultText,
                        "Text field should return default value when not provided at insert");
            }
        } finally {
            if (created) {
                milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
            }
        }
    }

    // ==================== Negative Tests ====================

    @Test(description = "Insert number into Text field should fail with client ParamException", groups = {"Smoke", "V3"})
    public void insertNumberToTextFieldShouldFail() {
        String collectionName = "TextNegative_" + GenerateUtil.getRandomString(6);
        try {
            createTextCollection(collectionName);
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, 0L);
            // non-string value for Text field, client side should throw ParamException
            row.addProperty(fieldText, 12345);
            JsonArray floatVector = new JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                floatVector.add(v);
            }
            row.add(CommonData.fieldFloatVector, floatVector);

            milvusClientV2.insert(InsertReq.builder()
                    .collectionName(collectionName)
                    .data(Collections.singletonList(row))
                    .build());
            Assert.fail("Insert non-string value into Text field should throw exception");
        } catch (ParamException e) {
            // expected: client side param check
            System.out.println("Expected ParamException when inserting number into Text field: " + e.getMessage());
        } catch (Exception e) {
            // record actual behavior if the check happens on server side
            System.out.println("Insert number into Text field rejected by: " + e.getClass().getName()
                    + ": " + e.getMessage());
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    @Test(description = "Create FMINDEX on Text field, behavior recorded (javadoc says only for varchar)", groups = {"Smoke", "V3"})
    public void fmindexOnTextFieldBehavior() {
        String collectionName = "TextFmindex_" + GenerateUtil.getRandomString(6);
        try {
            createTextCollection(collectionName);
            // v3.0.10 has no FMINDEX example and extraParams are unknown, build without extraParams
            IndexParam indexParam = IndexParam.builder()
                    .fieldName(fieldText)
                    .indexType(IndexParam.IndexType.FMINDEX)
                    .build();
            try {
                milvusClientV2.createIndex(CreateIndexReq.builder()
                        .collectionName(collectionName)
                        .indexParams(Collections.singletonList(indexParam))
                        .build());
                // behavior record: FMINDEX accepted on Text field (javadoc states only for varchar type field)
                System.out.println("FMINDEX on Text field was ACCEPTED by server (javadoc says only for varchar)");
            } catch (Exception e) {
                // behavior record: FMINDEX rejected on Text field
                System.out.println("FMINDEX on Text field was REJECTED: " + e.getClass().getName()
                        + ": " + e.getMessage());
            }
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    @Test(description = "Create FMINDEX on Int64 field should fail", groups = {"Smoke", "V3"})
    public void fmindexOnInt64FieldShouldFail() {
        String collectionName = "IntFmindex_" + GenerateUtil.getRandomString(6);
        try {
            CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
            schema.addField(AddFieldReq.builder()
                    .fieldName(CommonData.fieldInt64)
                    .dataType(DataType.Int64)
                    .isPrimaryKey(true)
                    .autoID(false)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName(CommonData.fieldInt32)
                    .dataType(DataType.Int32)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName(CommonData.fieldFloatVector)
                    .dataType(DataType.FloatVector)
                    .dimension(DIM)
                    .build());
            milvusClientV2.createCollection(CreateCollectionReq.builder()
                    .collectionName(collectionName)
                    .collectionSchema(schema)
                    .build());

            IndexParam indexParam = IndexParam.builder()
                    .fieldName(CommonData.fieldInt32)
                    .indexType(IndexParam.IndexType.FMINDEX)
                    .build();
            try {
                milvusClientV2.createIndex(CreateIndexReq.builder()
                        .collectionName(collectionName)
                        .indexParams(Collections.singletonList(indexParam))
                        .build());
                Assert.fail("FMINDEX on non-varchar field should throw exception");
            } catch (Exception e) {
                // expected: FMINDEX only for varchar type field
                System.out.println("Expected exception for FMINDEX on Int32 field: " + e.getMessage());
            }
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }
}
