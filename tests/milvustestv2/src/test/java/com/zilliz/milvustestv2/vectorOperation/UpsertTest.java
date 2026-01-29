package com.zilliz.milvustestv2.vectorOperation;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
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
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.UpsertResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 17:02
 */
public class UpsertTest extends BaseTest {
    String newCollectionName;
    String nullableDefaultCollectionName;
    String partialUpdateCollection;
    String dynamicFieldCollection;
    private static final int DIM = 128;
    private static final int PARTIAL_UPDATE_ENTITY_COUNT = 100;

    @BeforeClass(alwaysRun = true)
    public void providerCollection() {
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        List<JsonObject> jsonObjects = CommonFunction.generateDefaultData(0,CommonData.numberEntities, CommonData.dim, DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(newCollectionName).data(jsonObjects).build());
        nullableDefaultCollectionName = CommonFunction.createNewNullableDefaultValueCollection(CommonData.dim, null, DataType.FloatVector);

        // Create collections for partial update tests
        partialUpdateCollection = "PartialUpdate_" + GenerateUtil.getRandomString(6);
        createPartialUpdateCollection(partialUpdateCollection, false);

        dynamicFieldCollection = "PartialUpdateDynamic_" + GenerateUtil.getRandomString(6);
        createPartialUpdateCollection(dynamicFieldCollection, true);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(nullableDefaultCollectionName).build());
        if (partialUpdateCollection != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(partialUpdateCollection).build());
        }
        if (dynamicFieldCollection != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(dynamicFieldCollection).build());
        }
    }

    private void createPartialUpdateCollection(String collectionName, boolean enableDynamicField) {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(enableDynamicField)
                .build();

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt32)
                .dataType(DataType.Int32)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldVarchar)
                .dataType(DataType.VarChar)
                .maxLength(256)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloat)
                .dataType(DataType.Float)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldDouble)
                .dataType(DataType.Double)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldBool)
                .dataType(DataType.Bool)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldJson)
                .dataType(DataType.JSON)
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

        // Insert initial data
        List<JsonObject> data = generatePartialUpdateData(0, PARTIAL_UPDATE_ENTITY_COUNT, enableDynamicField);
        milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(data)
                .build());

        // Create index and load
        CommonFunction.createVectorIndex(collectionName, CommonData.fieldFloatVector,
                IndexParam.IndexType.HNSW, IndexParam.MetricType.L2);
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());
    }

    private List<JsonObject> generatePartialUpdateData(int startId, int count, boolean withDynamicFields) {
        List<JsonObject> dataList = new ArrayList<>();

        for (int i = startId; i < startId + count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            row.addProperty(CommonData.fieldInt32, i * 10);
            row.addProperty(CommonData.fieldVarchar, "original_" + i);
            row.addProperty(CommonData.fieldFloat, i * 1.5f);
            row.addProperty(CommonData.fieldDouble, i * 2.5);
            row.addProperty(CommonData.fieldBool, i % 2 == 0);

            JsonObject jsonField = new JsonObject();
            jsonField.addProperty("key1", "value1_" + i);
            jsonField.addProperty("key2", i);
            row.add(CommonData.fieldJson, jsonField);

            JsonArray floatVector = new JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                floatVector.add(v);
            }
            row.add(CommonData.fieldFloatVector, floatVector);

            if (withDynamicFields) {
                row.addProperty("dynamic_field_a", "dynamic_value_" + i);
                row.addProperty("dynamic_field_b", i * 100);
            }

            dataList.add(row);
        }
        return dataList;
    }

    @DataProvider(name = "DifferentCollection")
    public Object[][] providerVectorType() {
        return new Object[][]{
                { DataType.FloatVector},
                { DataType.BinaryVector},
                { DataType.Float16Vector},
                { DataType.BFloat16Vector},
                { DataType.SparseFloatVector},
        };
    }

    @Test(description = "upsert collection", groups = {"Smoke"}, dataProvider = "DifferentCollection")
    public void upsert( DataType vectorType) {
        String collectionName = CommonFunction.createNewCollection(CommonData.dim, null, vectorType);
        CommonFunction.createIndexAndInsertAndLoad(collectionName,vectorType,true,CommonData.numberEntities);

        List<JsonObject> jsonObjects = CommonFunction.generateDefaultData(0,1, CommonData.dim, vectorType);
        for (int i = 1; i < 10; i++) {
            JsonObject jsonObject0 = jsonObjects.get(0).deepCopy();
            jsonObject0.addProperty(CommonData.fieldInt64, i);
            jsonObjects.add(jsonObject0);
        }
        UpsertResp upsert = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(collectionName)
                .data(jsonObjects)
                .partitionName("_default")
                .build());
        System.out.println(upsert);
        Assert.assertEquals(upsert.getUpsertCnt(), 10);
        // search
/*        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, vectorType);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .outputFields(Lists.newArrayList(CommonData.fieldInt64, CommonData.fieldInt32))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .partitionNames(Lists.newArrayList("_default"))
                .filter(CommonData.fieldInt32 + " == 0")
                .data(data)
                .topK(100)
                .build());
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), 10);*/

        // query
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter(CommonData.fieldInt32 + "== 0")
                        .partitionNames(Lists.newArrayList(CommonData.defaultPartitionName))
                .outputFields(Lists.newArrayList(CommonData.fieldInt64, CommonData.fieldInt32))
                .consistencyLevel(ConsistencyLevel.STRONG).build());
        Assert.assertEquals(query.getQueryResults().size(),10);
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }

    @Test(description = "upsert collection", groups = {"Smoke"})
    public void simpleUpsert() {
        String collection = CommonFunction.createSimpleCollection(128, null,false);
        List<JsonObject> jsonObjects = CommonFunction.generateSimpleData(CommonData.numberEntities, CommonData.dim);
        milvusClientV2.insert(InsertReq.builder().collectionName(collection).data(jsonObjects).build());
        List<JsonObject> jsonObjectsNew = CommonFunction.generateSimpleData(10, CommonData.dim);
        UpsertResp upsert = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(collection)
                .data(jsonObjectsNew)
                .build());
        System.out.println(upsert);
    }

    @Test(description = "upsert nullable collection", groups = {"Smoke"}, dataProvider = "DifferentCollection")
    public void nullableCollectionUpsert( DataType vectorType) {
        String collectionName = CommonFunction.createNewNullableDefaultValueCollection(CommonData.dim, null, vectorType);
        CommonFunction.createIndexAndInsertAndLoad(collectionName,vectorType,true,CommonData.numberEntities);

        List<JsonObject> jsonObjects = CommonFunction.generateSimpleNullData(0,1, CommonData.dim, vectorType);
        for (int i = 1; i < 10; i++) {
            JsonObject jsonObject0 = jsonObjects.get(0).deepCopy();
            jsonObject0.addProperty(CommonData.fieldInt64, i);
            jsonObjects.add(jsonObject0);
        }
        UpsertResp upsert = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(collectionName)
                .data(jsonObjects)
                .partitionName("_default")
                .build());
        System.out.println(upsert);
        Assert.assertEquals(upsert.getUpsertCnt(), 10);

        // query
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter(CommonData.fieldInt32 + " == 0")
                .partitionNames(Lists.newArrayList(CommonData.defaultPartitionName))
                .outputFields(Lists.newArrayList(CommonData.fieldInt64, CommonData.fieldInt32))
                .consistencyLevel(ConsistencyLevel.STRONG).build());
        Assert.assertEquals(query.getQueryResults().size(),10);
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }

    // ==================== Partial Update Tests ====================

    @Test(description = "Partial update - update single scalar field", groups = {"Smoke"})
    public void partialUpdateSingleField() {
        List<JsonObject> updateData = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 5L);
        row.addProperty(CommonData.fieldVarchar, "updated_description");
        updateData.add(row);

        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(partialUpdateCollection)
                .data(updateData)
                .partialUpdate(true)
                .build());

        Assert.assertEquals(upsertResp.getUpsertCnt(), 1);

        // Verify
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(partialUpdateCollection)
                .filter(CommonData.fieldInt64 + " == 5")
                .outputFields(Arrays.asList(CommonData.fieldVarchar, CommonData.fieldInt32))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().size(), 1);
        QueryResp.QueryResult result = queryResp.getQueryResults().get(0);
        Assert.assertEquals(result.getEntity().get(CommonData.fieldVarchar), "updated_description");
        Assert.assertEquals(((Number) result.getEntity().get(CommonData.fieldInt32)).intValue(), 50);
    }

    @Test(description = "Partial update - update multiple scalar fields", groups = {"Smoke"})
    public void partialUpdateMultipleFields() {
        List<JsonObject> updateData = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 10L);
        row.addProperty(CommonData.fieldVarchar, "multi_updated");
        row.addProperty(CommonData.fieldInt32, 9999);
        updateData.add(row);

        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(partialUpdateCollection)
                .data(updateData)
                .partialUpdate(true)
                .build());

        Assert.assertEquals(upsertResp.getUpsertCnt(), 1);

        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(partialUpdateCollection)
                .filter(CommonData.fieldInt64 + " == 10")
                .outputFields(Arrays.asList(CommonData.fieldVarchar, CommonData.fieldInt32, CommonData.fieldFloat))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().size(), 1);
        QueryResp.QueryResult result = queryResp.getQueryResults().get(0);
        Assert.assertEquals(result.getEntity().get(CommonData.fieldVarchar), "multi_updated");
        Assert.assertEquals(((Number) result.getEntity().get(CommonData.fieldInt32)).intValue(), 9999);
        Assert.assertEquals(((Number) result.getEntity().get(CommonData.fieldFloat)).floatValue(), 15.0f, 0.01f);
    }

    @Test(description = "Partial update - batch update multiple entities", groups = {"Smoke"})
    public void partialUpdateBatch() {
        List<JsonObject> updateData = new ArrayList<>();
        for (int i = 20; i < 25; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            row.addProperty(CommonData.fieldVarchar, "batch_updated_" + i);
            updateData.add(row);
        }

        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(partialUpdateCollection)
                .data(updateData)
                .partialUpdate(true)
                .build());

        Assert.assertEquals(upsertResp.getUpsertCnt(), 5);

        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(partialUpdateCollection)
                .filter(CommonData.fieldInt64 + " >= 20 && " + CommonData.fieldInt64 + " < 25")
                .outputFields(Arrays.asList(CommonData.fieldVarchar))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().size(), 5);
        for (QueryResp.QueryResult r : queryResp.getQueryResults()) {
            String varchar = (String) r.getEntity().get(CommonData.fieldVarchar);
            Assert.assertTrue(varchar.startsWith("batch_updated_"));
        }
    }

    @Test(description = "Partial update - update float field", groups = {"Smoke"})
    public void partialUpdateFloatField() {
        List<JsonObject> updateData = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 30L);
        row.addProperty(CommonData.fieldFloat, 999.99f);
        updateData.add(row);

        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(partialUpdateCollection)
                .data(updateData)
                .partialUpdate(true)
                .build());

        Assert.assertEquals(upsertResp.getUpsertCnt(), 1);

        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(partialUpdateCollection)
                .filter(CommonData.fieldInt64 + " == 30")
                .outputFields(Arrays.asList(CommonData.fieldFloat, CommonData.fieldInt32))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().size(), 1);
        Float floatValue = ((Number) queryResp.getQueryResults().get(0).getEntity().get(CommonData.fieldFloat)).floatValue();
        Assert.assertEquals(floatValue, 999.99f, 0.01f);
        Assert.assertEquals(((Number) queryResp.getQueryResults().get(0).getEntity().get(CommonData.fieldInt32)).intValue(), 300);
    }

    @Test(description = "Partial update - update bool field", groups = {"Smoke"})
    public void partialUpdateBoolField() {
        List<JsonObject> updateData = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 40L);
        row.addProperty(CommonData.fieldBool, false);
        updateData.add(row);

        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(partialUpdateCollection)
                .data(updateData)
                .partialUpdate(true)
                .build());

        Assert.assertEquals(upsertResp.getUpsertCnt(), 1);

        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(partialUpdateCollection)
                .filter(CommonData.fieldInt64 + " == 40")
                .outputFields(Arrays.asList(CommonData.fieldBool))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().size(), 1);
        Assert.assertEquals(queryResp.getQueryResults().get(0).getEntity().get(CommonData.fieldBool), false);
    }

    @Test(description = "Partial update - update JSON field", groups = {"Smoke"})
    public void partialUpdateJsonField() {
        List<JsonObject> updateData = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 45L);

        JsonObject newJsonValue = new JsonObject();
        newJsonValue.addProperty("updated_key", "updated_value");
        newJsonValue.addProperty("new_key", 12345);
        row.add(CommonData.fieldJson, newJsonValue);
        updateData.add(row);

        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(partialUpdateCollection)
                .data(updateData)
                .partialUpdate(true)
                .build());

        Assert.assertEquals(upsertResp.getUpsertCnt(), 1);

        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(partialUpdateCollection)
                .filter(CommonData.fieldInt64 + " == 45")
                .outputFields(Arrays.asList(CommonData.fieldJson))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().size(), 1);
    }

    @Test(description = "Partial update - update vector field", groups = {"Smoke"})
    public void partialUpdateVectorField() {
        List<JsonObject> updateData = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 50L);

        JsonArray newVector = new JsonArray();
        for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
            newVector.add(v);
        }
        row.add(CommonData.fieldFloatVector, newVector);
        updateData.add(row);

        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(partialUpdateCollection)
                .data(updateData)
                .partialUpdate(true)
                .build());

        Assert.assertEquals(upsertResp.getUpsertCnt(), 1);

        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(partialUpdateCollection)
                .filter(CommonData.fieldInt64 + " == 50")
                .outputFields(Arrays.asList(CommonData.fieldInt32, CommonData.fieldVarchar))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().size(), 1);
        Assert.assertEquals(((Number) queryResp.getQueryResults().get(0).getEntity().get(CommonData.fieldInt32)).intValue(), 500);
    }

    @Test(description = "Partial update - update dynamic field", groups = {"Smoke"})
    public void partialUpdateDynamicField() {
        List<JsonObject> updateData = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 55L);
        row.addProperty("dynamic_field_a", "new_dynamic_value");
        updateData.add(row);

        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(dynamicFieldCollection)
                .data(updateData)
                .partialUpdate(true)
                .build());

        Assert.assertEquals(upsertResp.getUpsertCnt(), 1);

        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(dynamicFieldCollection)
                .filter(CommonData.fieldInt64 + " == 55")
                .outputFields(Arrays.asList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().size(), 1);
        Assert.assertEquals(queryResp.getQueryResults().get(0).getEntity().get("dynamic_field_a"), "new_dynamic_value");
        Object dynamicFieldB = queryResp.getQueryResults().get(0).getEntity().get("dynamic_field_b");
        Assert.assertEquals(((Number) dynamicFieldB).longValue(), 5500L);
    }

    @Test(description = "Partial update - add new dynamic field", groups = {"Smoke"})
    public void partialUpdateAddNewDynamicField() {
        List<JsonObject> updateData = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 60L);
        row.addProperty("new_dynamic_field", "brand_new_value");
        updateData.add(row);

        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(dynamicFieldCollection)
                .data(updateData)
                .partialUpdate(true)
                .build());

        Assert.assertEquals(upsertResp.getUpsertCnt(), 1);

        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(dynamicFieldCollection)
                .filter(CommonData.fieldInt64 + " == 60")
                .outputFields(Arrays.asList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().size(), 1);
        Assert.assertEquals(queryResp.getQueryResults().get(0).getEntity().get("new_dynamic_field"), "brand_new_value");
    }

    @Test(description = "Partial update with different vector types", groups = {"Smoke"}, dataProvider = "DifferentCollection")
    public void partialUpdateWithDifferentVectorTypes(DataType vectorType) {
        String collectionName = CommonFunction.createNewCollection(CommonData.dim, null, vectorType);
        CommonFunction.createIndexAndInsertAndLoad(collectionName, vectorType, true, 100L);

        List<JsonObject> updateData = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 5L);
        row.addProperty(CommonData.fieldVarchar, "vector_type_test_" + vectorType.name());
        updateData.add(row);

        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(collectionName)
                .data(updateData)
                .partialUpdate(true)
                .build());

        Assert.assertEquals(upsertResp.getUpsertCnt(), 1);

        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter(CommonData.fieldInt64 + " == 5")
                .outputFields(Arrays.asList(CommonData.fieldVarchar))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().get(0).getEntity().get(CommonData.fieldVarchar),
                "vector_type_test_" + vectorType.name());

        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }
}
