package com.zilliz.milvustestv2.collection;

import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.MathUtil;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.param.Constant;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

/**
 * @Author yongpeng.li
 * @Date 2024/1/31 15:24
 */

public class CreateCollectionTest extends BaseTest {
    String simpleCollection = "simpleCollection";
    String repeatCollection = "repeatCollection";
    String collectionNameWithIndex = "collectionNameWithIndex";
    String collectionNameWithNull = "collectionNameWithNull";
    String collectionNameWithDefault = "collectionNameWithDefault";
    String collectionNameWithNullAndDefault = "collectionNameWithNullAndDefault";

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(simpleCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(repeatCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionNameWithIndex).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionNameWithNull).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionNameWithDefault).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionNameWithNullAndDefault).build());

    }

    @DataProvider(name = "VectorTypeList")
    public Object[][] providerVectorType() {
        return new Object[][]{
                {DataType.FloatVector},
                {DataType.BinaryVector},
                {DataType.Float16Vector},
                {DataType.BFloat16Vector},
                {DataType.SparseFloatVector},
        };
    }

    @Test(description = "Create simple collection success", groups = {"Smoke"})
    public void createSimpleCollectionSuccess() {
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(simpleCollection)
                .dimension(CommonData.dim)
                .autoID(false)
                .build());
        ListCollectionsResp listCollectionsResp = milvusClientV2.listCollections();
        Assert.assertTrue(listCollectionsResp.getCollectionNames().contains("simpleCollection"));
    }

    @Test(description = "Create duplicate collection", groups = {"Smoke"})
    public void createDuplicateSimpleCollection() {
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(repeatCollection)
                .dimension(CommonData.dim)
                .autoID(true)
                .build());
        try {
            milvusClientV2.createCollection(CreateCollectionReq.builder()
                    .collectionName(repeatCollection)
                    .dimension(CommonData.dim + 1)
                    .autoID(true)
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("create duplicate collection with different parameters"));
        }
    }

    @Test(description = "Create collection with index params,will auto load", groups = {"Smoke"})
    public void createCollectionWithIndexParams() {
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldInt32 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int32)
                .name(CommonData.fieldInt32)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldInt16 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int16)
                .name(CommonData.fieldInt16)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldInt8 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int8)
                .name(CommonData.fieldInt8)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldDouble = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Double)
                .name(CommonData.fieldDouble)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldArray = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Array)
                .name(CommonData.fieldArray)
                .elementType(DataType.Int64)
                .maxCapacity(1000)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldBool = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Bool)
                .name(CommonData.fieldBool)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldVarchar = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.VarChar)
                .name(CommonData.fieldVarchar)
                .isPrimaryKey(false)
                .maxLength(1000)
                .build();
        CreateCollectionReq.FieldSchema fieldFloat = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Float)
                .name(CommonData.fieldFloat)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldJson = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.JSON)
                .name(CommonData.fieldJson)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldFloatVector = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.FloatVector)
                .name(CommonData.fieldFloatVector)
                .isPrimaryKey(false)
                .dimension(CommonData.dim)
                .build();

        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        fieldSchemaList.add(fieldInt64);
        fieldSchemaList.add(fieldInt32);
        fieldSchemaList.add(fieldInt16);
        fieldSchemaList.add(fieldInt8);
        fieldSchemaList.add(fieldFloat);
        fieldSchemaList.add(fieldDouble);
        fieldSchemaList.add(fieldArray);
        fieldSchemaList.add(fieldBool);
        fieldSchemaList.add(fieldJson);
        fieldSchemaList.add(fieldVarchar);
        fieldSchemaList.add(fieldFloatVector);
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fieldSchemaList)
                .build();
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .extraParams(CommonFunction.provideExtraParam(IndexParam.IndexType.AUTOINDEX))
                .metricType(IndexParam.MetricType.L2)
                .build();
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionSchema(collectionSchema)
                .collectionName(collectionNameWithIndex)
                .enableDynamicField(false)
                .indexParams(Collections.singletonList(indexParam))
                .numShards(1)
                .build();
        BaseTest.milvusClientV2.createCollection(createCollectionReq);

        ListCollectionsResp listCollectionsResp = milvusClientV2.listCollections();
        Assert.assertTrue(listCollectionsResp.getCollectionNames().contains(collectionNameWithIndex));
        //insert
        CommonFunction.generateDefaultData(0, 100, CommonData.dim, DataType.FloatVector);
        // search
        SearchResp searchResp = CommonFunction.defaultSearch(collectionNameWithIndex);
        Assert.assertEquals(searchResp.getSearchResults().size(), 10);
    }

    @Test(description = "create collection with different vector ", groups = {"Smoke"}, dataProvider = "VectorTypeList")
    public void createCollectionWithDifferentVector(DataType vectorType) {
        String newCollection = CommonFunction.createNewCollection(CommonData.dim, null, vectorType);
        ListCollectionsResp listCollectionsResp = milvusClientV2.listCollections();
        Assert.assertTrue(listCollectionsResp.getCollectionNames().contains(newCollection));
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollection).build());
    }

    @Test(description = "Create collection with Null Data, will auto load", groups = {"Smoke"})
    public void createCollectionWithNullData() {
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldInt32 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int32)
                .name(CommonData.fieldInt32)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldInt16 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int16)
                .name(CommonData.fieldInt16)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldInt8 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int8)
                .name(CommonData.fieldInt8)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldDouble = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Double)
                .name(CommonData.fieldDouble)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldArray = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Array)
                .name(CommonData.fieldArray)
                .elementType(DataType.Int64)
                .maxCapacity(1000)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldBool = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Bool)
                .name(CommonData.fieldBool)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldVarchar = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.VarChar)
                .name(CommonData.fieldVarchar)
                .isPrimaryKey(false)
                .maxLength(1000)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldFloat = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Float)
                .name(CommonData.fieldFloat)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldJson = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.JSON)
                .name(CommonData.fieldJson)
                .isPrimaryKey(false)
                .isNullable(true)
                .build();
        CreateCollectionReq.FieldSchema fieldFloatVector = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.FloatVector)
                .name(CommonData.fieldFloatVector)
                .isPrimaryKey(false)
                .dimension(CommonData.dim)
                .build();

        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        fieldSchemaList.add(fieldInt64);
        fieldSchemaList.add(fieldInt32);
        fieldSchemaList.add(fieldInt16);
        fieldSchemaList.add(fieldInt8);
        fieldSchemaList.add(fieldFloat);
        fieldSchemaList.add(fieldDouble);
        fieldSchemaList.add(fieldArray);
        fieldSchemaList.add(fieldBool);
        fieldSchemaList.add(fieldJson);
        fieldSchemaList.add(fieldVarchar);
        fieldSchemaList.add(fieldFloatVector);
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fieldSchemaList)
                .build();
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .extraParams(CommonFunction.provideExtraParam(IndexParam.IndexType.AUTOINDEX))
                .metricType(IndexParam.MetricType.L2)
                .build();
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionSchema(collectionSchema)
                .collectionName(collectionNameWithNull)
                .enableDynamicField(false)
                .indexParams(Collections.singletonList(indexParam))
                .numShards(1)
                .build();
        BaseTest.milvusClientV2.createCollection(createCollectionReq);

        ListCollectionsResp listCollectionsResp = milvusClientV2.listCollections();
        Assert.assertTrue(listCollectionsResp.getCollectionNames().contains(collectionNameWithNull));
        //insert
        CommonFunction.generateDefaultData(0, 100, CommonData.dim, DataType.FloatVector);
        // search
        SearchResp searchResp = CommonFunction.defaultSearch(collectionNameWithNull);
        Assert.assertEquals(searchResp.getSearchResults().size(), 10);
    }

    @Test(description = "Create collection with Default Data, will auto load", groups = {"Smoke"})
    public void createCollectionWithDefaultData() {
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldInt32 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int32)
                .name(CommonData.fieldInt32)
                .isPrimaryKey(false)
                .defaultValue(CommonData.defaultValueInt)
                .build();
        CreateCollectionReq.FieldSchema fieldInt16 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int16)
                .name(CommonData.fieldInt16)
                .isPrimaryKey(false)
                .defaultValue(CommonData.defaultValueShort)
                .build();
        CreateCollectionReq.FieldSchema fieldInt8 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int8)
                .name(CommonData.fieldInt8)
                .isPrimaryKey(false)
                .defaultValue(CommonData.defaultValueShort)
                .build();
        CreateCollectionReq.FieldSchema fieldDouble = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Double)
                .name(CommonData.fieldDouble)
                .isPrimaryKey(false)
                .defaultValue(CommonData.defaultValueDouble)
                .build();
        CreateCollectionReq.FieldSchema fieldArray = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Array)
                .name(CommonData.fieldArray)
                .elementType(DataType.Int64)
                .maxCapacity(1000)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldBool = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Bool)
                .name(CommonData.fieldBool)
                .isPrimaryKey(false)
                .defaultValue(CommonData.defaultValueBool)
                .build();
        CreateCollectionReq.FieldSchema fieldVarchar = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.VarChar)
                .name(CommonData.fieldVarchar)
                .isPrimaryKey(false)
                .maxLength(1000)
                .defaultValue(CommonData.defaultValueString)
                .build();
        CreateCollectionReq.FieldSchema fieldFloat = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Float)
                .name(CommonData.fieldFloat)
                .isPrimaryKey(false)
                .defaultValue(CommonData.defaultValueFloat)
                .build();
        CreateCollectionReq.FieldSchema fieldJson = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.JSON)
                .name(CommonData.fieldJson)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldFloatVector = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.FloatVector)
                .name(CommonData.fieldFloatVector)
                .isPrimaryKey(false)
                .dimension(CommonData.dim)
                .build();

        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        fieldSchemaList.add(fieldInt64);
        fieldSchemaList.add(fieldInt32);
        fieldSchemaList.add(fieldInt16);
        fieldSchemaList.add(fieldInt8);
        fieldSchemaList.add(fieldFloat);
        fieldSchemaList.add(fieldDouble);
        fieldSchemaList.add(fieldArray);
        fieldSchemaList.add(fieldBool);
        fieldSchemaList.add(fieldJson);
        fieldSchemaList.add(fieldVarchar);
        fieldSchemaList.add(fieldFloatVector);
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fieldSchemaList)
                .build();
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .extraParams(CommonFunction.provideExtraParam(IndexParam.IndexType.AUTOINDEX))
                .metricType(IndexParam.MetricType.L2)
                .build();
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionSchema(collectionSchema)
                .collectionName(collectionNameWithDefault)
                .enableDynamicField(false)
                .indexParams(Collections.singletonList(indexParam))
                .numShards(1)
                .build();
        BaseTest.milvusClientV2.createCollection(createCollectionReq);

        ListCollectionsResp listCollectionsResp = milvusClientV2.listCollections();
        Assert.assertTrue(listCollectionsResp.getCollectionNames().contains(collectionNameWithDefault));
        //insert
        CommonFunction.generateDefaultData(0, 100, CommonData.dim, DataType.FloatVector);
        // search
        SearchResp searchResp = CommonFunction.defaultSearch(collectionNameWithDefault);
        Assert.assertEquals(searchResp.getSearchResults().size(), 10);
    }

    @Test(description = "Create collection with nullable and Default Data, will auto load", groups = {"Smoke"})
    public void createCollectionWithNullAndDefaultData() {
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldInt32 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int32)
                .name(CommonData.fieldInt32)
                .isPrimaryKey(false)
                .isNullable(true)
                .defaultValue(CommonData.defaultValueInt)
                .build();
        CreateCollectionReq.FieldSchema fieldInt16 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int16)
                .name(CommonData.fieldInt16)
                .isPrimaryKey(false)
                .isNullable(true)
                .defaultValue(CommonData.defaultValueShort)
                .build();
        CreateCollectionReq.FieldSchema fieldInt8 = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int8)
                .name(CommonData.fieldInt8)
                .isPrimaryKey(false)
                .isNullable(true)
                .defaultValue(CommonData.defaultValueShort)
                .build();
        CreateCollectionReq.FieldSchema fieldDouble = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Double)
                .name(CommonData.fieldDouble)
                .isPrimaryKey(false)
                .isNullable(true)
                .defaultValue(CommonData.defaultValueDouble)
                .build();
        CreateCollectionReq.FieldSchema fieldArray = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Array)
                .name(CommonData.fieldArray)
                .elementType(DataType.Int64)
                .maxCapacity(1000)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldBool = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Bool)
                .name(CommonData.fieldBool)
                .isPrimaryKey(false)
                .isNullable(true)
                .defaultValue(CommonData.defaultValueBool)
                .build();
        CreateCollectionReq.FieldSchema fieldVarchar = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.VarChar)
                .name(CommonData.fieldVarchar)
                .isPrimaryKey(false)
                .maxLength(1000)
                .isNullable(true)
                .defaultValue(CommonData.defaultValueString)
                .build();
        CreateCollectionReq.FieldSchema fieldFloat = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Float)
                .name(CommonData.fieldFloat)
                .isPrimaryKey(false)
                .isNullable(true)
                .defaultValue(CommonData.defaultValueFloat)
                .build();
        CreateCollectionReq.FieldSchema fieldJson = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.JSON)
                .name(CommonData.fieldJson)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldFloatVector = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.FloatVector)
                .name(CommonData.fieldFloatVector)
                .isPrimaryKey(false)
                .dimension(CommonData.dim)
                .build();

        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        fieldSchemaList.add(fieldInt64);
        fieldSchemaList.add(fieldInt32);
        fieldSchemaList.add(fieldInt16);
        fieldSchemaList.add(fieldInt8);
        fieldSchemaList.add(fieldFloat);
        fieldSchemaList.add(fieldDouble);
        fieldSchemaList.add(fieldArray);
        fieldSchemaList.add(fieldBool);
        fieldSchemaList.add(fieldJson);
        fieldSchemaList.add(fieldVarchar);
        fieldSchemaList.add(fieldFloatVector);
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fieldSchemaList)
                .build();
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .extraParams(CommonFunction.provideExtraParam(IndexParam.IndexType.AUTOINDEX))
                .metricType(IndexParam.MetricType.L2)
                .build();
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionSchema(collectionSchema)
                .collectionName(collectionNameWithNullAndDefault)
                .enableDynamicField(false)
                .indexParams(Collections.singletonList(indexParam))
                .numShards(1)
                .build();
        BaseTest.milvusClientV2.createCollection(createCollectionReq);

        ListCollectionsResp listCollectionsResp = milvusClientV2.listCollections();
        Assert.assertTrue(listCollectionsResp.getCollectionNames().contains(collectionNameWithNullAndDefault));
        //insert
        CommonFunction.generateDefaultData(0, 100, CommonData.dim, DataType.FloatVector);
        // search
        SearchResp searchResp = CommonFunction.defaultSearch(collectionNameWithNullAndDefault);
        Assert.assertEquals(searchResp.getSearchResults().size(), 10);
    }

    @Test(description = "Create collection with properties", groups = {"Smoke"})
    public void createCollectionWithProperties() {
        String collection = "a"+MathUtil.getRandomString(10);
        Map<String, String> map = new HashMap<String, String>() {{
            put(Constant.MMAP_ENABLED, "true");
        }};
        CreateCollectionReq.CollectionSchema collectionSchema = CommonFunction.providerCollectionSchema(CommonData.dim, DataType.FloatVector);
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collection)
                .properties(map)
                .collectionSchema(collectionSchema)
                .build());
        DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collection)
                .build());
        Map<String, String> properties = describeCollectionResp.getProperties();
        Assert.assertTrue(properties.containsKey(Constant.MMAP_ENABLED));
        Assert.assertTrue(properties.get(Constant.MMAP_ENABLED).equalsIgnoreCase("true"));
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collection).build());
    }

    @Test(description = "Create collection with functions", groups = {"Smoke"})
    public void createCollectionWithFunctions() {
        String collection = "a"+MathUtil.getRandomString(10);
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldVarchar = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.VarChar)
                .isPrimaryKey(false)
                .name(CommonData.fieldVarchar)
                .enableAnalyzer(true)
                .build();
        CreateCollectionReq.FieldSchema fieldVector = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.SparseFloatVector)
                .isPrimaryKey(false)
//                .dimension(CommonData.dim)
                .name(CommonData.fieldSparseVector)
                .build();

        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        fieldSchemaList.add(fieldInt64);
        fieldSchemaList.add(fieldVarchar);
        fieldSchemaList.add(fieldVector);
        List<CreateCollectionReq.Function> functions = new ArrayList<>();
        functions.add(CreateCollectionReq.Function.builder()
                .name("varcharToFloatVector")
                .functionType(FunctionType.BM25)
                .inputFieldNames(Lists.newArrayList(CommonData.fieldVarchar))
                .outputFieldNames(Lists.newArrayList(CommonData.fieldSparseVector))
                .build());
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fieldSchemaList)
                .functionList(functions)
                .build();

        milvusClientV2.createCollection(CreateCollectionReq.builder().collectionName(collection)
                .collectionSchema(schema)
                .numShards(1)
                .enableDynamicField(false)
                .description("collection desc")
                .build());

        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collection).build());
    }


}
