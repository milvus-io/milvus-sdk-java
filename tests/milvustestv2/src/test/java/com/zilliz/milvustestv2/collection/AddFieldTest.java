package com.zilliz.milvustestv2.collection;

import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.response.SearchResp;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

@Slf4j
public class AddFieldTest extends BaseTest {
    String collectionName;
    String collectionNameWithLoaded;
    String collectionWithDynamicField;

    @BeforeClass
    public void initTestData() {
        collectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        collectionWithDynamicField = CommonFunction.createNewCollectionWithDynamic(CommonData.dim, null, DataType.FloatVector);
        CommonFunction.createIndexAndInsertAndLoad(collectionWithDynamicField, DataType.FloatVector, true, CommonData.numberEntities);
        collectionNameWithLoaded = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        CommonFunction.createIndexAndInsertAndLoad(collectionNameWithLoaded, DataType.FloatVector, true, CommonData.numberEntities);
    }

    @DataProvider(name = "providerDataType")
    public Object[][] addFieldData() {
        return new Object[][]{
                {DataType.Bool},
                {DataType.Int8},
                {DataType.Int16},
                {DataType.Int32},
                {DataType.Int64},
                {DataType.Float},
                {DataType.Double},
                {DataType.FloatVector},
                {DataType.BinaryVector},
                {DataType.VarChar},
                {DataType.SparseFloatVector},
                {DataType.Float16Vector},
                {DataType.Int8Vector},
                {DataType.Array},
                {DataType.BFloat16Vector}
        };
    }


    @AfterClass
    public void cleanData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionWithDynamicField).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionNameWithLoaded).build());
    }

    @Test(description = "add field without load", groups = {"Smoke"}, dataProvider = "providerDataType")
    public void addFieldBeforeLoad(DataType dataType) {
        AddCollectionFieldReq build = AddCollectionFieldReq.builder().collectionName(collectionName)
                .dataType(dataType)
                .fieldName("add_" + CommonData.providerFieldNameByDatatype(dataType))
                .isNullable(true)
                .build();
        boolean vector = dataType.equals(DataType.FloatVector) || dataType.equals(DataType.BinaryVector) || dataType.equals(DataType.SparseFloatVector) || dataType.equals(DataType.Float16Vector) || dataType.equals(DataType.Int8Vector) || dataType.equals(DataType.BFloat16Vector);
        if (vector) {
            build.setDimension(CommonData.dim);
        }
        if (dataType.equals(DataType.Array)) {
            build.setElementType(DataType.Int32);
            build.setMaxCapacity(CommonData.maxCapacity);
        }
        try {
            milvusClientV2.addCollectionField(build);
            DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
            CreateCollectionReq.CollectionSchema collectionSchema = describeCollectionResp.getCollectionSchema();
            CreateCollectionReq.FieldSchema field = collectionSchema.getField("add_" + CommonData.providerFieldNameByDatatype(dataType));
            Assert.assertEquals(field.getDataType(), dataType);
        } catch (Exception e) {
            Assert.assertTrue(vector);
            Assert.assertTrue(e.getMessage().contains("not support to add vector field"));
        }
    }

    @Test(description = "dynamic field collection add field ", groups = {"Smoke"}, dataProvider = "providerDataType")
    public void dynamicFieldCollectionAddField(DataType dataType) {
        AddCollectionFieldReq build = AddCollectionFieldReq.builder().collectionName(collectionWithDynamicField)
                .dataType(dataType)
                .fieldName("add_" + CommonData.providerFieldNameByDatatype(dataType))
                .isNullable(true)
                .build();
        boolean vector = dataType.equals(DataType.FloatVector) || dataType.equals(DataType.BinaryVector) || dataType.equals(DataType.SparseFloatVector) || dataType.equals(DataType.Float16Vector) || dataType.equals(DataType.Int8Vector) || dataType.equals(DataType.BFloat16Vector);
        if (vector) {
            build.setDimension(CommonData.dim);
        }
        if (dataType.equals(DataType.Array)) {
            build.setElementType(DataType.Int32);
            build.setMaxCapacity(CommonData.maxCapacity);
        }
        try {
            milvusClientV2.addCollectionField(build);
            DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                    .collectionName(collectionWithDynamicField)
                    .build());
            CreateCollectionReq.CollectionSchema collectionSchema = describeCollectionResp.getCollectionSchema();
            CreateCollectionReq.FieldSchema field = collectionSchema.getField("add_" + CommonData.providerFieldNameByDatatype(dataType));
            Assert.assertEquals(field.getDataType(), dataType);
        } catch (Exception e) {
            Assert.assertTrue(vector);
            Assert.assertTrue(e.getMessage().contains("not support to add vector field"));
        }
    }

    @Test(description = "add field with dynamic name ", groups = {"Smoke"}, dependsOnMethods = {"dynamicFieldCollectionAddField"})
    public void addFieldWithDynamicName() {
        milvusClientV2.addCollectionField(AddCollectionFieldReq.builder()
                .isNullable(true)
                .dataType(DataType.VarChar)
                .maxLength(100)
                .defaultValue("123")
                .fieldName(CommonData.dynamicField)
                .collectionName(collectionWithDynamicField)
                .build());
        DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionWithDynamicField)
                .build());
        CreateCollectionReq.CollectionSchema collectionSchema = describeCollectionResp.getCollectionSchema();
        CreateCollectionReq.FieldSchema field = collectionSchema.getField(CommonData.dynamicField);
        Assert.assertEquals(field.getDataType(), DataType.VarChar);
        // 查询
        List<BaseVector> baseVectors = CommonFunction.providerBaseVector(1, CommonData.dim, DataType.FloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .topK(1)
                .collectionName(collectionWithDynamicField)
                .outputFields(Lists.newArrayList(CommonData.dynamicField,"$meta"))
                .data(baseVectors)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .filter("$meta['dynamicField']['fieldInt64'] < 10")
                .build());
        List<SearchResp.SearchResult> searchResults = search.getSearchResults().get(0);
        Assert.assertEquals(searchResults.size(), 1);
        SearchResp.SearchResult searchResult = searchResults.get(0);
        Object o = searchResult.getEntity().get(CommonData.dynamicField);
        Assert.assertEquals(o.toString(), "123");
    }

    @Test(description = "add field repeatedly", groups = {"Smoke"}, dependsOnMethods = {"addFieldBeforeLoad"})
    public void addFieldRepeatedly() {
        AddCollectionFieldReq build = AddCollectionFieldReq.builder().collectionName(collectionName)
                .dataType(DataType.VarChar)
                .fieldName("add_" + CommonData.providerFieldNameByDatatype(DataType.VarChar))
                .isNullable(true)
                .build();
        try {
            milvusClientV2.addCollectionField(build);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("duplicate field"));
        }
    }

    @Test(description = "add field after alter field name", groups = {"Smoke"}, dependsOnMethods = {"addFieldBeforeLoad"})
    public void alterFieldNamAfterAddField() {
        milvusClientV2.alterCollectionField(AlterCollectionFieldReq.builder()
                .collectionName(collectionName)
                .fieldName("add_" + CommonData.providerFieldNameByDatatype(DataType.VarChar))
                .property("max_length", "199")
                .build());
        DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionName)
                .build());
        CreateCollectionReq.CollectionSchema collectionSchema = describeCollectionResp.getCollectionSchema();
        CreateCollectionReq.FieldSchema field = collectionSchema.getField("add_" + CommonData.providerFieldNameByDatatype(DataType.VarChar));
        Assert.assertEquals(field.getMaxLength(), 199);
    }

    @Test(description = "add field after load", groups = {"Smoke"}, dataProvider = "providerDataType")
    public void addFieldAfterLoad(DataType dataType) {
        AddCollectionFieldReq build = AddCollectionFieldReq.builder().collectionName(collectionNameWithLoaded)
                .dataType(dataType)
                .fieldName("add_" + CommonData.providerFieldNameByDatatype(dataType))
                .isNullable(true)
                .build();
        boolean vector = dataType.equals(DataType.FloatVector) || dataType.equals(DataType.BinaryVector) || dataType.equals(DataType.SparseFloatVector) || dataType.equals(DataType.Float16Vector) || dataType.equals(DataType.Int8Vector) || dataType.equals(DataType.BFloat16Vector);
        if (vector) {
            build.setDimension(CommonData.dim);
        }
        if (dataType.equals(DataType.Array)) {
            build.setElementType(DataType.Int32);
            build.setMaxCapacity(CommonData.maxCapacity);
        }
        try {
            milvusClientV2.addCollectionField(build);
            DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                    .collectionName(collectionNameWithLoaded)
                    .build());
            CreateCollectionReq.CollectionSchema collectionSchema = describeCollectionResp.getCollectionSchema();
            CreateCollectionReq.FieldSchema field = collectionSchema.getField("add_" + CommonData.providerFieldNameByDatatype(dataType));
            Assert.assertEquals(field.getDataType(), dataType);
        } catch (Exception e) {
            Assert.assertTrue(vector);
            Assert.assertTrue(e.getMessage().contains("not support to add vector field"));
        }
    }


}

