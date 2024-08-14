package com.zilliz.milvustestv2.collection;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.common.resourcegroup.ResourceGroupConfig;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.ws.rs.ext.Provider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/1/31 15:24
 */

public class CreateCollectionTest extends BaseTest {
    String simpleCollection="simpleCollection";
    String repeatCollection="repeatCollection";
    String collectionNameWithIndex="collectionNameWithIndex";
    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(simpleCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(repeatCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionNameWithIndex).build());

    }

    @DataProvider(name = "VectorTypeList")
    public Object[][] providerVectorType(){
        return new Object[][]{
                {DataType.FloatVector},
                {DataType.BinaryVector},
                {DataType.Float16Vector},
                {DataType.BFloat16Vector},
                {DataType.SparseFloatVector},
        };
    }
    @Test(description = "Create simple collection success", groups = {"Smoke"})
    public void createSimpleCollectionSuccess(){
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                        .collectionName(simpleCollection)
                        .dimension(CommonData.dim)
                        .autoID(false)
                .build());
        ListCollectionsResp listCollectionsResp = milvusClientV2.listCollections();
        Assert.assertTrue(listCollectionsResp.getCollectionNames().contains("simpleCollection"));
    }
    @Test(description = "Create duplicate collection", groups = {"Smoke"})
    public void createDuplicateSimpleCollection(){
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(repeatCollection)
                .dimension(CommonData.dim)
                .autoID(true)
                .build());
        try {
            milvusClientV2.createCollection(CreateCollectionReq.builder()
                    .collectionName(repeatCollection)
                    .dimension(CommonData.dim+1)
                    .autoID(true)
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("create duplicate collection with different parameters"));
        }
    }

    @Test(description = "Create collection with index params,will auto load", groups = {"Smoke"})
    public void createCollectionWithIndexParams(){
        CreateCollectionReq.FieldSchema fieldInt64=CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldInt32=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int32)
                .name(CommonData.fieldInt32)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldInt16=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int16)
                .name(CommonData.fieldInt16)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldInt8=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int8)
                .name(CommonData.fieldInt8)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldDouble=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Double)
                .name(CommonData.fieldDouble)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldArray=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Array)
                .name(CommonData.fieldArray)
                .elementType(DataType.Int64)
                .maxCapacity(1000)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldBool=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Bool)
                .name(CommonData.fieldBool)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldVarchar=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.VarChar)
                .name(CommonData.fieldVarchar)
                .isPrimaryKey(false)
                .maxLength(1000)
                .build();
        CreateCollectionReq.FieldSchema fieldFloat=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Float)
                .name(CommonData.fieldFloat)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldJson=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.JSON)
                .name(CommonData.fieldJson)
                .isPrimaryKey(false)
                .build();
        CreateCollectionReq.FieldSchema fieldFloatVector=CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.FloatVector)
                .name(CommonData.fieldFloatVector)
                .isPrimaryKey(false)
                .dimension(CommonData.dim)
                .build();

        List<CreateCollectionReq.FieldSchema> fieldSchemaList=new ArrayList<>();
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
        CreateCollectionReq.CollectionSchema collectionSchema= CreateCollectionReq.CollectionSchema.builder()
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
        CommonFunction.generateDefaultData(0,100,CommonData.dim,DataType.FloatVector);
        // search
        SearchResp searchResp = CommonFunction.defaultSearch(collectionNameWithIndex);
        Assert.assertEquals(searchResp.getSearchResults().size(),10);
    }

    @Test(description="create collection with different vector ", groups={"Smoke"}, dataProvider="VectorTypeList")
    public void createCollectionWithDifferentVector(DataType vectorType){
        String newCollection = CommonFunction.createNewCollection(CommonData.dim, null, vectorType);
        ListCollectionsResp listCollectionsResp = milvusClientV2.listCollections();
        Assert.assertTrue(listCollectionsResp.getCollectionNames().contains(newCollection));
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollection).build());
    }

}
