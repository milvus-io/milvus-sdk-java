package com.zilliz.milvustestv2.vectorOperation;

import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.param.collection.FlushParam;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.GetCollectionStatsReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.response.InsertResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 17:02
 */
public class InsertTest extends BaseTest {
    private String newCollectionName;
    private String simpleCollectionName;

    @BeforeClass(alwaysRun = true)
    public void providerCollection() {
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        simpleCollectionName = CommonFunction.createSimpleCollection(CommonData.dim, null);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(simpleCollectionName).build());
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

    @Test(description = "insert test", groups = {"Smoke"})
    public void insert() {
        List<JsonObject> jsonObjects = CommonFunction.generateDefaultData(0,CommonData.numberEntities, CommonData.dim, DataType.FloatVector);
        InsertResp insert = milvusClientV2.insert(InsertReq.builder()
                .data(jsonObjects)
                .collectionName(newCollectionName)
                .build());
        Assert.assertEquals(insert.getInsertCnt(), CommonData.numberEntities);
    }

    @Test(description = "insert simple collection test", groups = {"Smoke"})
    public void insertIntoSimpleCollection() {
        List<JsonObject> jsonObjects = CommonFunction.generateSimpleData(CommonData.numberEntities, CommonData.dim);
        InsertResp insert = milvusClientV2.insert(InsertReq.builder()
                .data(jsonObjects)
                .collectionName(simpleCollectionName)
                .build());
        Assert.assertEquals(insert.getInsertCnt(), CommonData.numberEntities);
    }

    @Test(description = "insert different vector collection test", groups = {"Smoke"}, dataProvider = "VectorTypeList")
    public void insertIntoDiffVectorCollection(DataType dataType) {
        String newCollection = CommonFunction.createNewCollection(CommonData.dim, null, dataType);
        List<JsonObject> jsonObjects = CommonFunction.generateDefaultData(0,CommonData.numberEntities, CommonData.dim, dataType);
        InsertResp insert = milvusClientV2.insert(InsertReq.builder()
                .collectionName(newCollection)
                .data(jsonObjects).build());
        Assert.assertEquals(insert.getInsertCnt(), CommonData.numberEntities);
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollection).build());
    }

}
