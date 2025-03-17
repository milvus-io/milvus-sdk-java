package com.zilliz.milvustestv2.vectorOperation;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import io.milvus.v2.service.vector.response.UpsertResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 17:02
 */
public class UpsertTest extends BaseTest {
    String newCollectionName;
    String nullableDefaultCollectionName;

    @BeforeClass(alwaysRun = true)
    public void providerCollection() {
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        List<JsonObject> jsonObjects = CommonFunction.generateDefaultData(0,CommonData.numberEntities, CommonData.dim, DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(newCollectionName).data(jsonObjects).build());
        nullableDefaultCollectionName = CommonFunction.createNewNullableDefaultValueCollection(CommonData.dim, null, DataType.FloatVector);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(nullableDefaultCollectionName).build());
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
}
