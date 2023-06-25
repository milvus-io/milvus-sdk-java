package com.zilliz.milvustest.serverless;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.highlevel.collection.CreateSimpleCollectionParam;
import io.milvus.param.highlevel.collection.ListCollectionsParam;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
import io.milvus.param.highlevel.dml.*;
import io.milvus.param.highlevel.dml.response.*;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2023/6/20 10:33
 */
@Epic("HighLevelApi")
@Feature("HighLevelApi")
public class HighLevel extends BaseTest {

    String commonCollection;

    @DataProvider(name = "collectionByDataProvider")
    public Object[][] provideCollectionName() {
        return new String[][]{{"collection_" + MathUtil.getRandomString(10)}};
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "Create collection success", dataProvider = "collectionByDataProvider", groups = {"Smoke"})
    public void createCollection(String collectionName) {
        commonCollection = collectionName;
        R<RpcStatus> collection = milvusClient.createCollection(CreateSimpleCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .withDescription("simple collection")
                .withDimension(128)
                .withAutoId(false)
                .withVectorField("book_intro")
                .withPrimaryField("book_id")
                .build());
        Assert.assertEquals(collection.getStatus().toString(), "0");
        Assert.assertEquals(collection.getData().getMsg(), "Success");
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "List Collection", groups = {"Smoke"}, dependsOnMethods = "createCollection")
    public void listCollection() {
        R<ListCollectionsResponse> listCollectionsResponseR = milvusClient.listCollections(ListCollectionsParam.newBuilder().build());
        Assert.assertEquals(listCollectionsResponseR.getStatus().intValue(), R.Status.Success.getCode());
        Assert.assertTrue(listCollectionsResponseR.getData().collectionNames.contains(commonCollection));
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "insert data into Collection", groups = {"Smoke"}, dependsOnMethods = "listCollection")
    public void insertIntoCollection() {
        List<JSONObject> jsonObjects = CommonFunction.generateDataWithDynamicFiledRow(10000);
        R<InsertResponse> insert = milvusClient.insert(InsertRowsParam.newBuilder()
                .withCollectionName(commonCollection)
                .withRows(jsonObjects)
                .build());
        Assert.assertEquals(insert.getStatus().intValue(), R.Status.Success.getCode());
        Assert.assertEquals(insert.getData().getInsertCount().intValue(), 10000);
    }


    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "search Collection", groups = {"Smoke"}, dependsOnMethods = "insertIntoCollection")
    public void searchCollection() {
        List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
        R<SearchResponse> search = milvusClient.search(SearchSimpleParam.newBuilder()
                .withCollectionName(commonCollection)
                .withOffset(0L)
                .withLimit(100)
                .withFilter("book_id>5000")
                .withVectors(search_vectors)
                .withOutputFields(Lists.newArrayList("*"))
                .build());
        Assert.assertEquals(search.getStatus().intValue(), R.Status.Success.getCode());
        Assert.assertEquals(search.getData().getRowRecords().size(), 100);
        System.out.println("search:"+search.getData().getRowRecords().toString());
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "query Collection", groups = {"Smoke"}, dependsOnMethods = "insertIntoCollection")
    public void queryCollection() {
        R<QueryResponse> query = milvusClient.query(QuerySimpleParam.newBuilder()
                .withCollectionName(commonCollection)
                .withFilter("book_id>5000")
                .withLimit(100L)
                .withOffset(0L)
                .withOutFields(Lists.newArrayList("*"))
                .build());
        Assert.assertEquals(query.getStatus().intValue(), R.Status.Success.getCode());
        Assert.assertEquals(query.getData().getRowRecords().size(), 100);
        System.out.println("query:"+query.getData().getRowRecords().toString());
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "get Collection", groups = {"Smoke"}, dependsOnMethods = "insertIntoCollection")
    public void getCollection() {
        R<GetResponse> getResponseR = milvusClient.get(GetIdsParam.newBuilder()
                .withCollectionName(commonCollection)
                .withOutputFields(Lists.newArrayList("*"))
                .withPrimaryIds(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
                .build());
        Assert.assertEquals(getResponseR.getStatus().intValue(), R.Status.Success.getCode());
        Assert.assertEquals(getResponseR.getData().getRowRecords().size(), 10);
        System.out.println("get:"+getResponseR.getData().getRowRecords().toString());
    }


    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "delete data ", groups = {"Smoke"}, dependsOnMethods = {"searchCollection", "queryCollection","getCollection"})
    public void delete() {
        ArrayList<Integer> integers = Lists.newArrayList(11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
        R<DeleteResponse> delete = milvusClient.delete(DeleteIdsParam.newBuilder()
                .withCollectionName(commonCollection)
                .withPrimaryIds(integers)
                .build());
        Assert.assertEquals(delete.getStatus().intValue(), R.Status.Success.getCode());
        Assert.assertEquals(delete.getData().getDeleteIds().size(), integers.size());
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "drop Collection", groups = {"Smoke"}, dependsOnMethods = {"delete"})
    public void dropCollection() {
        R<RpcStatus> rpcStatusR = milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(commonCollection).build());
        Assert.assertEquals(rpcStatusR.getStatus().intValue(), R.Status.Success.getCode());
    }
}
