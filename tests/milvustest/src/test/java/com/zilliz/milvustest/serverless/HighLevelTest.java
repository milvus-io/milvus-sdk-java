package com.zilliz.milvustest.serverless;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.DataType;
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
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2023/6/20 10:33
 */
@Epic("HighLevelApi")
@Feature("HighLevelApi")
public class HighLevelTest extends BaseTest {

    String commonCollection;
    String varcharPKCollection;

    @BeforeTest(alwaysRun = true)
    public void provideTestData(){
        commonCollection="collection_" + MathUtil.getRandomString(10);
        varcharPKCollection="collection_" + MathUtil.getRandomString(10);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "Create collection success", groups = {"Smoke"})
    public void createCollectionTest() {
        R<RpcStatus> collection = milvusClient.createCollection(CreateSimpleCollectionParam.newBuilder()
                .withCollectionName(commonCollection)
                .withDescription("simple collection")
                .withDimension(128)
                .withAutoId(false)
                .withVectorField("book_intro")
                .withPrimaryField("book_id").withPrimaryFieldType(DataType.Int64)
                .build());
        Assert.assertEquals(collection.getStatus().toString(), "0");
        Assert.assertEquals(collection.getData().getMsg(), "Success");
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "List Collection", groups = {"Smoke"}, dependsOnMethods = "createCollectionTest")
    public void listCollectionTest() {
        R<ListCollectionsResponse> listCollectionsResponseR = milvusClient.listCollections(ListCollectionsParam.newBuilder().build());
        Assert.assertEquals(listCollectionsResponseR.getStatus().intValue(), R.Status.Success.getCode());
        Assert.assertTrue(listCollectionsResponseR.getData().collectionNames.contains(commonCollection));
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "insert data into Collection", groups = {"Smoke"}, dependsOnMethods = "listCollectionTest")
    public void insertIntoCollectionTest() {
        List<JSONObject> jsonObjects = CommonFunction.generateDataWithDynamicFiledRow(10000);
        R<InsertResponse> insert = milvusClient.insert(InsertRowsParam.newBuilder()
                .withCollectionName(commonCollection)
                .withRows(jsonObjects)
                .build());
        Assert.assertEquals(insert.getStatus().intValue(), R.Status.Success.getCode());
        Assert.assertEquals(insert.getData().getInsertCount().intValue(), 10000);
    }


    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "search Collection", groups = {"Smoke"}, dependsOnMethods = "insertIntoCollectionTest")
    public void searchCollection() {
        List<List<Float>> search_vectors = Collections.singletonList(Arrays.asList(MathUtil.generateFloat(128)));
        R<SearchResponse> search = milvusClient.search(SearchSimpleParam.newBuilder()
                .withCollectionName(commonCollection)
                .withOffset(0L)
                .withLimit(100L)
                .withFilter("book_id>5000")
                .withVectors(search_vectors)
                .withOutputFields(Lists.newArrayList("*"))
                .build());
        Assert.assertEquals(search.getStatus().intValue(), R.Status.Success.getCode());
        Assert.assertEquals(search.getData().getRowRecords().size(), 100);
        System.out.println("search:"+search.getData().getRowRecords().toString());
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "query Collection", groups = {"Smoke"}, dependsOnMethods = "insertIntoCollectionTest")
    public void queryCollection() {
        R<QueryResponse> query = milvusClient.query(QuerySimpleParam.newBuilder()
                .withCollectionName(commonCollection)
                .withFilter("book_id>5000")
                .withLimit(100L)
                .withOffset(0L)
                .withOutputFields(Lists.newArrayList("*"))
                .build());
        Assert.assertEquals(query.getStatus().intValue(), R.Status.Success.getCode());
        Assert.assertEquals(query.getData().getRowRecords().size(), 100);
        System.out.println("query:"+query.getData().getRowRecords().toString());
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "get Collection", groups = {"Smoke"}, dependsOnMethods = "insertIntoCollectionTest")
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
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "drop collection", groups = {"Smoke"}, dependsOnMethods = {"delete"})
    public void dropCollection() {
        R<RpcStatus> rpcStatusR = milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(commonCollection).build());
        Assert.assertEquals(rpcStatusR.getStatus().intValue(), R.Status.Success.getCode());
    }


    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "create collection with varchar PK",groups = {"Smoke"})
    public  void createCollectionWithVarcharPK(){
        R<RpcStatus> collection = milvusClient.createCollection(CreateSimpleCollectionParam.newBuilder()
                .withCollectionName(varcharPKCollection)
                .withDescription("simple collection")
                .withDimension(128)
                .withAutoId(false)
                .withVectorField(CommonData.defaultVectorField)
                .withPrimaryField("book_name")
                .withPrimaryFieldType(DataType.VarChar)
                .withMaxLength(100)
                .build());
        Assert.assertEquals(collection.getStatus().toString(), "0");
        Assert.assertEquals(collection.getData().getMsg(), "Success");
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "List varchar pk collection", groups = {"Smoke"}, dependsOnMethods = "createCollectionWithVarcharPK")
    public void listVarcharPKCollectionTest() {
        R<ListCollectionsResponse> listCollectionsResponseR = milvusClient.listCollections(ListCollectionsParam.newBuilder().build());
        Assert.assertEquals(listCollectionsResponseR.getStatus().intValue(), R.Status.Success.getCode());
        Assert.assertTrue(listCollectionsResponseR.getData().collectionNames.contains(varcharPKCollection));
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "insert data into varchar PK Collection", groups = {"Smoke"}, dependsOnMethods = "listVarcharPKCollectionTest")
    public void insertIntoVarcharPKCollectionTest() throws InterruptedException {
        List<JSONObject> jsonObjects = CommonFunction.generateVarcharPKDataWithDynamicFiledRow(10000);
        R<InsertResponse> insert = milvusClient.insert(InsertRowsParam.newBuilder()
                .withCollectionName(varcharPKCollection)
                .withRows(jsonObjects)
                .build());
        Thread.sleep(2000);
        Assert.assertEquals(insert.getStatus().intValue(), R.Status.Success.getCode());
        Assert.assertEquals(insert.getData().getInsertCount().intValue(), 10000);
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "search varchar PK Collection", groups = {"Smoke"}, dependsOnMethods = "insertIntoVarcharPKCollectionTest")
    public void searchVarcharPKCollection() {
        List<List<Float>> search_vectors = Collections.singletonList(Arrays.asList(MathUtil.generateFloat(128)));
        R<SearchResponse> search = milvusClient.search(SearchSimpleParam.newBuilder()
                .withCollectionName(varcharPKCollection)
                .withOffset(0L)
                .withLimit(100L)
                .withFilter("book_name>\"StringPK5000\"")
                .withVectors(search_vectors)
                .withOutputFields(Lists.newArrayList("*"))
                .build());
        Assert.assertEquals(search.getStatus().intValue(), R.Status.Success.getCode());
        Assert.assertEquals(search.getData().getRowRecords().size(), 100);
        System.out.println("search:"+search.getData().getRowRecords().toString());
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "query varchar PK Collection", groups = {"Smoke"}, dependsOnMethods = "insertIntoVarcharPKCollectionTest")
    public void queryVarcharPKCollection() {
        R<QueryResponse> query = milvusClient.query(QuerySimpleParam.newBuilder()
                .withCollectionName(varcharPKCollection)
                .withFilter("book_name>\"StringPK5000\"")
                .withLimit(100L)
                .withOffset(0L)
                .withOutputFields(Lists.newArrayList("*"))
                .build());
        Assert.assertEquals(query.getStatus().intValue(), R.Status.Success.getCode());
        Assert.assertEquals(query.getData().getRowRecords().size(), 100);
        System.out.println("query:"+query.getData().getRowRecords().toString());
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "get varchar pk Collection", groups = {"Smoke"}, dependsOnMethods = "insertIntoVarcharPKCollectionTest")
    public void getVarcharPKCollection() {
        R<GetResponse> getResponseR = milvusClient.get(GetIdsParam.newBuilder()
                .withCollectionName(varcharPKCollection)
                .withOutputFields(Lists.newArrayList("*"))
                .withPrimaryIds(Lists.newArrayList("StringPK0", "StringPK1", "StringPK2", "StringPK3", "StringPK4", "StringPK5", "StringPK6", "StringPK7", "StringPK8", "StringPK9"))
                .build());
        Assert.assertEquals(getResponseR.getStatus().intValue(), R.Status.Success.getCode());
        Assert.assertEquals(getResponseR.getData().getRowRecords().size(), 10);
        System.out.println("get:"+getResponseR.getData().getRowRecords().toString());
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "delete varchar PK data ", groups = {"Smoke"}, dependsOnMethods = {"searchVarcharPKCollection", "queryVarcharPKCollection","getVarcharPKCollection"})
    public void deleteVarcharPKData() {
        ArrayList<String> pks = Lists.newArrayList( "StringPK11", "StringPK12", "StringPK13", "StringPK14", "StringPK15", "StringPK16", "StringPK17", "StringPK18", "StringPK19","StringPK20");
        R<DeleteResponse> delete = milvusClient.delete(DeleteIdsParam.newBuilder()
                .withCollectionName(varcharPKCollection)
                .withPrimaryIds(pks)
                .build());
        Assert.assertEquals(delete.getStatus().intValue(), R.Status.Success.getCode());
    }


    @Severity(SeverityLevel.BLOCKER)
    @Test(description = "drop varchar PK collection", groups = {"Smoke"}, dependsOnMethods = {"deleteVarcharPKData"})
    public void dropVarcharPKCollection() {
        R<RpcStatus> rpcStatusR = milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(varcharPKCollection).build());
        Assert.assertEquals(rpcStatusR.getStatus().intValue(), R.Status.Success.getCode());
    }
}
