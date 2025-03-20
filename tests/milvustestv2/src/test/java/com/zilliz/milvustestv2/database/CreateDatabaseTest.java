package com.zilliz.milvustestv2.database;

import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.MathUtil;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import io.milvus.v2.service.database.response.ListDatabasesResp;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.junit.platform.commons.util.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;


public class CreateDatabaseTest extends BaseTest {

    @DataProvider(name = "differentCollection")
    public Object[][] providerVectorType() {
        return new Object[][]{
                {DataType.FloatVector},
                {DataType.BinaryVector},
                {DataType.Float16Vector},
                {DataType.BFloat16Vector},
                {DataType.SparseFloatVector},
        };
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        try {
            milvusClientV2.useDatabase(CommonData.databaseName);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        ListCollectionsResp listCollectionsResp = milvusClientV2.listCollections();
        for (String collectionName : listCollectionsResp.getCollectionNames()) {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
        milvusClientV2.dropDatabase(DropDatabaseReq.builder().databaseName(CommonData.databaseName).build());
    }

    @Test(description = "Create database", groups = {"Smoke"})
    public void createDatabase() {
        milvusClientV2.createDatabase(CreateDatabaseReq.builder()
                .databaseName(CommonData.databaseName).build());
        ListDatabasesResp listDatabasesResp = milvusClientV2.listDatabases();
        Assert.assertTrue(listDatabasesResp.getDatabaseNames().contains(CommonData.databaseName));
    }

    @Test(description = "Create database with same name repeatedly ", groups = {"Smoke"}, dependsOnMethods = {"createDatabase"})
    public void createDatabaseRepeatedly() {
        try {
            milvusClientV2.createDatabase(CreateDatabaseReq.builder()
                    .databaseName(CommonData.databaseName).build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("database already exist"));
        }


    }

    @Test(description = "Search in database", groups = {"Smoke"}, dependsOnMethods = {"createDatabase"})
    public void searchInDatabase() {
        String collectionName = "a" + MathUtil.getRandomString(10);
        CreateCollectionReq.CollectionSchema collectionSchema = CommonFunction.providerCollectionSchema(CommonData.dim, DataType.FloatVector);
        milvusClientV2.createCollection(CreateCollectionReq.builder().collectionSchema(collectionSchema)
                .numShards(1)
                .databaseName(CommonData.databaseName)
                .collectionName(collectionName)
                .build());
        // use db
        try {
            milvusClientV2.useDatabase(CommonData.databaseName);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        CommonFunction.createIndexAndInsertAndLoad(collectionName, DataType.FloatVector, true, CommonData.numberEntities);
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .databaseName(CommonData.databaseName)
                .data(data)
                .topK(10)
                .build());

        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), 10);


        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());


        SearchResp search2 = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .databaseName("default")
                .data(data)
                .topK(10)
                .build());
        Assert.assertEquals(search2.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search2.getSearchResults().get(0).size(), 10);

        // 不指定databaseName
        try {
            SearchResp search3 = milvusClientV2.search(SearchReq.builder()
                    .collectionName(CommonData.defaultFloatVectorCollection)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .annsField(CommonData.fieldFloatVector)
                    .data(data)
                    .topK(10)
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("collection not found"));
        }

    }

    @Test(description = "Query in database", groups = {"Smoke"}, dependsOnMethods = {"createDatabase"})
    public void queryInDatabase() {
        String collectionName = "a" + MathUtil.getRandomString(10);
        CreateCollectionReq.CollectionSchema collectionSchema = CommonFunction.providerCollectionSchema(CommonData.dim, DataType.FloatVector);
        milvusClientV2.createCollection(CreateCollectionReq.builder().collectionSchema(collectionSchema)
                .numShards(1)
                .databaseName(CommonData.databaseName)
                .collectionName(collectionName)
                .build());
        // use database
        try {
            milvusClientV2.useDatabase(CommonData.databaseName);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        CommonFunction.createIndexAndInsertAndLoad(collectionName, DataType.FloatVector, true, CommonData.numberEntities);
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Lists.newArrayList("*"))
                .ids(Lists.newArrayList(1, 2, 3, 4))
                .databaseName(CommonData.databaseName)
                .build());
        Assert.assertEquals(query.getQueryResults().size(), 4);
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());

        QueryResp query2 = milvusClientV2.query(QueryReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Lists.newArrayList("*"))
                .ids(Lists.newArrayList(1, 2, 3, 4))
                .databaseName("default")
                .build());
        Assert.assertEquals(query2.getQueryResults().size(), 4);

        // 不指定databaseName
        try {
            QueryResp query3 = milvusClientV2.query(QueryReq.builder()
                    .collectionName(CommonData.defaultFloatVectorCollection)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .outputFields(Lists.newArrayList("*"))
                    .ids(Lists.newArrayList(1, 2, 3, 4))
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("can't find collection"));
        }
    }

}
