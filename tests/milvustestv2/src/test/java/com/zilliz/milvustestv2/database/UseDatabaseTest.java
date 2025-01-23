package com.zilliz.milvustestv2.database;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

public class UseDatabaseTest extends BaseTest {

    private static String collectionName = "";

    @BeforeClass(alwaysRun = true)
    public void provideTestData() {
        milvusClientV2.createDatabase(CreateDatabaseReq.builder()
                .databaseName(CommonData.databaseName2).build());
        milvusClientV2.createDatabase(CreateDatabaseReq.builder()
                .databaseName(CommonData.databaseName1).build());

    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        try {
            milvusClientV2.useDatabase(CommonData.databaseName2);
            ListCollectionsResp listCollectionsResp = milvusClientV2.listCollections();
            List<String> collectionNames = listCollectionsResp.getCollectionNames();
            collectionNames.forEach(x->{
                milvusClientV2.dropCollection(DropCollectionReq.builder()
                        .collectionName(x).build());
            });

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        milvusClientV2.dropDatabase(DropDatabaseReq.builder()
                .databaseName(CommonData.databaseName2).build());
        milvusClientV2.dropDatabase(DropDatabaseReq.builder()
                .databaseName(CommonData.databaseName1).build());
        try {
            milvusClientV2.useDatabase("default");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(description = "use database", groups = {"Smoke"})
    public void testUseDatabase() {
        try {
            milvusClientV2.useDatabase(CommonData.databaseName2);
            collectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
            CommonFunction.createIndexAndInsertAndLoad(collectionName, DataType.FloatVector, true, CommonData.numberEntities);
            ListCollectionsResp listCollectionsResp = milvusClientV2.listCollections();
            Assert.assertTrue(listCollectionsResp.getCollectionNames().contains(collectionName));
            // use databaseName1
            milvusClientV2.useDatabase(CommonData.databaseName1);
            ListCollectionsResp listCollectionsResp2 = milvusClientV2.listCollections();
            Assert.assertFalse(listCollectionsResp2.getCollectionNames().contains(collectionName));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(description = "use nonexistent database", groups = {"Smoke"})
    public void testUseNonexistentDatabase(){
        try {
            milvusClientV2.useDatabase("NonexistentDB");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not exist"));
        }
    }


}
