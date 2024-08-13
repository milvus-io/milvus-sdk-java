package com.zilliz.milvustestv2.database;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import io.milvus.v2.service.database.response.ListDatabasesResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class CreateDatabaseTest extends BaseTest {

    @DataProvider(name = "differentCollection")
    public Object[][] providerVectorType() {
        return new Object[][]{
                { DataType.FloatVector},
                { DataType.BinaryVector},
                { DataType.Float16Vector},
                { DataType.BFloat16Vector},
                { DataType.SparseFloatVector},
        };
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropDatabase(DropDatabaseReq.builder().databaseName(CommonData.databaseName).build());
    }

    @Test(description = "Create database",groups = {"Smoke"})
    public void createDatabase(){
        milvusClientV2.createDatabase(CreateDatabaseReq.builder()
                .databaseName(CommonData.databaseName).build());
        ListDatabasesResp listDatabasesResp = milvusClientV2.listDatabases();
        Assert.assertTrue(listDatabasesResp.getDatabaseNames().contains(CommonData.databaseName));
    }

    @Test(description = "Create database with same name repeatedly ",groups = {"Smoke"},dependsOnMethods = {"createDatabase"})
    public void createDatabaseRepeatedly(){
        try {
            milvusClientV2.createDatabase(CreateDatabaseReq.builder()
                    .databaseName(CommonData.databaseName).build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("database already exist"));
        }


    }
}
