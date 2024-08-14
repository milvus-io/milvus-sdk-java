package com.zilliz.milvustestv2.database;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DescribeDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import io.milvus.v2.service.database.response.DescribeDatabaseResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DescribeDatabaseTest extends BaseTest {

    @BeforeClass(alwaysRun = true)
    public void initTestData(){
        milvusClientV2.createDatabase(CreateDatabaseReq.builder().databaseName(CommonData.databaseName).build());
    }


    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropDatabase(DropDatabaseReq.builder().databaseName(CommonData.databaseName).build());
    }

    @Test(description = "describe database",groups = {"Smoke"})
    public void describeDatabase(){
        DescribeDatabaseResp describeDatabaseResp = milvusClientV2.describeDatabase(DescribeDatabaseReq.builder()
                .databaseName(CommonData.databaseName).build());
        Assert.assertEquals(describeDatabaseResp.getDatabaseName(),CommonData.databaseName);
    }
}
