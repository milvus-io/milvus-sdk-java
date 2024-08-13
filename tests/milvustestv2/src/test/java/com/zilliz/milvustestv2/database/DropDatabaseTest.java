package com.zilliz.milvustestv2.database;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DescribeDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import io.milvus.v2.service.database.response.DescribeDatabaseResp;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DropDatabaseTest extends BaseTest {
    @BeforeClass(alwaysRun = true)
    public void initTestData(){
        milvusClientV2.createDatabase(CreateDatabaseReq.builder().databaseName(CommonData.databaseName).build());
    }

    @Test(description = "drop database",groups = {"Smoke"})
    public void dropDatabase(){
        milvusClientV2.dropDatabase(DropDatabaseReq.builder().databaseName(CommonData.databaseName).build());
        try {
            DescribeDatabaseResp describeDatabaseResp = milvusClientV2.describeDatabase(DescribeDatabaseReq.builder()
                    .databaseName(CommonData.databaseName).build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("database not found"));
        }
    }

}
