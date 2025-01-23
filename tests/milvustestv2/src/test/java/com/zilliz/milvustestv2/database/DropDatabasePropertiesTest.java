package com.zilliz.milvustestv2.database;

import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.param.Constant;
import io.milvus.v2.service.database.request.*;
import io.milvus.v2.service.database.response.DescribeDatabaseResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DropDatabasePropertiesTest extends BaseTest {

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        milvusClientV2.createDatabase(CreateDatabaseReq.builder().databaseName(CommonData.databaseName).build());
        milvusClientV2.alterDatabaseProperties(AlterDatabasePropertiesReq.builder()
                .databaseName(CommonData.databaseName)
                .property(Constant.DATABASE_REPLICA_NUMBER, "2")
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropDatabase(DropDatabaseReq.builder().databaseName(CommonData.databaseName).build());
    }


    @Test(description = "drop database properties", groups = {"Smoke"})
    public void dropDatabaseProperties() {
        milvusClientV2.dropDatabaseProperties(DropDatabasePropertiesReq.builder()
                .databaseName(CommonData.databaseName)
                .propertyKeys(Lists.newArrayList(Constant.DATABASE_REPLICA_NUMBER))
                .build());
        DescribeDatabaseResp describeDatabaseResp = milvusClientV2.describeDatabase(DescribeDatabaseReq.builder()
                .databaseName(CommonData.databaseName).build());
        Assert.assertEquals(describeDatabaseResp.getDatabaseName(), CommonData.databaseName);
        Assert.assertNull(describeDatabaseResp.getProperties().get(Constant.DATABASE_REPLICA_NUMBER));
    }

    @Test(description = "drop database not existed properties", groups = {"L2"})
    public void dropDatabaseNotExistedProperties() {
        milvusClientV2.dropDatabaseProperties(DropDatabasePropertiesReq.builder()
                .databaseName(CommonData.databaseName)
                .propertyKeys(Lists.newArrayList("not existed"))
                .build());
    }
}
