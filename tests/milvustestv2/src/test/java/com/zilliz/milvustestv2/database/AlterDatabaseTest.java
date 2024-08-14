package com.zilliz.milvustestv2.database;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.param.Constant;
import io.milvus.v2.service.database.request.AlterDatabaseReq;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DescribeDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import io.milvus.v2.service.database.response.DescribeDatabaseResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class AlterDatabaseTest extends BaseTest {
    @BeforeClass(alwaysRun = true)
    public void initTestData(){
        milvusClientV2.createDatabase(CreateDatabaseReq.builder().databaseName(CommonData.databaseName).build());
    }
    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropDatabase(DropDatabaseReq.builder().databaseName(CommonData.databaseName).build());
    }

    @Test(description = "alter database",groups = {"Smoke"})
    public void alterDatabase(){
        Map<String,String> map1=new HashMap<>();
        map1.put(Constant.DATABASE_REPLICA_NUMBER,"2");
        milvusClientV2.alterDatabase(AlterDatabaseReq.builder()
                .databaseName(CommonData.databaseName)
                .properties(map1).build());
        DescribeDatabaseResp describeDatabaseResp = milvusClientV2.describeDatabase(DescribeDatabaseReq.builder()
                .databaseName(CommonData.databaseName).build());
        Assert.assertEquals(describeDatabaseResp.getDatabaseName(),CommonData.databaseName);
        Assert.assertEquals(describeDatabaseResp.getProperties().get(Constant.DATABASE_REPLICA_NUMBER),"2");
    }
}
