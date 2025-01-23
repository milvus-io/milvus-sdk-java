package com.zilliz.milvustestv2.database;

import com.zilliz.milvustestv2.common.BaseTest;
import io.milvus.v2.service.database.response.ListDatabasesResp;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ListDatabaseTest extends BaseTest {
    @Test(description = "list database", groups = {"Smoke"})
    public void listDatabase() {
        ListDatabasesResp listDatabasesResp = milvusClientV2.listDatabases();
        Assert.assertTrue(listDatabasesResp.getDatabaseNames().contains("default"));
    }


}
