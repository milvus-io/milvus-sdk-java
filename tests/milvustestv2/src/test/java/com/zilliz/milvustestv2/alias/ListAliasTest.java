package com.zilliz.milvustestv2.alias;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import io.milvus.v2.service.utility.request.CreateAliasReq;
import io.milvus.v2.service.utility.request.DropAliasReq;
import io.milvus.v2.service.utility.request.ListAliasesReq;
import io.milvus.v2.service.utility.response.ListAliasResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/2/26 09:24
 */
public class ListAliasTest extends BaseTest {

    String aliasWithDB;
    String collectionWithDB;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        aliasWithDB = "db_" + GenerateUtil.getRandomString(10);
        milvusClientV2.createDatabase(CreateDatabaseReq.builder().databaseName(CommonData.databaseName2).build());
        collectionWithDB = CommonFunction.createNewCollectionWithDatabase(CommonData.dim, null, DataType.FloatVector, CommonData.databaseName2);
        milvusClientV2.createAlias(CreateAliasReq.builder().alias(aliasWithDB).collectionName(collectionWithDB).databaseName(CommonData.databaseName2).build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropAlias(DropAliasReq.builder().alias(aliasWithDB).databaseName(CommonData.databaseName2).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionWithDB).databaseName(CommonData.databaseName2).build());
        milvusClientV2.dropDatabase(DropDatabaseReq.builder().databaseName(CommonData.databaseName2).build());
    }


    @Test(description = "List alias",groups = {"Smoke"})
    public void listAlias(){
        ListAliasResp listAliasResp = milvusClientV2.listAliases(ListAliasesReq.builder().collectionName(CommonData.defaultFloatVectorCollection).build());
        Assert.assertTrue(listAliasResp.getAlias().contains(CommonData.alias));
    }

    @Test(description = "List alias with database", groups = {"Smoke"})
    public void listAliasWithDatabase() {
        ListAliasResp listAliasResp = milvusClientV2.listAliases(ListAliasesReq.builder().collectionName(collectionWithDB).databaseName(CommonData.databaseName2).build());
        Assert.assertTrue(listAliasResp.getAlias().contains(aliasWithDB));
    }

}
