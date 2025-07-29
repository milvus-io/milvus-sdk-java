package com.zilliz.milvustestv2.alias;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.utility.request.CreateAliasReq;
import io.milvus.v2.service.utility.request.DropAliasReq;
import io.milvus.v2.service.utility.request.ListAliasesReq;
import io.milvus.v2.service.utility.response.ListAliasResp;
import io.milvus.v2.service.vector.request.InsertReq;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/26 09:23
 */
public class DropAliasTest extends BaseTest {

    String newCollectionName;
    String aliasName;
    String aliasWithDB;
    String collectionWithDB;
    @BeforeClass(alwaysRun = true)
    public void providerCollection() {
        aliasName= GenerateUtil.getRandomString(10);
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        milvusClientV2.createAlias(CreateAliasReq.builder()
                .collectionName(newCollectionName)
                .alias(aliasName)
                .build());
        aliasWithDB = "db_" + GenerateUtil.getRandomString(10);
        milvusClientV2.createDatabase(CreateDatabaseReq.builder().databaseName(CommonData.databaseName2).build());
        collectionWithDB = CommonFunction.createNewCollectionWithDatabase(CommonData.dim, null, DataType.FloatVector, CommonData.databaseName2);
        milvusClientV2.createAlias(CreateAliasReq.builder().alias(aliasWithDB).collectionName(collectionWithDB).databaseName(CommonData.databaseName2).build());

    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionWithDB).databaseName(CommonData.databaseName2).build());
        milvusClientV2.dropDatabase(DropDatabaseReq.builder().databaseName(CommonData.databaseName2).build());
    }

    @Test(description = "drop alias test",groups = {"Smoke"})
    public void dropAliasTest(){
        milvusClientV2.dropAlias(DropAliasReq.builder().alias(aliasName).build());
        ListAliasResp listAliasResp = milvusClientV2.listAliases(ListAliasesReq.builder().collectionName(newCollectionName).build());
        Assert.assertFalse(listAliasResp.getAlias().contains(aliasName));
    }

    @Test(description = "drop alias with database", groups = {"Smoke"})
    public void dropAliasWithDatabase() {
        milvusClientV2.dropAlias(DropAliasReq.builder().alias(aliasWithDB).databaseName(CommonData.databaseName2).build());
        ListAliasResp listAliasResp = milvusClientV2.listAliases(ListAliasesReq.builder().databaseName(CommonData.databaseName2).collectionName(collectionWithDB).build());
        Assert.assertFalse(listAliasResp.getAlias().contains(aliasWithDB));
    }

}
