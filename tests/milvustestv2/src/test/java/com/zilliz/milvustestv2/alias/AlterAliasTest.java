package com.zilliz.milvustestv2.alias;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import io.milvus.v2.service.utility.request.AlterAliasReq;
import io.milvus.v2.service.utility.request.CreateAliasReq;
import io.milvus.v2.service.utility.request.DescribeAliasReq;
import io.milvus.v2.service.utility.request.DropAliasReq;
import io.milvus.v2.service.utility.response.DescribeAliasResp;
import io.milvus.v2.service.utility.response.ListAliasResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/2/26 09:24
 */
public class AlterAliasTest extends BaseTest {
    String newCollectionName;
    String newCollectionName2;
    String newCollectionNameWithDB;
    String aliasName;
    String aliasName2;

    @BeforeClass(alwaysRun = true)
    public void providerCollection() {
        aliasName= GenerateUtil.getRandomString(10);
        aliasName2= GenerateUtil.getRandomString(10);
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        newCollectionName2 = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        milvusClientV2.createAlias(CreateAliasReq.builder()
                .collectionName(newCollectionName2)
                .alias(aliasName)
                .build());

        milvusClientV2.createDatabase(CreateDatabaseReq.builder().databaseName(CommonData.databaseName2).build());
        newCollectionNameWithDB = CommonFunction.createNewCollectionWithDatabase(CommonData.dim, null, DataType.FloatVector, CommonData.databaseName2);
        milvusClientV2.createAlias(CreateAliasReq.builder()
                .collectionName(newCollectionNameWithDB)
                .alias(aliasName2)
                .databaseName(CommonData.databaseName2)
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
        // remove alias before drop collection
        milvusClientV2.dropAlias(DropAliasReq.builder().alias(aliasName).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName2).build());
        milvusClientV2.dropAlias(DropAliasReq.builder().alias(aliasName2).databaseName(CommonData.databaseName2).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionNameWithDB).databaseName(CommonData.databaseName2).build());
        milvusClientV2.dropDatabase(DropDatabaseReq.builder().databaseName(CommonData.databaseName2).build());
    }

    @Test(description = "Alter alias test",groups = {"Smoke"})
    public void alterAliasTest(){
        milvusClientV2.alterAlias(AlterAliasReq.builder().collectionName(newCollectionName2).alias(aliasName).build());
        DescribeAliasResp describeAliasResp = milvusClientV2.describeAlias(DescribeAliasReq.builder().alias(aliasName).build());
        Assert.assertEquals(describeAliasResp.getCollectionName(),newCollectionName2);
    }

    @Test(description = "Alter alias test with default database",groups = {"Smoke"})
    public void alterAliasWithoutDatabase(){
        try {
            milvusClientV2.alterAlias(AlterAliasReq.builder().collectionName(newCollectionNameWithDB)
                    .alias(aliasName2)
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("collection not found"));
        }
    }

    @Test(description = "Alter alias test with correct database",groups = {"Smoke"})
    public void alterAliasWithCorrectDatabase(){
            milvusClientV2.alterAlias(AlterAliasReq.builder().collectionName(newCollectionNameWithDB)
                    .alias(aliasName2)
                    .databaseName(CommonData.databaseName2)
                    .build());
        DescribeAliasResp describeAliasResp = milvusClientV2.describeAlias(DescribeAliasReq.builder().alias(aliasName2).databaseName(CommonData.databaseName2).build());
        Assert.assertEquals(describeAliasResp.getCollectionName(),newCollectionNameWithDB);
    }

}
