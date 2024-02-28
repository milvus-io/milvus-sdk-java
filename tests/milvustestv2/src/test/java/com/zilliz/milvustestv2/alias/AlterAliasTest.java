package com.zilliz.milvustestv2.alias;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.service.collection.request.DropCollectionReq;
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
    String aliasName;
    @BeforeClass(alwaysRun = true)
    public void providerCollection() {
        aliasName= GenerateUtil.getRandomString(10);
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null);
        newCollectionName2 = CommonFunction.createNewCollection(CommonData.dim, null);
        milvusClientV2.createAlias(CreateAliasReq.builder()
                .collectionName(newCollectionName)
                .alias(aliasName)
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName2).build());
        milvusClientV2.dropAlias(DropAliasReq.builder().alias(aliasName).build());
    }

    @Test(description = "Alter alias test",groups = {"Smoke"})
    public void alterAliasTest(){
        milvusClientV2.alterAlias(AlterAliasReq.builder().collectionName(newCollectionName2).alias(aliasName).build());
        DescribeAliasResp describeAliasResp = milvusClientV2.describeAlias(DescribeAliasReq.builder().alias(aliasName).build());
        Assert.assertEquals(describeAliasResp.getCollectionName(),newCollectionName2);

    }
}
