package com.zilliz.milvustest.collection;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.RenameCollectionParam;
import io.milvus.param.highlevel.collection.ListCollectionsParam;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2023/10/13 11:32
 */
@Epic("Collection")
@Feature("RenameCollection")
public class RenameCollectionTest extends BaseTest {
    private String rnCollection;
    private String newName;

    @BeforeTest(alwaysRun = true)
    public void initData(){
        rnCollection= CommonFunction.createNewCollection();
        newName = "collection_" + MathUtil.getRandomString(10);

    }

    @AfterClass(description = "delete test data after CreateCollectionTest", alwaysRun = true)
    public void deleteTestData() {
        if (rnCollection != null) {
            milvusClient.dropCollection(
                    DropCollectionParam.newBuilder().withCollectionName(newName).build());
        }
    }

    @Severity(SeverityLevel.BLOCKER)
    @Test(
            description = "Rename collection",
            groups = {"Smoke"})
    public void renameCollectionSuccess() {
        R<ListCollectionsResponse> listCollectionsResponseR = milvusClient.listCollections(ListCollectionsParam.newBuilder().build());
        logger.info("List0:"+listCollectionsResponseR.getData().collectionNames);
        logger.info("rnCollection:"+rnCollection);
        logger.info("newCollection:"+newName);
        Assert.assertTrue(listCollectionsResponseR.getData().collectionNames.contains(rnCollection));
        R<RpcStatus> rpcStatusR = milvusClient.renameCollection(RenameCollectionParam.newBuilder()
                .withOldCollectionName(rnCollection)
                .withNewCollectionName(newName)
                .build());
        Assert.assertEquals(rpcStatusR.getStatus().intValue(),0);
        R<ListCollectionsResponse> listCollectionsResponseR1 = milvusClient.listCollections(ListCollectionsParam.newBuilder().build());
        logger.info("List1:"+listCollectionsResponseR1.getData().collectionNames);
        Assert.assertFalse(listCollectionsResponseR1.getData().collectionNames.contains(rnCollection));
        Assert.assertTrue(listCollectionsResponseR1.getData().collectionNames.contains(newName));
    }

}
