package com.zilliz.milvustestv2.collection;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.RenameCollectionReq;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import org.testng.Assert;
import org.testng.annotations.*;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 14:24
 */
public class RenameCollectionTest extends BaseTest {
    String newCollectionName;
    @BeforeClass(alwaysRun = true)
    public void providerCollection(){
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
    }

    @Test(description = "Rename collection", groups = {"Smoke"})
    public void renameCollection(){
            String newName="Collection_" + GenerateUtil.getRandomString(10);
        milvusClientV2.renameCollection(RenameCollectionReq.builder()
                .collectionName(newCollectionName)
                .newCollectionName(newName)
                .build());
        ListCollectionsResp listCollectionsResp = milvusClientV2.listCollections();
        Assert.assertTrue(listCollectionsResp.getCollectionNames().contains(newName));
        Assert.assertFalse(listCollectionsResp.getCollectionNames().contains(newCollectionName));
    }

}
