package com.zilliz.milvustestv2.collection;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 14:22
 */
public class HasCollectionTest extends BaseTest {
    @Test(description = "Has collection", groups = {"Smoke"})
    public void hasCollection(){
        Boolean aBoolean = milvusClientV2.hasCollection(HasCollectionReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .build());
        Assert.assertTrue(aBoolean);
    }
}
