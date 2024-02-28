package com.zilliz.milvustestv2.collection;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 11:11
 */
public class DescribeCollectionTest extends BaseTest {

    @Test(description = "Describe collection", groups = {"Smoke"})
    public void describeCollection() {
        DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .build());
        Assert.assertEquals(describeCollectionResp.getVectorFieldName().get(0),CommonData.fieldFloatVector);
    }
}
