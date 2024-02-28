package com.zilliz.milvustestv2.alias;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.utility.request.DescribeAliasReq;
import io.milvus.v2.service.utility.response.DescribeAliasResp;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/2/26 09:24
 */
public class DescribeAliasTest extends BaseTest {


    @Test(description = "Describe alias",groups = {"Smoke"})
    public void describeAlias(){
        DescribeAliasResp describeAliasResp = milvusClientV2.describeAlias(DescribeAliasReq.builder().alias(CommonData.alias).build());
        Assert.assertEquals(describeAliasResp.getCollectionName(),CommonData.defaultFloatVectorCollection);
    }
}
