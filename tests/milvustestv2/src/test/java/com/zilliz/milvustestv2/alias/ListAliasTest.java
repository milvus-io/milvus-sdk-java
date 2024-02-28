package com.zilliz.milvustestv2.alias;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.utility.request.ListAliasesReq;
import io.milvus.v2.service.utility.response.ListAliasResp;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/2/26 09:24
 */
public class ListAliasTest extends BaseTest {

    @Test(description = "List alias",groups = {"Smoke"})
    public void listAlias(){
        ListAliasResp listAliasResp = milvusClientV2.listAliases(ListAliasesReq.builder().collectionName(CommonData.defaultFloatVectorCollection).build());
        Assert.assertTrue(listAliasResp.getAlias().contains(CommonData.alias));
    }
}
