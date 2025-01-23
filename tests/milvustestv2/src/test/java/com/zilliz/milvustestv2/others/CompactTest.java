package com.zilliz.milvustestv2.others;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.MathUtil;
import io.milvus.v2.service.utility.request.CompactReq;
import io.milvus.v2.service.utility.response.CompactResp;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class CompactTest extends BaseTest {

    @Test(description = "Compact", groups = {"Smoke"})
    public void compactTest() {
        CompactResp compact = milvusClientV2.compact(CompactReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .build());
        Long compactionID = compact.getCompactionID();
        Assert.assertNotNull(compactionID);

    }

    @Test(description = "Compact not existed collection", groups = {"Smoke"})
    public void compactNotExistedColTest() {
        String collectionName="a"+MathUtil.getRandomString(10);
        try {
            CompactResp compact = milvusClientV2.compact(CompactReq.builder()
                    .collectionName(collectionName)
                    .build());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("can't find"));
        }

    }
}
