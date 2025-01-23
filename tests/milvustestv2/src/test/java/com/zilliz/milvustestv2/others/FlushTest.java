package com.zilliz.milvustestv2.others;

import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.MathUtil;
import io.milvus.v2.service.utility.request.FlushReq;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Random;

public class FlushTest extends BaseTest {

    @Test(description = "flush", groups = {"Smoke"})
    public void flushTest() {
        milvusClientV2.flush(FlushReq.builder()
                .collectionNames(Lists.newArrayList(CommonData.defaultFloatVectorCollection))
                .waitFlushedTimeoutMs(60 * 1000L)
                .build());
    }

    @Test(description = "flush not existed collection", groups = {"Smoke"})
    public void flushNotExistedCollectionTest() {
        try {
            milvusClientV2.flush(FlushReq.builder()
                    .collectionNames(Lists.newArrayList("a"+MathUtil.getRandomString(5)))
                    .waitFlushedTimeoutMs(60 * 1000L)
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("collection not found"));
        }
    }
}
