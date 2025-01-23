package com.zilliz.milvustestv2.others;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.common.CompactionState;
import io.milvus.v2.service.utility.request.CompactReq;
import io.milvus.v2.service.utility.request.GetCompactionStateReq;
import io.milvus.v2.service.utility.response.CompactResp;
import io.milvus.v2.service.utility.response.GetCompactionStateResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class GetCompactionStateTest extends BaseTest {

    Long compactionID;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        CompactResp compact = milvusClientV2.compact(CompactReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .build());
        compactionID = compact.getCompactionID();
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {

    }

    @Test(description = "get compact state", groups = {"Smoke"})
    public void getCompactState() {
        GetCompactionStateResp compactionState = milvusClientV2.getCompactionState(GetCompactionStateReq.builder()
                .compactionID(compactionID).build());
        Assert.assertEquals(compactionState.getState(), CompactionState.Completed);
    }
}
