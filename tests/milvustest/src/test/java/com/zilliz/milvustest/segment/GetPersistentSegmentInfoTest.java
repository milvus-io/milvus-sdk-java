package com.zilliz.milvustest.segment;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.GetPersistentSegmentInfoResponse;
import io.milvus.grpc.SegmentState;
import io.milvus.param.R;
import io.milvus.param.collection.FlushParam;
import io.milvus.param.control.GetPersistentSegmentInfoParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

@Epic("Segment")
@Feature("GetPersistentSegmentInfo")
public class GetPersistentSegmentInfoTest extends BaseTest {
  @BeforeClass
  public void flushFirst() {
    milvusClient.flush(
        FlushParam.newBuilder()
            .withCollectionNames(Arrays.asList(CommonData.defaultCollection))
            .build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description =
          "Gets the information of persistent segments from data node, including row count, persistence state")
  public void getPersistentSegmentInfoTest() {
    R<GetPersistentSegmentInfoResponse> responseR =
        milvusClient.getPersistentSegmentInfo(
            GetPersistentSegmentInfoParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .build());
    Assert.assertEquals(responseR.getStatus().intValue(), 0);
    Assert.assertEquals(responseR.getData().getInfos(0).getState(), SegmentState.Flushed);
    System.out.println(responseR.getData());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
          description =
                  "Gets the information of persistent segments without flush")
  public void getPersistentSegmentInfoWithoutFlush() {
    R<GetPersistentSegmentInfoResponse> responseR =
            milvusClient.getPersistentSegmentInfo(
                    GetPersistentSegmentInfoParam.newBuilder()
                            .withCollectionName(CommonData.defaultStringPKCollection)
                            .build());
    Assert.assertEquals(responseR.getStatus().intValue(), 0);
    System.out.println(responseR.getData());
  }
}
