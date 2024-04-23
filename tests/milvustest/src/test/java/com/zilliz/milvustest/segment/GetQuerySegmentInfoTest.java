package com.zilliz.milvustest.segment;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.GetQuerySegmentInfoResponse;
import io.milvus.param.R;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ReleaseCollectionParam;
import io.milvus.param.control.GetQuerySegmentInfoParam;
import io.qameta.allure.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Epic("Segment")
@Feature("GetQuerySegmentInfo")
public class GetQuerySegmentInfoTest extends BaseTest {
  @BeforeClass(alwaysRun = true)
  public void LoadFirst() {
    milvusClient.loadCollection(
        LoadCollectionParam.newBuilder().withCollectionName(CommonData.defaultCollection).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Issue("https://github.com/milvus-io/milvus-sdk-java/issues/322")
  @Test(
      description =
          "Gets the query information of segments in a collection from query node, including row count,"
          ,groups = {"Smoke"})
  public void getQuerySegmentInfoTest() {
    R<GetQuerySegmentInfoResponse> responseR =
        milvusClient.getQuerySegmentInfo(
            GetQuerySegmentInfoParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .build());
    System.out.println(responseR.getData());
    Assert.assertEquals(responseR.getStatus().intValue(), 0);
//    Assert.assertTrue(responseR.getData().getInfos(0).getNumRows()>1);
    Assert.assertTrue(responseR.getData().getInfosCount()>=2);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Gets the query information of segments without load")
  public void getQuerySegmentInfoWithoutLoad() {
    milvusClient.releaseCollection(
        ReleaseCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .build());
    R<GetQuerySegmentInfoResponse> responseR =
        milvusClient.getQuerySegmentInfo(
            GetQuerySegmentInfoParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .build());
    System.out.println(responseR.getData());
    Assert.assertEquals(responseR.getStatus().intValue(), 0);
    Assert.assertEquals(responseR.getData().getInfosCount(),0);
  }
}
