package com.zilliz.milvustest.collection;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.param.R;
import io.milvus.param.collection.GetCollectionStatisticsParam;
import io.milvus.response.GetCollStatResponseWrapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

@Epic("Collection")
@Feature("GetCollectionStatistics")
public class GetCollectionStatisticsTest extends BaseTest {

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Shows the statistics information of a collection.",groups = {"Smoke"})
  public void getCollectionStatisticsInfo() {
    R<GetCollectionStatisticsResponse> respCollectionStatistics =
        milvusClient
            .getCollectionStatistics( // Return the statistics information of the collection.
                GetCollectionStatisticsParam.newBuilder()
                    .withCollectionName(CommonData.defaultCollection)
                    .withFlush(false)
                    .build());
    Assert.assertEquals(respCollectionStatistics.getStatus().intValue(), 0);
    GetCollStatResponseWrapper wrapperCollectionStatistics =
        new GetCollStatResponseWrapper(respCollectionStatistics.getData());
    Assert.assertEquals(wrapperCollectionStatistics.getRowCount(), 2000);
    System.out.println(wrapperCollectionStatistics);
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Shows the statistics information of a collection with flush")
  public void getCollectionStatisticsWithFlushTest() {
    R<GetCollectionStatisticsResponse> respCollectionStatistics =
        milvusClient
            .getCollectionStatistics( // Return the statistics information of the collection.
                GetCollectionStatisticsParam.newBuilder()
                    .withCollectionName(CommonData.defaultCollection)
                    .withFlush(true)
                    .build());
    Assert.assertEquals(respCollectionStatistics.getStatus().intValue(), 0);
    GetCollStatResponseWrapper wrapperCollectionStatistics =
        new GetCollStatResponseWrapper(respCollectionStatistics.getData());
    Assert.assertEquals(wrapperCollectionStatistics.getRowCount(), 2000);
    System.out.println(wrapperCollectionStatistics);
  }

  @Severity(SeverityLevel.MINOR)
  @Test(description = "get the statistics information of nonexistent collection.")
  public void getNonexistentCollectionStatisticsInfo() {
    R<GetCollectionStatisticsResponse> respCollectionStatistics =
            milvusClient
                    .getCollectionStatistics( // Return the statistics information of the collection.
                            GetCollectionStatisticsParam.newBuilder()
                                    .withCollectionName("NonexistentCollection")
                                    .withFlush(false)
                                    .build());
    Assert.assertEquals(respCollectionStatistics.getStatus().intValue(), 1);
    Assert.assertTrue(respCollectionStatistics.getException().getMessage().contains("can't find collection"));

  }
}
