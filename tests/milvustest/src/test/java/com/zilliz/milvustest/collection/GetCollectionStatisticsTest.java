package com.zilliz.milvustest.collection;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.param.R;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.GetCollectionStatisticsParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.response.GetCollStatResponseWrapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

@Epic("Collection")
@Feature("GetCollectionStatistics")
public class GetCollectionStatisticsTest extends BaseTest {

  public String commonCollection;

  @BeforeClass(description = "Create collection before test",alwaysRun=true)
  public void provideCollectionName() {
    String newCollection = CommonFunction.createNewCollection();
    commonCollection = newCollection;
    List<InsertParam.Field> fields = CommonFunction.generateData(2000);
    CommonFunction.insertDataIntoCollection(newCollection,fields);
  }
  @AfterClass(description = "drop collection before test",alwaysRun=true)
  public void dropCollection() {
    milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(commonCollection).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Shows the statistics information of a collection.",groups = {"Smoke"})
  public void getCollectionStatisticsInfo() {
    R<GetCollectionStatisticsResponse> respCollectionStatistics =
        milvusClient
            .getCollectionStatistics( // Return the statistics information of the collection.
                GetCollectionStatisticsParam.newBuilder()
                    .withCollectionName(commonCollection)
                    .withFlush(false)
                    .build());
    Assert.assertEquals(respCollectionStatistics.getStatus().intValue(), 0);
    GetCollStatResponseWrapper wrapperCollectionStatistics =
        new GetCollStatResponseWrapper(respCollectionStatistics.getData());
    Assert.assertEquals(wrapperCollectionStatistics.getRowCount(), 0);
    System.out.println(wrapperCollectionStatistics);
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Shows the statistics information of a collection with flush")
  public void getCollectionStatisticsWithFlushTest() {
    R<GetCollectionStatisticsResponse> respCollectionStatistics =
        milvusClient
            .getCollectionStatistics( // Return the statistics information of the collection.
                GetCollectionStatisticsParam.newBuilder()
                    .withCollectionName(commonCollection)
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
