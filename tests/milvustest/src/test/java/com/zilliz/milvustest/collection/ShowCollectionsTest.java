package com.zilliz.milvustest.collection;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.param.R;
import io.milvus.param.collection.ShowCollectionsParam;
import io.milvus.response.ShowCollResponseWrapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

@Epic("Collection")
@Feature("ShowCollection")
public class ShowCollectionsTest extends BaseTest {

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Show collections list",groups = {"Smoke"})
  public void showCollectionTest1() {
    R<ShowCollectionsResponse> respShowCollections =
        milvusClient.showCollections(ShowCollectionsParam.newBuilder().build());
    Assert.assertEquals(respShowCollections.getStatus().intValue(), 0);
    ShowCollResponseWrapper showCollResponseWrapper =
        new ShowCollResponseWrapper(respShowCollections.getData());
    System.out.println(showCollResponseWrapper);
    Assert.assertTrue(showCollResponseWrapper.getCollectionsInfo().size() >= 1);
    Assert.assertEquals(
        showCollResponseWrapper.getCollectionInfoByName(CommonData.defaultCollection).getName(),
        CommonData.defaultCollection);
  }
}
