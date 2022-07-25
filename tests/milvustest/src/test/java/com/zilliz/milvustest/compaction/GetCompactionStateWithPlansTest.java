package com.zilliz.milvustest.compaction;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.CompactionState;
import io.milvus.grpc.GetCompactionPlansResponse;
import io.milvus.grpc.ManualCompactionResponse;
import io.milvus.param.R;
import io.milvus.param.control.GetCompactionPlansParam;
import io.milvus.param.control.ManualCompactParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Epic("Compaction")
@Feature("GetCompactionStateWithPlans")
public class GetCompactionStateWithPlansTest extends BaseTest {
  @DataProvider(name = "provideCompaction")
  public Object[][] provideManualCompaction() {
    R<ManualCompactionResponse> responseR =
        milvusClient.manualCompact(
            ManualCompactParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .build());
    long CompactionId = responseR.getData().getCompactionID();
    return new Object[][] {{CompactionId}};
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Gets compaction state with its plan.", dataProvider = "provideCompaction",groups = {"Smoke"})
  public void getCompactionStateWithPlansTest(long compactionId) {
    R<GetCompactionPlansResponse> responseR =
        milvusClient.getCompactionStateWithPlans(
            GetCompactionPlansParam.newBuilder().withCompactionID(compactionId).build());
    Assert.assertEquals(responseR.getStatus().intValue(), 0);
    Assert.assertEquals(responseR.getData().getState(), CompactionState.Completed);
  }
}
