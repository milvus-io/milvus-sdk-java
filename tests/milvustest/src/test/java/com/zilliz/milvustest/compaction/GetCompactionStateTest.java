package com.zilliz.milvustest.compaction;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.CompactionState;
import io.milvus.grpc.GetCompactionStateResponse;
import io.milvus.grpc.ManualCompactionResponse;
import io.milvus.param.R;
import io.milvus.param.control.GetCompactionStateParam;
import io.milvus.param.control.ManualCompactParam;
import io.qameta.allure.*;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Epic("Compaction")
@Feature("GetCompactionState")
public class GetCompactionStateTest extends BaseTest {
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
  @Test(description = "Gets the compaction state by id.", dataProvider = "provideCompaction",groups = {"Smoke"})
  public void getCompactionState(long compactionId) {
    R<GetCompactionStateResponse> responseR =
        milvusClient.getCompactionState(
            GetCompactionStateParam.newBuilder().withCompactionID(compactionId).build());
    Assert.assertEquals(responseR.getStatus().intValue(), 0);
    GetCompactionStateResponse data = responseR.getData();
    Assert.assertEquals(data.getState(), CompactionState.Completed);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Gets the compaction state by nonexistent id.")
  @Issue("nonexistent id return success ?")
  public void getCompactionStateWithNonexistentId() {
    R<GetCompactionStateResponse> responseR =
            milvusClient.getCompactionState(
                    GetCompactionStateParam.newBuilder().withCompactionID(1L).build());
    Assert.assertEquals(responseR.getStatus().intValue(), 0);
    GetCompactionStateResponse data = responseR.getData();
    Assert.assertEquals(data.getState(), CompactionState.Completed);
  }
}
