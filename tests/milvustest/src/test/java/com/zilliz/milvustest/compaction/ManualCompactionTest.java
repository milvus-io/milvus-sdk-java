package com.zilliz.milvustest.compaction;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.grpc.CompactionState;
import io.milvus.grpc.GetCompactionPlansResponse;
import io.milvus.grpc.GetCompactionStateResponse;
import io.milvus.grpc.ManualCompactionResponse;
import io.milvus.param.R;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FlushParam;
import io.milvus.param.control.GetCompactionPlansParam;
import io.milvus.param.control.GetCompactionStateParam;
import io.milvus.param.control.ManualCompactParam;
import io.milvus.param.dml.DeleteParam;
import io.milvus.param.dml.InsertParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

@Epic("Compaction")
@Feature("ManualCompaction")
public class ManualCompactionTest extends BaseTest {
  public String collection;

  @BeforeClass(alwaysRun = true)
  public void providerData() {
    collection = CommonFunction.createNewCollection();
    List<InsertParam.Field> fields = CommonFunction.generateData(10000);
    milvusClient.insert(
        InsertParam.newBuilder().withCollectionName(collection).withFields(fields).build());
    milvusClient.flush(
        FlushParam.newBuilder().withCollectionNames(Arrays.asList(collection)).build());
    List<InsertParam.Field> fields2 = CommonFunction.generateData(20000);
    milvusClient.insert(
        InsertParam.newBuilder().withCollectionName(collection).withFields(fields2).build());
    milvusClient.flush(
        FlushParam.newBuilder().withCollectionNames(Arrays.asList(collection)).build());
  }

  @AfterClass(alwaysRun = true)
  public void dropCollection() {
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(collection).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "performs a manual compaction.",
      groups = {"Smoke"},enabled = false)
  public void manualCompactionTest() {
    R<ManualCompactionResponse> responseR =
        milvusClient.manualCompact(
            ManualCompactParam.newBuilder().withCollectionName(collection).build());
    Assert.assertEquals(responseR.getStatus().intValue(), 0);
    Assert.assertTrue(responseR.getData().getCompactionID() > 0);
    long compactionID = responseR.getData().getCompactionID();
    R<GetCompactionStateResponse> GetCompactionStateResponse =
        milvusClient.getCompactionState(
            GetCompactionStateParam.newBuilder().withCompactionID(compactionID).build());
    Assert.assertEquals(GetCompactionStateResponse.getStatus().intValue(), 0);
    Assert.assertTrue(GetCompactionStateResponse.getData().getState().equals(CompactionState.Executing)||
            GetCompactionStateResponse.getData().getState().equals(CompactionState.Completed));
   }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "manual compaction with nonexistent collection")
  public void manualCompactionWithNonexistentCollection(){
    R<ManualCompactionResponse> responseR =
            milvusClient.manualCompact(
                    ManualCompactParam.newBuilder()
                            .withCollectionName("NonexistentCollection")
                            .build());
    Assert.assertEquals(responseR.getStatus().intValue(), 1);
    Assert.assertTrue(responseR.getException().getMessage().contains("can't find collection"));
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "performs a manual compaction with data delete")
  public void manualCompactionAfterDelete(){
    milvusClient.delete(
            DeleteParam.newBuilder()
                    .withCollectionName(collection)
                    .withExpr("book_id in [1,2,3]")
                    .build());
    R<ManualCompactionResponse> responseR =
            milvusClient.manualCompact(
                    ManualCompactParam.newBuilder()
                            .withCollectionName(collection)
                            .build());
    Assert.assertEquals(responseR.getStatus().intValue(), 0);
    Assert.assertTrue(responseR.getData().getCompactionID() > 0);
    long compactionID = responseR.getData().getCompactionID();
    R<GetCompactionStateResponse> GetCompactionStateResponse =
            milvusClient.getCompactionState(
                    GetCompactionStateParam.newBuilder().withCompactionID(compactionID).build());
    Assert.assertEquals(GetCompactionStateResponse.getStatus().intValue(), 0);
    Assert.assertEquals(GetCompactionStateResponse.getData().getState(), CompactionState.Executing);
    R<GetCompactionPlansResponse> GetCompactionPlansResponse =
            milvusClient.getCompactionStateWithPlans(
                    GetCompactionPlansParam.newBuilder().withCompactionID(compactionID).build());
    Assert.assertEquals(GetCompactionPlansResponse.getStatus().intValue(), 0);
    Assert.assertEquals(GetCompactionPlansResponse.getData().getState(),CompactionState.Executing);
  }
}
