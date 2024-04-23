package com.zilliz.milvustest.insert;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.MutationResult;
import io.milvus.param.R;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.DeleteParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.partition.CreatePartitionParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

@Epic("Insert")
@Feature("Delete")
public class DeleteTest extends BaseTest {
  public String commonCollection;
  public String commonPartition;

  @BeforeClass(description = "Create collection before test",alwaysRun = true)
  public void provideCollectionName() {
    commonCollection = CommonFunction.createNewCollection();
    commonPartition = "partition_" + MathUtil.getRandomString(10);
    milvusClient.createPartition(
        CreatePartitionParam.newBuilder()
            .withCollectionName(commonCollection)
            .withPartitionName(commonPartition)
            .build());
    List<InsertParam.Field> fields = CommonFunction.generateData(2000);
    milvusClient.insert(
        InsertParam.newBuilder().withCollectionName(commonCollection).withPartitionName(commonPartition).withFields(fields).build());
  }

  @AfterClass(description = "delete collection after deleteDataTest",alwaysRun = true)
  public void deleteTestData() {
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(commonCollection).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "delete data in expression ",groups = {"Smoke"})
  public void deleteData() {
    R<MutationResult> mutationResultR =
        milvusClient.delete(
            DeleteParam.newBuilder()
                .withCollectionName(commonCollection)
                .withExpr("book_id in [1,2,3]")
                .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
    Assert.assertEquals(mutationResultR.getData().getDeleteCnt(), 3L);
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "delete data in complex expression without load",groups = {"Smoke"})
  public void deleteDataInComplexExpWithUnload() {
    R<MutationResult> mutationResultR =
            milvusClient.delete(
                    DeleteParam.newBuilder()
                            .withCollectionName(commonCollection)
                            .withExpr("book_id <100")
                            .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 101);
    Assert.assertTrue(mutationResultR.getException().getMessage().contains("collection not loaded"));
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "delete data in complex expression with load",groups = {"Smoke"},
  dependsOnMethods = {"deleteDataInComplexExpWithUnload"})
  public void deleteDataInComplexExpWithLoad() {
    CommonFunction.prepareCollectionForSearch(commonCollection,"default");
    R<MutationResult> mutationResultR =
            milvusClient.delete(
                    DeleteParam.newBuilder()
                            .withCollectionName(commonCollection)
                            .withExpr("book_id <100")
                            .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "delete data in expression by partition ")
  public void deleteDataByPartition() {
    R<MutationResult> mutationResultR =
        milvusClient.delete(
            DeleteParam.newBuilder()
                .withCollectionName(commonCollection)
                .withPartitionName(commonPartition)
                .withExpr("book_id in [1,2,3]")
                .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
    Assert.assertEquals(mutationResultR.getData().getDeleteCnt(), 3L);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "delete data with invalid expression ",dependsOnMethods = {"deleteDataInComplexExpWithLoad"})
  public void deleteDataInvalidExpression() {
    R<MutationResult> mutationResultR =
        milvusClient.delete(
            DeleteParam.newBuilder()
                .withCollectionName(commonCollection)
                .withExpr("book_id not in  [1,2,3]")
                .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
  }
}
