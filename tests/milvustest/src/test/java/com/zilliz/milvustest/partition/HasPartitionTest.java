package com.zilliz.milvustest.partition;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.param.R;
import io.milvus.param.partition.CreatePartitionParam;
import io.milvus.param.partition.DropPartitionParam;
import io.milvus.param.partition.HasPartitionParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Epic("Partition")
@Feature("HasPartition")
public class HasPartitionTest extends BaseTest {
  private String partition;

  @BeforeClass(description = "init partition Name",alwaysRun = true)
  public void createPartitionTest() {
    partition = "partition_" + MathUtil.getRandomString(10);
    milvusClient.createPartition(
        CreatePartitionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionName(partition)
            .build());
  }

  @AfterClass(description = "delete partition after test",alwaysRun = true)
  public void deletePartition() {
    milvusClient.dropPartition(
        DropPartitionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionName(partition)
            .build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Check partition",groups = {"Smoke"})
  public void hasPartitionTest1() {
    R<Boolean> booleanR =
        milvusClient.hasPartition(
            HasPartitionParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionName(partition)
                .build());
    Assert.assertEquals(booleanR.getStatus().intValue(), 0);
    Assert.assertTrue(booleanR.getData());
  }

  @Severity(SeverityLevel.MINOR)
  @Test(description = "Check nonexistent partition")
  public void hasPartitionWithNonexistentName() {
    R<Boolean> booleanR =
        milvusClient.hasPartition(
            HasPartitionParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionName(MathUtil.getRandomString(10))
                .build());
    Assert.assertEquals(booleanR.getStatus().intValue(), 0);
    Assert.assertFalse(booleanR.getData());
  }
}
