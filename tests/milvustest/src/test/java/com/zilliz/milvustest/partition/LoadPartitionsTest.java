package com.zilliz.milvustest.partition;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.partition.CreatePartitionParam;
import io.milvus.param.partition.DropPartitionParam;
import io.milvus.param.partition.LoadPartitionsParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
@Epic("Partition")
@Feature("LoadPartitions")
public class LoadPartitionsTest extends BaseTest {
  private String partition;

  @BeforeClass(description = "init partition Name")
  public void createPartitionTest() {
    partition = "partition_" + MathUtil.getRandomString(10);
    milvusClient.createPartition(
        CreatePartitionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionName(partition)
            .build());
  }

  @AfterClass(description = "delete partition after test")
  public void deletePartition() {
    milvusClient.dropPartition(
        DropPartitionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionName(partition)
            .build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "load partition")
  public void loadPartitionSuccess() {
    List<String> partitionNames =
        new ArrayList<String>() {
          {
            add(partition);
          }
        };
    R<RpcStatus> rpcStatusR =
        milvusClient.loadPartitions(
            LoadPartitionsParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionNames(partitionNames)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "load nonexistent partition")
  public void loadNonexistentPartition() {
    String nonexsitPartition = "partition_" + MathUtil.getRandomString(10);
    List<String> partitionNames =
        new ArrayList<String>() {
          {
            add(nonexsitPartition);
          }
        };
    R<RpcStatus> rpcStatusR =
        milvusClient.loadPartitions(
            LoadPartitionsParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionNames(partitionNames)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
    Assert.assertEquals(
        rpcStatusR.getException().getMessage(),
        "partitionID of partitionName:" + nonexsitPartition + " can not be find");
  }
}
