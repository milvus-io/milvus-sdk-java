package com.zilliz.milvustest.partition;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.partition.CreatePartitionParam;
import io.milvus.param.partition.DropPartitionParam;
import io.milvus.param.partition.LoadPartitionsParam;
import io.milvus.param.partition.ReleasePartitionsParam;
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
  private String collection;
  private String partition;
  private String partition2;

  @BeforeClass(description = "init partition Name",alwaysRun = true)
  public void createPartitionTest() {
    collection= CommonFunction.createNewCollection();
    partition = "partition_" + MathUtil.getRandomString(10);
    partition2 = "partition_" + MathUtil.getRandomString(10);
    milvusClient.createPartition(
        CreatePartitionParam.newBuilder()
            .withCollectionName(collection)
            .withPartitionName(partition)
            .build());
    milvusClient.createPartition(
            CreatePartitionParam.newBuilder()
                    .withCollectionName(collection)
                    .withPartitionName(partition2)
                    .build());
  }

  @AfterClass(description = "delete partition after test",alwaysRun = true)
  public void deletePartition() {
    milvusClient.dropPartition(
        DropPartitionParam.newBuilder()
            .withCollectionName(collection)
            .withPartitionName(partition)
            .build());
    milvusClient.dropPartition(
            DropPartitionParam.newBuilder()
                    .withCollectionName(collection)
                    .withPartitionName(partition2)
                    .build());
    milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(collection).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "load partition",groups = {"Smoke"})
  public void loadPartition() {
    List<String> partitionNames =
        new ArrayList<String>() {
          {
            add(partition);
            add(partition2);
          }
        };
    R<RpcStatus> rpcStatusR =
        milvusClient.loadPartitions(
            LoadPartitionsParam.newBuilder()
                .withCollectionName(collection)
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
                .withCollectionName(collection)
                .withPartitionNames(partitionNames)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
    Assert.assertEquals(
        rpcStatusR.getException().getMessage(),
        "partitionID of partitionName:" + nonexsitPartition + " can not be find");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "load partition after release")
  public void loadPartitionAfterRelease() {
    // release first
    milvusClient.releasePartitions(
        ReleasePartitionsParam.newBuilder()
            .withCollectionName(collection)
            .addPartitionName(partition)
            .build());
    milvusClient.releasePartitions(
            ReleasePartitionsParam.newBuilder()
                    .withCollectionName(collection)
                    .addPartitionName(partition2)
                    .build());
    // load
    List<String> partitionNames =
        new ArrayList<String>() {
          {
            add(partition);
          }
        };
    R<RpcStatus> rpcStatusR =
        milvusClient.loadPartitions(
            LoadPartitionsParam.newBuilder()
                .withCollectionName(collection)
                .withPartitionNames(partitionNames)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }
}
