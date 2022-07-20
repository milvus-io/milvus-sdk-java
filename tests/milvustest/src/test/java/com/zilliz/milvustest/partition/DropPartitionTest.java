package com.zilliz.milvustest.partition;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.GetPartitionStatisticsResponse;
import io.milvus.grpc.QueryResults;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.partition.*;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Epic("Partition")
@Feature("DropPartition")
public class DropPartitionTest extends BaseTest {
  public String partition;

  @DataProvider(name = "partitionName")
  public Object[][] createPartitionTest() {
    partition = "partition_" + MathUtil.getRandomString(10);
    milvusClient.createPartition(
        CreatePartitionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionName(partition)
            .build());
    return new Object[][] {{partition}};
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "drop partition", dataProvider = "partitionName")
  public void dropPartition(String partitionName) {
    R<RpcStatus> rpcStatusR =
        milvusClient.dropPartition(
            DropPartitionParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionName(partitionName)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Test(description = "query float vector from partition ", dependsOnMethods = "dropPartition")
  @Severity(SeverityLevel.NORMAL)
  public void queryFromDroppedPartition() {
    milvusClient.loadCollection(
        LoadCollectionParam.newBuilder().withCollectionName(CommonData.defaultCollection).build());
    String SEARCH_PARAM = "book_id in [2,4,6,8]";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    List<String> partitions =
        new ArrayList<String>() {
          {
            add(partition);
          }
        };
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionNames(partitions)
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 1);
    Assert.assertEquals(
        queryResultsR.getException().getMessage(), "partition name " + partition + " not found");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "drop nonexistent partition")
  public void dropNonexistentPartition() {
    R<RpcStatus> rpcStatusR =
        milvusClient.dropPartition(
            DropPartitionParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionName("NonexistentPartition")
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
    Assert.assertEquals(
        rpcStatusR.getException().getMessage(),
        "DropPartition failed: partition NonexistentPartition does not exist");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "create partition after drop partition ", dependsOnMethods = "dropPartition")
  public void dropPartitionAfterDropped() {
    CreatePartitionParam createPartitionParam =
        CreatePartitionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionName(partition)
            .build();
    R<RpcStatus> rpcStatusR = milvusClient.createPartition(createPartitionParam);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
    milvusClient.dropPartition(
        DropPartitionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionName(partition)
            .build());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "release partition after drop partition ", dependsOnMethods = "dropPartition")
  public void releasePartitionAfterDropped() {
    R<RpcStatus> rpcStatusR =
        milvusClient.releasePartitions(
            ReleasePartitionsParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionNames(Arrays.asList(partition))
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
    Assert.assertTrue(
        rpcStatusR
            .getException()
            .getMessage()
            .contains("partitionID of partitionName:" + partition + " can not be find"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "load partition after drop partition ", dependsOnMethods = "dropPartition")
  public void loadPartitionAfterDropped() {
    R<RpcStatus> rpcStatusR =
        milvusClient.loadPartitions(
            LoadPartitionsParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionNames(Arrays.asList(partition))
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
    Assert.assertTrue(
        rpcStatusR
            .getException()
            .getMessage()
            .contains("partitionID of partitionName:" + partition + " can not be find"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Has partition after drop partition ", dependsOnMethods = "dropPartition")
  public void hasPartitionAfterDropped() {
    R<Boolean> booleanR =
        milvusClient.hasPartition(
            HasPartitionParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionName(partition)
                .build());
    Assert.assertEquals(booleanR.getStatus().intValue(), 0);
    Assert.assertEquals(booleanR.getData(), Boolean.FALSE);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "Get partition Statistics after drop partition ",
      dependsOnMethods = "dropPartition")
  public void getPartitionStatisticsAfterDropped() {
    R<GetPartitionStatisticsResponse> partitionStatistics =
        milvusClient.getPartitionStatistics(
            GetPartitionStatisticsParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionName(partition)
                .build());
    Assert.assertEquals(partitionStatistics.getStatus().intValue(), 1);
    Assert.assertTrue(
        partitionStatistics
            .getException()
            .getMessage()
            .contains("partitionID of partitionName:" + partition + " can not be find"));
  }
}
