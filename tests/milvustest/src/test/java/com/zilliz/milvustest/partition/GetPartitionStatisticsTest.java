package com.zilliz.milvustest.partition;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.GetPartitionStatisticsResponse;
import io.milvus.param.R;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.partition.CreatePartitionParam;
import io.milvus.param.partition.DropPartitionParam;
import io.milvus.param.partition.GetPartitionStatisticsParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

@Epic("Partition")
@Feature("GetPartitionStatistics")
public class GetPartitionStatisticsTest extends BaseTest {
  public String partition;

  @BeforeClass(description = "load partition",alwaysRun = true)
  public void initPartition() {
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
  @Test(description = "Get partition statistics",groups = {"Smoke"})
  public void getPartitionStatistics() {
    List<InsertParam.Field> fields = CommonFunction.generateData(1000);
    milvusClient.insert(
        InsertParam.newBuilder()
            .withPartitionName(partition)
            .withFields(fields)
            .withCollectionName(CommonData.defaultCollection)
            .build());
    R<GetPartitionStatisticsResponse> partitionStatisticsResponseR =
        milvusClient.getPartitionStatistics(
            GetPartitionStatisticsParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionName(partition)
                .build());
    Assert.assertEquals(partitionStatisticsResponseR.getStatus().intValue(), 0);
    GetPartitionStatisticsResponse partitionStatisticsResponse =
        partitionStatisticsResponseR.getData();
    Assert.assertEquals(partitionStatisticsResponse.getStats(0).getValue(), "1000");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Get nonexistent partition statistics")
  public void getNonexistentPartitionStatistics() {

    R<GetPartitionStatisticsResponse> partitionStatisticsResponseR =
            milvusClient.getPartitionStatistics(
                    GetPartitionStatisticsParam.newBuilder()
                            .withCollectionName(CommonData.defaultCollection)
                            .withPartitionName("NonexistentPartition")
                            .build());
    Assert.assertEquals(partitionStatisticsResponseR.getStatus().intValue(), 1);
    Assert.assertEquals(partitionStatisticsResponseR.getException().getMessage(),"partitionID of partitionName:NonexistentPartition can not be find");
  }
}
