package com.zilliz.milvustest.partition;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.QueryResults;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.partition.CreatePartitionParam;
import io.milvus.param.partition.DropPartitionParam;
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
  public void dropPartitionTest(String partitionName) {
    R<RpcStatus> rpcStatusR =
        milvusClient.dropPartition(
            DropPartitionParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionName(partitionName)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Test(
          description = "query float vector from partition ",
          dependsOnMethods = "dropPartitionTest")
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
    Assert.assertEquals(queryResultsR.getException().getMessage(), "partition name "+partition+" not found");
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
    Assert.assertEquals(rpcStatusR.getException().getMessage(), "DropPartition failed: partition NonexistentPartition does not exist");
  }
}
