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
import java.util.Arrays;
import java.util.List;

@Epic("Partition")
@Feature("ReleasePartitions")
public class ReleasePartitionsTest extends BaseTest {
  public String partition;

  @BeforeClass(description = "load partition")
  public void initPartition() {
    partition = "partition_" + MathUtil.getRandomString(10);
    milvusClient.createPartition(
        CreatePartitionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionName(partition)
            .build());
    milvusClient.loadPartitions(
        LoadPartitionsParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .addPartitionName(partition)
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
  @Test(description = "release partition")
  public void releasePartitionSuccess() {
    R<RpcStatus> rpcStatusR =
        milvusClient.releasePartitions(
            ReleasePartitionsParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addPartitionName(partition)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Test(
          description = "query from partition after release ",
          dependsOnMethods = "releasePartitionSuccess")
  @Severity(SeverityLevel.NORMAL)
  public void queryAfterReleasePartition() {
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
    Assert.assertTrue(queryResultsR.getException().getMessage().contains("checkIfLoaded failed when query"));
  }
}
