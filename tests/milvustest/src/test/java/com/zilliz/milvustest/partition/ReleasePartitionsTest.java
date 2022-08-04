package com.zilliz.milvustest.partition;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.QueryResults;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ReleaseCollectionParam;
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
  public String collection;

  @BeforeClass(description = "load partition",alwaysRun = true)
  public void initPartition() {
    collection = CommonFunction.createNewCollection();
    partition = "partition_" + MathUtil.getRandomString(10);
    milvusClient.createPartition(
        CreatePartitionParam.newBuilder()
            .withCollectionName(collection)
            .withPartitionName(partition)
            .build());
    milvusClient.loadPartitions(
        LoadPartitionsParam.newBuilder()
            .withCollectionName(collection)
            .addPartitionName(partition)
            .build());
  }

  @AfterClass(description = "delete partition after test",alwaysRun = true)
  public void deletePartition() {
    milvusClient.dropPartition(
        DropPartitionParam.newBuilder()
            .withCollectionName(collection)
            .withPartitionName(partition)
            .build());
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(collection).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "release partition",groups = {"Smoke"})
  public void releasePartition() {
    R<RpcStatus> rpcStatusR =
        milvusClient.releasePartitions(
            ReleasePartitionsParam.newBuilder()
                .withCollectionName(collection)
                .addPartitionName(partition)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Test(description = "query from partition after release ", dependsOnMethods = "releasePartition")
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
            .withCollectionName(collection)
            .withPartitionNames(partitions)
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 1);
    Assert.assertTrue(
        queryResultsR.getException().getMessage().contains("checkIfLoaded failed when query"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "release nonexistent partition")
  public void releaseNonexistentPartition() {
    R<RpcStatus> rpcStatusR =
        milvusClient.releasePartitions(
            ReleasePartitionsParam.newBuilder()
                .withCollectionName(collection)
                .addPartitionName("NonexistentPartition")
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
    Assert.assertTrue(
        rpcStatusR
            .getException()
            .getMessage()
            .contains("partitionID of partitionName:NonexistentPartition can not be find"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "release partition after load collection")
  public void releasePartitionAfterLoadCollection() {
    LoadCollectionParam loadCollectionParam =
            LoadCollectionParam.newBuilder().withCollectionName(CommonData.defaultCollection).build();
    milvusClient.loadCollection(loadCollectionParam);
    R<RpcStatus> rpcStatusR = milvusClient.releasePartitions(ReleasePartitionsParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionNames(Arrays.asList(CommonData.defaultPartition)).build());

    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
    Assert.assertTrue(
            rpcStatusR
                    .getException()
                    .getMessage()
                    .contains("releasing some partitions after load collection is not supported"));
    milvusClient.releaseCollection(
            ReleaseCollectionParam.newBuilder()
                    .withCollectionName(CommonData.defaultCollection)
                    .build());
  }
}
