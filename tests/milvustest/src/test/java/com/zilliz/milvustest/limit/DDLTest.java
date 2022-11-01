package com.zilliz.milvustest.limit;

import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.FlushResponse;
import io.milvus.grpc.ManualCompactionResponse;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.control.ManualCompactParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DropIndexParam;
import io.milvus.param.partition.CreatePartitionParam;
import io.milvus.param.partition.DropPartitionParam;
import io.milvus.param.partition.LoadPartitionsParam;
import io.milvus.param.partition.ReleasePartitionsParam;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * @Author yongpeng.li @Date 2022/10/10 13:55
 */
@Slf4j
public class DDLTest {
  public static final MilvusServiceClient milvusClient =
          new MilvusServiceClient(
                  ConnectParam.newBuilder()
                          .withUri(
                                  System.getProperty("milvusUri") == null
                                          ? PropertyFilesUtil.getRunValue("milvusUri")
                                          : System.getProperty("milvusUri"))
                          .withAuthorization("db_admin", "Lyp0107!")
                          .build());

  @Test(description = "create collection test")
  public void createCollectionTest() {
    for (int i = 0; i < 20; i++) {
      String collectionName = "book" + i;
      int dim = 128;
      FieldType bookIdField =
          FieldType.newBuilder()
              .withName("book_id")
              .withDataType(DataType.Int64)
              .withPrimaryKey(true)
              .withAutoID(false)
              .build();
      FieldType wordCountField =
          FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
      FieldType bookIntroField =
          FieldType.newBuilder()
              .withName("book_intro")
              .withDataType(DataType.FloatVector)
              .withDimension(dim)
              .build();
      CreateCollectionParam createCollectionParam =
          CreateCollectionParam.newBuilder()
              .withCollectionName(collectionName)
              .withDescription("Test book search")
              .withShardsNum(2)
              .addFieldType(bookIdField)
              .addFieldType(wordCountField)
              .addFieldType(bookIntroField)
              .build();
      try {
        Thread.sleep(0);
      } catch (InterruptedException e) {
        log.info(e.getMessage());
      }
      R<RpcStatus> collection = milvusClient.createCollection(createCollectionParam);
      log.info(collection.toString());
    }
  }

  @Test(description = "load collection test")
  public void loadCollectionTest() {
    for (int i = 0; i < 20; i++) {
      String collectionName = "book" + i;
      R<RpcStatus> rpcStatusR =
          milvusClient.loadCollection(
              LoadCollectionParam.newBuilder().withCollectionName(collectionName).build());
      log.info(rpcStatusR.toString());
    }
  }

  @Test(description = "release collection test")
  public void releaseCollectionTest() {
    for (int i = 0; i < 20; i++) {
      String collectionName = "book" + i;
      R<RpcStatus> rpcStatusR =
          milvusClient.releaseCollection(
              ReleaseCollectionParam.newBuilder().withCollectionName(collectionName).build());
      log.info(rpcStatusR.toString());
    }
  }

  @Test(description = "Drop collection test")
  public void dropCollectionTest() {
    for (int i = 0; i < 20; i++) {
      String collectionName = "book" + i;
      R<RpcStatus> rpcStatusR =
          milvusClient.dropCollection(
              DropCollectionParam.newBuilder().withCollectionName(collectionName).build());
      log.info(rpcStatusR.toString());
      try {
        Thread.sleep(0);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test(description = "Mix Collection Operate Test")
  public void mixCollectionOperateTest() throws InterruptedException {
    for (int i = 0; i < 20; i++) {
      String collectionName = "book" + i;
      R<RpcStatus> rpcStatusR =
              milvusClient.loadCollection(
                      LoadCollectionParam.newBuilder().withCollectionName(collectionName).build());
      log.info(rpcStatusR.toString());
      Thread.sleep(1000);
      R<RpcStatus> rpcStatusR2 =
              milvusClient.releaseCollection(
                      ReleaseCollectionParam.newBuilder().withCollectionName(collectionName).build());
      log.info(rpcStatusR2.toString());
      Thread.sleep(1000);

    }
  }

  @Test(description = "create partition test")
  public void createPartitionTest() {
    for (int i = 0; i < 20; i++) {
      String collectionName = "book" + i;
      String partitionName = "book_partition" + i;
      R<RpcStatus> partition = milvusClient.createPartition(CreatePartitionParam.newBuilder()
              .withCollectionName(collectionName)
              .withPartitionName(partitionName).build());
      log.info(partition.toString());
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test(description = "load partition test")
  public void loadPartitionTest() {
    for (int i = 0; i < 20; i++) {
      String collectionName = "book" + i;
      String partitionName = "book_partition" + i;
      R<RpcStatus> loadPartitions = milvusClient.loadPartitions(LoadPartitionsParam.newBuilder()
              .withCollectionName(collectionName)
              .withPartitionNames(Arrays.asList(partitionName)).build());
      log.info(loadPartitions.toString());
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test(description = "release partition test")
  public void releasePartitionTest() {
    for (int i = 0; i < 20; i++) {
      String collectionName = "book" + i;
      String partitionName = "book_partition" + i;
      R<RpcStatus> releasePartitions = milvusClient.releasePartitions(ReleasePartitionsParam.newBuilder()
              .withCollectionName(collectionName)
              .withPartitionNames(Arrays.asList(partitionName)).build());
      log.info(releasePartitions.toString());
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test(description = "drop partition test")
  public void dropPartitionTest() {
    for (int i = 0; i < 20; i++) {
      String collectionName = "book" + i;
      String partitionName = "book_partition" + i;
      R<RpcStatus> dropPartition = milvusClient.dropPartition(DropPartitionParam.newBuilder()
              .withCollectionName(collectionName)
              .withPartitionName(partitionName).build());
      log.info(dropPartition.toString());
      try {
        Thread.sleep(0);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }


  @Test(description = "Mix partition Operate Test")
  public void mixPartitionOperateTest() throws InterruptedException {
    for (int i = 0; i < 20; i++) {
      String collectionName = "book" + i;
      String partitionName = "book_partition" + i;
      R<RpcStatus> loadPartitions = milvusClient.loadPartitions(LoadPartitionsParam.newBuilder()
              .withCollectionName(collectionName)
              .withPartitionNames(Arrays.asList(partitionName)).build());
      log.info(loadPartitions.toString());
      Thread.sleep(1000);
      R<RpcStatus> releasePartitions = milvusClient.releasePartitions(ReleasePartitionsParam.newBuilder()
              .withCollectionName(collectionName)
              .withPartitionNames(Arrays.asList(partitionName)).build());
      log.info(releasePartitions.toString());
      Thread.sleep(1000);

    }
  }

  //IndexRate
  @Test(description = "createIndex rate test")
  public void createIndexRateTest() throws InterruptedException {
    for (int i = 0; i < 1; i++) {
      String collectionName = "book" + 128;
      R<RpcStatus> rpcStatusR = milvusClient.createIndex(CreateIndexParam.newBuilder()
              .withIndexName("index" + i)
              .withMetricType(MetricType.L2)
              .withFieldName(CommonData.defaultVectorField)
              .withCollectionName(collectionName)
              .withIndexType(IndexType.AUTOINDEX)
              .build());
      log.info(rpcStatusR.toString());
      Thread.sleep(1000);
    }
  }

  @Test(description = "dropIndex rate test")
  public void dropIndexRateTest() throws InterruptedException {
    for (int i = 0; i < 1; i++) {
      String collectionName = "book" + 128;
      R<RpcStatus> rpcStatusR = milvusClient.dropIndex(DropIndexParam.newBuilder()
              .withIndexName("index" + i)
              .withCollectionName(collectionName).build());
      log.info(rpcStatusR.toString());
      Thread.sleep(0);
    }
  }

  @Test(description = "flush rate  test")
  public void flushTest() throws InterruptedException {
    for (int i = 0; i < 20; i++) {
      String collectionName = "book" + i;
      R<FlushResponse> flush = milvusClient.flush(FlushParam.newBuilder()
              .withCollectionNames(Collections.singletonList(collectionName)).build());
      log.info(flush.toString());
      Thread.sleep(0);
    }
  }

  @Test(description = "Compaction rate test")
  public void compactionTest() throws InterruptedException {
    for (int i = 0; i < 20; i++) {
      String collectionName = "book" + i;
      R<ManualCompactionResponse> manualCompactionResponseR = milvusClient.manualCompact(ManualCompactParam.newBuilder()
              .withCollectionName(collectionName).build());
      log.info(manualCompactionResponseR.toString());
      Thread.sleep(1000);
    }
  }




}
