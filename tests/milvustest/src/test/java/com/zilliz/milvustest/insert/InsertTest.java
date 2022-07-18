package com.zilliz.milvustest.insert;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.DataType;
import io.milvus.grpc.MutationResult;
import io.milvus.param.R;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Epic("Index")
@Feature("Insert")
public class InsertTest extends BaseTest {
  public String stringPKAndBinaryCollection;

  @BeforeClass(description = "provider collection")
  public void providerData() {
    stringPKAndBinaryCollection = CommonFunction.createStringPKAndBinaryCollection();
  }

  @AfterClass(description = "delete test data")
  public void deleteData() {
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(stringPKAndBinaryCollection).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Insert data into collection")
  public void insertDataIntoCollection() {
    List<InsertParam.Field> fields = CommonFunction.generateData(2000);
    R<MutationResult> mutationResultR =
        milvusClient.insert(
            InsertParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withFields(fields)
                .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
    Assert.assertEquals(mutationResultR.getData().getSuccIndexCount(), 2000);
    Assert.assertEquals(mutationResultR.getData().getDeleteCnt(), 0);
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Insert data into partition")
  public void insertDataIntoPartition() {
    List<InsertParam.Field> fields = CommonFunction.generateData(2000);
    R<MutationResult> mutationResultR =
        milvusClient.insert(
            InsertParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionName(CommonData.defaultPartition)
                .withFields(fields)
                .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
    Assert.assertEquals(mutationResultR.getData().getSuccIndexCount(), 2000);
    Assert.assertEquals(mutationResultR.getData().getDeleteCnt(), 0);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Multiple insert into String pk and binary vector Collection")
  public void multipleInsertIntoStringPKAndBinaryCollection() {
    List<InsertParam.Field> fields = CommonFunction.generateStringPKBinaryData(2000);
    R<MutationResult> mutationResultR =
        milvusClient.insert(
            InsertParam.newBuilder()
                .withCollectionName(stringPKAndBinaryCollection)
                .withFields(fields)
                .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
    Assert.assertEquals(mutationResultR.getData().getSuccIndexCount(), 2000);

    List<InsertParam.Field> fields2 = CommonFunction.generateStringPKBinaryData(3000);
    R<MutationResult> mutationResultR2 =
        milvusClient.insert(
            InsertParam.newBuilder()
                .withCollectionName(stringPKAndBinaryCollection)
                .withFields(fields2)
                .build());
    Assert.assertEquals(mutationResultR2.getStatus().intValue(), 0);
    Assert.assertEquals(mutationResultR2.getData().getSuccIndexCount(), 3000);

  }
}
