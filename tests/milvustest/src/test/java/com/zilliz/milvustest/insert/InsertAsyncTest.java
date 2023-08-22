package com.zilliz.milvustest.insert;

import com.google.common.util.concurrent.ListenableFuture;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.DataType;
import io.milvus.grpc.MutationResult;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.InsertParam;
import io.qameta.allure.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

@Epic("Insert")
@Feature("InsertAsyncT")
public class InsertAsyncTest extends BaseTest {
  public String stringPKAndBinaryCollection;

  @BeforeClass(description = "provider collection",alwaysRun = true)
  public void providerData() {
    stringPKAndBinaryCollection = CommonFunction.createStringPKAndBinaryCollection();
  }

  @AfterClass(description = "delete test data",alwaysRun = true)
  public void deleteData() {
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(stringPKAndBinaryCollection).build());
  }
  @Test(description = "Insert data Async",groups = {"Smoke"})
  @Severity(SeverityLevel.BLOCKER)
  public void insertAsyncSuccess() throws ExecutionException, InterruptedException {
    List<InsertParam.Field> fields = CommonFunction.generateData(2000);
    ListenableFuture<R<MutationResult>> rListenableFuture =
        milvusClient.insertAsync(
            InsertParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withFields(fields)
                .build());
    R<MutationResult> mutationResultR = rListenableFuture.get();
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
    Assert.assertEquals(mutationResultR.getData().getSuccIndexCount(), 2000);
    System.out.println(mutationResultR.getData());
  }
  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Insert async data into partition ")
  public void insertAsyncDataIntoPartition() throws ExecutionException, InterruptedException  {
    List<InsertParam.Field> fields = CommonFunction.generateData(2000);
    ListenableFuture<R<MutationResult>> rListenableFuture =
            milvusClient.insertAsync(
                    InsertParam.newBuilder()
                            .withCollectionName(CommonData.defaultCollection)
                            .withPartitionName(CommonData.defaultPartition)
                            .withFields(fields)
                            .build());
    R<MutationResult> mutationResultR = rListenableFuture.get();
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
    Assert.assertEquals(mutationResultR.getData().getSuccIndexCount(), 2000);
    Assert.assertEquals(mutationResultR.getData().getDeleteCnt(), 0);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Multiple insert async into String pk and binary vector Collection")
  public void multipleInsertIntoStringPKAndBinaryCollection() throws ExecutionException, InterruptedException  {
    List<InsertParam.Field> fields = CommonFunction.generateStringPKBinaryData(2000);
    ListenableFuture<R<MutationResult>> rListenableFuture =
            milvusClient.insertAsync(
                    InsertParam.newBuilder()
                            .withCollectionName(stringPKAndBinaryCollection)
                            .withFields(fields)
                            .build());
    R<MutationResult> mutationResultR = rListenableFuture.get();
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
    Assert.assertEquals(mutationResultR.getData().getSuccIndexCount(), 2000);

    List<InsertParam.Field> fields2 = CommonFunction.generateStringPKBinaryData(3000);
    ListenableFuture<R<MutationResult>> rListenableFuture2 =
            milvusClient.insertAsync(
                    InsertParam.newBuilder()
                            .withCollectionName(stringPKAndBinaryCollection)
                            .withFields(fields2)
                            .build());
    R<MutationResult> mutationResultR2 = rListenableFuture2.get();
    Assert.assertEquals(mutationResultR2.getStatus().intValue(), 0);
    Assert.assertEquals(mutationResultR2.getData().getSuccIndexCount(), 3000);

  }

  @Severity(SeverityLevel.NORMAL)
  @Issue("https://github.com/milvus-io/milvus-sdk-java/issues/358")
  @Test(description = "Insert async into collection not match vector type")
  public void insertAsyncIntoCollectionNotMatchVectorType() throws ExecutionException, InterruptedException  {
    List<InsertParam.Field> fields = CommonFunction.generateBinaryData(10);
    ListenableFuture<R<MutationResult>> rListenableFuture =
            milvusClient.insertAsync(
                    InsertParam.newBuilder()
                            .withCollectionName(CommonData.defaultCollection)
                            .withFields(fields)
                            .build());
    R<MutationResult> mutationResultR = rListenableFuture.get();
    Assert.assertEquals(mutationResultR.getStatus().intValue(), -5);
    Assert.assertTrue(mutationResultR.getException().getMessage().contains("The field: VectorFieldAutoTest is not provided."));
  }


  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Insert async into collection not match PK type")
  public void insertAsyncIntoCollectionNotMatchPKType() throws ExecutionException, InterruptedException  {
    Random ran = new Random();
    List<String> book_id_array = new ArrayList<>();
    List<String> word_count_array = new ArrayList<>();
    List<List<Float>> book_intro_array = new ArrayList<>();
    for (long i = 0L; i < 100; ++i) {
      book_id_array.add(MathUtil.genRandomStringAndChinese(10) + "-" + i);
      word_count_array.add(i + "-" + MathUtil.genRandomStringAndChinese(10));
      List<Float> vector = new ArrayList<>();
      for (int k = 0; k < 128; ++k) {
        vector.add(ran.nextFloat());
      }
      book_intro_array.add(vector);
    }
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id",  book_id_array));
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(
            new InsertParam.Field(
                    CommonData.defaultVectorField,  book_intro_array));

    ListenableFuture<R<MutationResult>> rListenableFuture =
            milvusClient.insertAsync(
                    InsertParam.newBuilder()
                            .withCollectionName(CommonData.defaultCollection)
                            .withFields(fields)
                            .build());
    R<MutationResult> mutationResultR = rListenableFuture.get();
    Assert.assertEquals(mutationResultR.getStatus().intValue(), -5);
    Assert.assertTrue(mutationResultR.getException().getMessage().contains("'book_id': Int64 field value type must be Long"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Insert async into collection not match scalar type")
  public void insertAsyncIntoCollectionNotMatchScalarType() throws ExecutionException, InterruptedException  {
    Random ran = new Random();
    List<Long> book_id_array = new ArrayList<>();
    List<String> word_count_array = new ArrayList<>();
    List<List<Float>> book_intro_array = new ArrayList<>();
    for (long i = 0L; i < 100; ++i) {
      book_id_array.add( i);
      word_count_array.add(i + "-" + MathUtil.genRandomStringAndChinese(10));
      List<Float> vector = new ArrayList<>();
      for (int k = 0; k < 128; ++k) {
        vector.add(ran.nextFloat());
      }
      book_intro_array.add(vector);
    }
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id",  book_id_array));
    fields.add(new InsertParam.Field("word_count",  word_count_array));
    fields.add(
            new InsertParam.Field(
                    CommonData.defaultVectorField,  book_intro_array));

    ListenableFuture<R<MutationResult>> rListenableFuture =
            milvusClient.insertAsync(
                    InsertParam.newBuilder()
                            .withCollectionName(CommonData.defaultCollection)
                            .withFields(fields)
                            .build());
    R<MutationResult> mutationResultR = rListenableFuture.get();
    Assert.assertEquals(mutationResultR.getStatus().intValue(), -5);
    Assert.assertTrue(mutationResultR.getException().getMessage().contains("'word_count': Int64 field value type must be Long"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Insert async into collection with auto pk")
  public void insertIntoCollectionWithAutoPK()  throws ExecutionException, InterruptedException {
    String newCollectionWithAutoPK = CommonFunction.createNewCollectionWithAutoPK();
    List<InsertParam.Field> fields = CommonFunction.generateDataWithAutoPK(10);
    ListenableFuture<R<MutationResult>> rListenableFuture =
            milvusClient.insertAsync(
                    InsertParam.newBuilder()
                            .withCollectionName(newCollectionWithAutoPK)
                            .withFields(fields)
                            .build());
    R<MutationResult> mutationResultR = rListenableFuture.get();
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
    Assert.assertEquals(mutationResultR.getData().getSuccIndexCount(), 10);
    milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(newCollectionWithAutoPK).build());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Insert async into collection with largest dim")
  public void insertIntoCollectionWithLargestDim() throws ExecutionException, InterruptedException  {
    // create largest dim
    String collectionName = "Collection_" + MathUtil.getRandomString(10);
    FieldType fieldType1 =
            FieldType.newBuilder()
                    .withName("book_id")
                    .withDataType(DataType.Int64)
                    .withPrimaryKey(true)
                    .withAutoID(true)
                    .build();
    FieldType fieldType2 =
            FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
    FieldType fieldType3 =
            FieldType.newBuilder()
                    .withName(CommonData.defaultVectorField)
                    .withDataType(DataType.FloatVector)
                    .withDimension(32768)
                    .build();
    CreateCollectionParam createCollectionReq =
            CreateCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withDescription("Test" + collectionName + "search")
                    .withShardsNum(2)
                    .addFieldType(fieldType1)
                    .addFieldType(fieldType2)
                    .addFieldType(fieldType3)
                    .build();
    R<RpcStatus> collection = BaseTest.milvusClient.createCollection(createCollectionReq);
    // general data
    Random ran = new Random();
    List<Long> word_count_array = new ArrayList<>();
    List<List<Float>> book_intro_array = new ArrayList<>();
    for (long i = 0L; i < 100; ++i) {
      word_count_array.add(i + 10000);
      List<Float> vector = new ArrayList<>();
      for (int k = 0; k < 32768; ++k) {
        vector.add(ran.nextFloat());
      }
      book_intro_array.add(vector);
    }
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("word_count",  word_count_array));
    fields.add(
            new InsertParam.Field(
                    CommonData.defaultVectorField,  book_intro_array));

    ListenableFuture<R<MutationResult>> rListenableFuture =
            milvusClient.insertAsync(
                    InsertParam.newBuilder()
                            .withCollectionName(collectionName)
                            .withFields(fields)
                            .build());
    R<MutationResult> mutationResultR = rListenableFuture.get();
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
    Assert.assertEquals(mutationResultR.getData().getSuccIndexCount(), 100);
    milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(collectionName).build());
  }

}
