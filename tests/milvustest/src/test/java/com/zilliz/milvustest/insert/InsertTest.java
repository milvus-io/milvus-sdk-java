package com.zilliz.milvustest.insert;

import com.alibaba.fastjson.JSONObject;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.exception.ParamException;
import io.milvus.grpc.DataType;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.MutationResult;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.GetCollectionStatisticsParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.response.GetCollStatResponseWrapper;
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
import java.util.Random;

@Epic("Insert")
@Feature("Insert")
public class InsertTest extends BaseTest {
  public String stringPKAndBinaryCollection;
  public String collectionWithJsonField;
  public String collectionWithDynamicField;

  public String collectionWithFloat16Vector;
  public String collectionWithBF16Vector;
  private String collectionWithSparseVector;
  private String collectionWithMultiVector;
  @BeforeClass(description = "provider collection",alwaysRun = true)
  public void providerData() {
    stringPKAndBinaryCollection = CommonFunction.createStringPKAndBinaryCollection();
    collectionWithJsonField= CommonFunction.createNewCollectionWithJSONField();
    collectionWithDynamicField= CommonFunction.createNewCollectionWithDynamicField();
    collectionWithFloat16Vector = CommonFunction.createFloat16Collection();
    collectionWithBF16Vector = CommonFunction.createBf16Collection();
    collectionWithSparseVector = CommonFunction.createSparseFloatVectorCollection();
    collectionWithMultiVector = CommonFunction.createMultiVectorCollection();
  }

  @AfterClass(description = "delete test data",alwaysRun = true)
  public void deleteData() {
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(stringPKAndBinaryCollection).build());
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(collectionWithJsonField).build());
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(collectionWithDynamicField).build());
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(collectionWithFloat16Vector).build());
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(collectionWithBF16Vector).build());
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(collectionWithSparseVector).build());
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(collectionWithMultiVector).build());


  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Insert data into collection",groups = {"Smoke"})
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

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Insert into collection not match vector type")
  public void insertIntoCollectionNotMatchVectorType() {
    List<InsertParam.Field> fields = CommonFunction.generateBinaryData(10);
    R<MutationResult> mutationResultR =
            milvusClient.insert(
                    InsertParam.newBuilder()
                            .withCollectionName(CommonData.defaultCollection)
                            .withFields(fields)
                            .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), -5);
    Assert.assertTrue(mutationResultR.getException().getMessage().contains("The field: VectorFieldAutoTest is not provided"));
  }


  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Insert into collection not match PK type")
  public void insertIntoCollectionNotMatchPKType() {
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
    fields.add(new InsertParam.Field("book_id", book_id_array));
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(
            new InsertParam.Field(
                    CommonData.defaultVectorField, book_intro_array));

    R<MutationResult> mutationResultR =
            milvusClient.insert(
                    InsertParam.newBuilder()
                            .withCollectionName(CommonData.defaultCollection)
                            .withFields(fields)
                            .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), -5);
    Assert.assertTrue(mutationResultR.getException().getMessage().contains("'book_id': Int64 field value type must be Long"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Insert into collection not match scalar type")
  public void insertIntoCollectionNotMatchScalarType() {
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
    fields.add(new InsertParam.Field("book_id", book_id_array));
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(
            new InsertParam.Field(
                    CommonData.defaultVectorField, book_intro_array));

    R<MutationResult> mutationResultR =
            milvusClient.insert(
                    InsertParam.newBuilder()
                            .withCollectionName(CommonData.defaultCollection)
                            .withFields(fields)
                            .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), -5);
    Assert.assertTrue(mutationResultR.getException().getMessage().contains("'word_count': Int64 field value type must be Long"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Insert into collection with auto pk")
  public void insertIntoCollectionWithAutoPK() {
    String newCollectionWithAutoPK = CommonFunction.createNewCollectionWithAutoPK();
    List<InsertParam.Field> fields = CommonFunction.generateDataWithAutoPK(10);
    R<MutationResult> mutationResultR =
            milvusClient.insert(
                    InsertParam.newBuilder()
                            .withCollectionName(newCollectionWithAutoPK)
                            .withFields(fields)
                            .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
    Assert.assertEquals(mutationResultR.getData().getSuccIndexCount(), 10);
    milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(newCollectionWithAutoPK).build());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Insert into collection with largest dim")
  public void insertIntoCollectionWithLargestDim() {
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
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(
            new InsertParam.Field(
                    CommonData.defaultVectorField, book_intro_array));

    R<MutationResult> mutationResultR =
            milvusClient.insert(
                    InsertParam.newBuilder()
                            .withCollectionName(collectionName)
                            .withFields(fields)
                            .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 0);
    Assert.assertEquals(mutationResultR.getData().getSuccIndexCount(), 100);
    milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(collectionName).build());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Insert field inconsistent dimension")
  public void insertVectorFieldInconsistentDim() {
    Random ran = new Random();
    List<Long> book_id_array = new ArrayList<>();
    List<Long> word_count_array = new ArrayList<>();
    List<List<Float>> book_intro_array = new ArrayList<>();
    for (long i = 0L; i < 100; ++i) {
      book_id_array.add( i);
      word_count_array.add(i+10000);
      List<Float> vector = new ArrayList<>();
      for (int k = 0; k < 127; ++k) {
        vector.add(ran.nextFloat());
      }
      book_intro_array.add(vector);
    }
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id", book_id_array));
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(
            new InsertParam.Field(
                    CommonData.defaultVectorField, book_intro_array));

    R<MutationResult> mutationResultR =
            milvusClient.insert(
                    InsertParam.newBuilder()
                            .withCollectionName(CommonData.defaultCollection)
                            .withFields(fields)
                            .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), -5);
    Assert.assertTrue(mutationResultR.getException().getMessage().contains("The field: VectorFieldAutoTest is not provided"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Insert field inconsistent number",expectedExceptions = ParamException.class)
  public void insertVectorFieldInconsistentNumber() {
    Random ran = new Random();
    List<Long> book_id_array = new ArrayList<>();
    List<Long> word_count_array = new ArrayList<>();
    List<List<Float>> book_intro_array = new ArrayList<>();
    for (long i = 0L; i < 100; ++i) {
      book_id_array.add( i);
      word_count_array.add(i+10000);

    }
    for (int i = 0; i < 90; i++) {
      List<Float> vector = new ArrayList<>();
      for (int k = 0; k < 128; ++k) {
        vector.add(ran.nextFloat());
      }
      book_intro_array.add(vector);
    }
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id", book_id_array));
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(
            new InsertParam.Field(
                    CommonData.defaultVectorField, book_intro_array));

    R<MutationResult> mutationResultR =
            milvusClient.insert(
                    InsertParam.newBuilder()
                            .withCollectionName(CommonData.defaultCollection)
                            .withFields(fields)
                            .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), -5);
    Assert.assertTrue(mutationResultR.getException().getMessage().contains("Row count of fields must be equal"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Insert into nonexistent collection")
  public void insertIntoNonexistentCollection() {
    List<InsertParam.Field> fields = CommonFunction.generateData(10);
    R<MutationResult> mutationResultR =
            milvusClient.insert(
                    InsertParam.newBuilder()
                            .withCollectionName("NonexistentCollection")
                            .withFields(fields)
                            .build());
    Assert.assertEquals(mutationResultR.getStatus().intValue(), 1);
    Assert.assertTrue(mutationResultR.getException().getMessage().contains("can't find collection"));
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Insert into collection with empty String fields")
  public void insertWithEmptyStringField(){
    String stringPKCollection = CommonFunction.createStringPKCollection();
    Random ran = new Random();
    List<String> book_name_array = new ArrayList<>();
    List<String> book_content_array = new ArrayList<>();
    List<List<Float>> book_intro_array = new ArrayList<>();
    for (long i = 0L; i < 1000; ++i) {
      book_name_array.add(MathUtil.genRandomStringAndChinese(10) + "-" + i);
      book_content_array.add("");
      List<Float> vector = new ArrayList<>();
      for (int k = 0; k < 128; ++k) {
        vector.add(ran.nextFloat());
      }
      book_intro_array.add(vector);
    }
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_name", book_name_array));
    fields.add(new InsertParam.Field("book_content", book_content_array));
    fields.add(
            new InsertParam.Field(
                    CommonData.defaultVectorField, book_intro_array));
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder().withCollectionName(stringPKCollection)
            .withFields(fields).build());
    Assert.assertEquals(insert.getStatus().intValue(), 0);
    Assert.assertEquals(insert.getData().getSuccIndexCount(),1000);
    milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(stringPKCollection).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Insert into collection with empty String fields")
  public void insertWithEmptyStringPK(){
    String stringPKCollection = CommonFunction.createStringPKCollection();
    Random ran = new Random();
    List<String> book_name_array = new ArrayList<>();
    List<String> book_content_array = new ArrayList<>();
    List<List<Float>> book_intro_array = new ArrayList<>();
    for (long i = 0L; i < 1000; ++i) {
      book_name_array.add("");
      book_content_array.add(MathUtil.genRandomStringAndChinese(10) + "-" + i);
      List<Float> vector = new ArrayList<>();
      for (int k = 0; k < 128; ++k) {
        vector.add(ran.nextFloat());
      }
      book_intro_array.add(vector);
    }
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_name", book_name_array));
    fields.add(new InsertParam.Field("book_content", book_content_array));
    fields.add(
            new InsertParam.Field(
                    CommonData.defaultVectorField, book_intro_array));
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder().withCollectionName(stringPKCollection)
            .withFields(fields).build());
    Assert.assertEquals(insert.getStatus().intValue(), 0);
    Assert.assertEquals(insert.getData().getSuccIndexCount(),1000);
    // has collection
    R<GetCollectionStatisticsResponse> respCollectionStatistics =
        milvusClient.getCollectionStatistics(
            GetCollectionStatisticsParam.newBuilder()
                .withCollectionName(stringPKCollection)
                .withFlush(false)
                .build());
    Assert.assertEquals(respCollectionStatistics.getStatus().intValue(), 0);
    GetCollStatResponseWrapper wrapperCollectionStatistics =
            new GetCollStatResponseWrapper(respCollectionStatistics.getData());
    Assert.assertEquals(wrapperCollectionStatistics.getRowCount(), 1000);
    milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(stringPKCollection).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Insert data into collection with JSON field",groups = {"Smoke"})
  public void insertDataCollectionWithJsonField(){
    List<JSONObject> jsonObjects = CommonFunction.generateJsonData(100);
    System.out.println(jsonObjects);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withCollectionName(collectionWithJsonField)
            .withRows(jsonObjects)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(), 0);
    Assert.assertEquals(insert.getData().getSuccIndexCount(),100);
  }
  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Insert data into collection with Dynamic field",groups = {"Smoke"})
  public void insertDataCollectionWithDynamicField(){
    List<JSONObject> jsonObjects = CommonFunction.generateDataWithDynamicFiledRow(100);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withCollectionName(collectionWithDynamicField)
            .withRows(jsonObjects)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(), 0);
    Assert.assertEquals(insert.getData().getSuccIndexCount(),100);
  }
  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Insert data into collection with Dynamic field use column data",expectedExceptions = ParamException.class)
  public void insertDataCollectionWithDynamicFieldUseColumnData(){
    List<InsertParam.Field> fields = CommonFunction.generateDataWithDynamicFiledColumn(100);
    List<JSONObject> jsonObjects = CommonFunction.generateJsonData(100);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withCollectionName(collectionWithDynamicField)
            .withFields(fields)
            .withRows(jsonObjects)
            .build());

  }
  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Insert data into collection with Dynamic field",expectedExceptions = ParamException.class)
  public void insertFieldsAndRowsData(){
    List<InsertParam.Field> fields = CommonFunction.generateDataWithDynamicFiledColumn(100);
    List<JSONObject> jsonObjects = CommonFunction.generateDataWithDynamicFiledRow(100);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withCollectionName(collectionWithDynamicField)
            .withFields(fields)
            .withRows(jsonObjects)
            .build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Insert data into float16 vector collection ",groups = {"Smoke"})
  public void insertDataIntoFloat16VectorCollection(){
    List<InsertParam.Field> fields = CommonFunction.generateDataWithFloat16Vector(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withCollectionName(collectionWithFloat16Vector)
            .withFields(fields)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(), 0);
    Assert.assertEquals(insert.getData().getSuccIndexCount(),1000);
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Insert data into bf16 vector collection ",groups = {"Smoke"})
  public void insertDataIntoBf16VectorCollection(){
    List<InsertParam.Field> fields = CommonFunction.generateDataWithBF16Vector(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withCollectionName(collectionWithBF16Vector)
            .withFields(fields)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(), 0);
    Assert.assertEquals(insert.getData().getSuccIndexCount(),1000);
  }

  @Test(description = "Insert data into sparse vector collection ",groups = {"Smoke"})
  public void insertDataIntoSparseVectorCollection(){
    List<InsertParam.Field> fields = CommonFunction.generateDataWithSparseFloatVector(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withCollectionName(collectionWithSparseVector)
            .withFields(fields)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(), 0);
    Assert.assertEquals(insert.getData().getSuccIndexCount(),1000);
  }

  @Test(description = "Insert data into multi vector collection ",groups = {"Smoke"})
  public void insertDataIntoMultiVectorCollection(){
    List<InsertParam.Field> fields = CommonFunction.generateDataWithMultiVector(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withCollectionName(collectionWithMultiVector)
            .withFields(fields)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(), 0);
    Assert.assertEquals(insert.getData().getSuccIndexCount(),1000);
  }


}
