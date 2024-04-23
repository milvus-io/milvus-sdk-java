package com.zilliz.milvustest.collection;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.exception.ParamException;
import io.milvus.grpc.DataType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FieldType;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Epic("Collection")
@Feature("CreateCollection")
public class CreateCollectionTest extends BaseTest {
  public String commonCollection;
  public String binaryVectorCollection;

  public String stringPKCollection;
  public String maxFieldCollection;

  public String arrayFieldCollection;

  public String float16FieldCollection;
  public String bf16FieldCollection;

  public String sparseVectorCollection;

  public String multiVectorCollection;

  @DataProvider(name = "collectionByDataProvider")
  public Object[][] provideCollectionName() {
    return new String[][] {{"collection_" + MathUtil.getRandomString(10)}};
  }

  @AfterClass(description = "delete test datas after CreateCollectionTest", alwaysRun = true)
  public void deleteTestData() {
    if (commonCollection != null) {
      milvusClient.dropCollection(
          DropCollectionParam.newBuilder().withCollectionName(commonCollection).build());
    }
    if (binaryVectorCollection != null) {
      milvusClient.dropCollection(
          DropCollectionParam.newBuilder().withCollectionName(binaryVectorCollection).build());
    }
    if (stringPKCollection != null) {
      milvusClient.dropCollection(
          DropCollectionParam.newBuilder().withCollectionName(stringPKCollection).build());
    }
    if (maxFieldCollection != null) {
      milvusClient.dropCollection(
          DropCollectionParam.newBuilder().withCollectionName(maxFieldCollection).build());
    }
    if (arrayFieldCollection != null) {
      milvusClient.dropCollection(
              DropCollectionParam.newBuilder().withCollectionName(arrayFieldCollection).build());
    }
    if (float16FieldCollection != null) {
      milvusClient.dropCollection(
              DropCollectionParam.newBuilder().withCollectionName(float16FieldCollection).build());
    }
    if (bf16FieldCollection != null) {
      milvusClient.dropCollection(
              DropCollectionParam.newBuilder().withCollectionName(bf16FieldCollection).build());
    }
    if (sparseVectorCollection != null) {
      milvusClient.dropCollection(
              DropCollectionParam.newBuilder().withCollectionName(sparseVectorCollection).build());
    }
    if (multiVectorCollection != null) {
      milvusClient.dropCollection(
              DropCollectionParam.newBuilder().withCollectionName(multiVectorCollection).build());
    }
  }

  @DataProvider(name = "dataTypeProvider")
  public Object[][] provideDataType() {
    Object[][] pk = new Object[][] {{DataType.Int64}, {DataType.VarChar}};
    Object[][] filedType =
        new Object[][] {
          {DataType.Double},
          {DataType.Float},
          {DataType.Int64},
          {DataType.Int8},
          {DataType.Int16},
          {DataType.Int32},
          {DataType.VarChar}
        };
    Object[][] vectorType = new Object[][] {{DataType.FloatVector}, {DataType.BinaryVector}};
    Object[][] dataType = new Object[pk.length * filedType.length * vectorType.length][3];
    int a = 0;
    for (int i = 0; i < pk.length; i++) {
      for (int j = 0; j < filedType.length; j++) {
        for (int k = 0; k < vectorType.length; k++) {
          dataType[a][0] = pk[i][0];
          dataType[a][1] = filedType[j][0];
          dataType[a][2] = vectorType[k][0];
          a++;
        }
      }
    }
    return dataType;
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Create collection success",
      dataProvider = "collectionByDataProvider",
      groups = {"Smoke"})
  public void createCollectionSuccess(String collectionName) {
    commonCollection = collectionName;
    FieldType fieldType1 =
        FieldType.newBuilder()
            .withName("book_id")
            .withDataType(DataType.Int64)
            .withPrimaryKey(true)
            .withAutoID(false)
            .build();
    FieldType fieldType2 =
        FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
    FieldType fieldType3 =
        FieldType.newBuilder()
            .withName("book_intro")
            .withDataType(DataType.FloatVector)
            .withDimension(128)
            .build();
    CreateCollectionParam createCollectionReq =
        CreateCollectionParam.newBuilder()
            .withCollectionName(collectionName)
            .withDescription("Test " + collectionName + " search")
            .withShardsNum(2)
            .addFieldType(fieldType1)
            .addFieldType(fieldType2)
            .addFieldType(fieldType3)
            .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
    Assert.assertEquals(collection.getStatus().toString(), "0");
    Assert.assertEquals(collection.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "Repeat create collection",
      dependsOnMethods = {"createCollectionSuccess"})
  public void createCollectionRepeat() {
    FieldType fieldType1 =
        FieldType.newBuilder()
            .withName("book_id")
            .withDataType(DataType.Int64)
            .withPrimaryKey(true)
            .withAutoID(false)
            .build();
    FieldType fieldType2 =
        FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
    FieldType fieldType3 =
        FieldType.newBuilder()
            .withName("book_intro")
            .withDataType(DataType.FloatVector)
            .withDimension(2)
            .build();
    CreateCollectionParam createCollectionReq =
        CreateCollectionParam.newBuilder()
            .withCollectionName(commonCollection)
            .withDescription("Test" + commonCollection + "search")
            .withShardsNum(2)
            .addFieldType(fieldType1)
            .addFieldType(fieldType2)
            .addFieldType(fieldType3)
            .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
    Assert.assertEquals(collection.getStatus().toString(), "1");
    Assert.assertEquals(
        collection.getException().getMessage(),
        "CreateCollection failed: meta table add collection failed,error = collection "
            + commonCollection
            + " exist");
  }

  @Severity(SeverityLevel.MINOR)
  @Test(description = "Create collection without params", expectedExceptions = ParamException.class)
  public void createCollectionWithoutCollectionName() {
    R<RpcStatus> rpcStatusR =
        milvusClient.createCollection(CreateCollectionParam.newBuilder().build());
    System.out.println(rpcStatusR);
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Create binary vector collection", dataProvider = "collectionByDataProvider")
  public void createBinaryVectorCollection(String collectionName) {
    binaryVectorCollection = collectionName;
    FieldType fieldType1 =
        FieldType.newBuilder()
            .withName("book_id")
            .withDataType(DataType.Int64)
            .withPrimaryKey(true)
            .withAutoID(false)
            .build();
    FieldType fieldType2 =
        FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
    FieldType fieldType3 =
        FieldType.newBuilder()
            .withName("book_intro")
            .withDataType(DataType.BinaryVector)
            .withDimension(128)
            .build();
    CreateCollectionParam createCollectionReq =
        CreateCollectionParam.newBuilder()
            .withCollectionName(collectionName)
            .withDescription("Test " + collectionName + " search")
            .withShardsNum(2)
            .addFieldType(fieldType1)
            .addFieldType(fieldType2)
            .addFieldType(fieldType3)
            .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
    Assert.assertEquals(collection.getStatus().toString(), "0");
    Assert.assertEquals(collection.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Create String collection", dataProvider = "collectionByDataProvider")
  public void createStringPKCollection(String collectionName) {
    stringPKCollection = collectionName;
    FieldType fieldType1 =
        FieldType.newBuilder()
            .withName("book_name")
            .withDataType(DataType.VarChar)
            .withMaxLength(20)
            .withPrimaryKey(true)
            .withAutoID(false)
            .build();
    FieldType fieldType2 =
        FieldType.newBuilder()
            .withName("book_content")
            .withDataType(DataType.VarChar)
            .withMaxLength(10)
            .build();
    FieldType fieldType3 =
        FieldType.newBuilder()
            .withName("book_intro")
            .withDataType(DataType.FloatVector)
            .withDimension(128)
            .build();
    CreateCollectionParam createCollectionReq =
        CreateCollectionParam.newBuilder()
            .withCollectionName(collectionName)
            .withDescription("Test " + collectionName + " search")
            .withShardsNum(2)
            .addFieldType(fieldType1)
            .addFieldType(fieldType2)
            .addFieldType(fieldType3)
            .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
    Assert.assertEquals(collection.getStatus().toString(), "0");
    Assert.assertEquals(collection.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "Create String collection without max length",
      dataProvider = "collectionByDataProvider",
      expectedExceptions = ParamException.class)
  public void createStringPKCollectionWithoutMaxLength(String collectionName) {
    FieldType fieldType1 =
        FieldType.newBuilder()
            .withName("book_name")
            .withDataType(DataType.VarChar)
            .withPrimaryKey(true)
            .withAutoID(false)
            .build();
    FieldType fieldType2 =
        FieldType.newBuilder().withName("book_content").withDataType(DataType.VarChar).build();
    FieldType fieldType3 =
        FieldType.newBuilder()
            .withName("book_intro")
            .withDataType(DataType.FloatVector)
            .withDimension(128)
            .build();
    CreateCollectionParam createCollectionReq =
        CreateCollectionParam.newBuilder()
            .withCollectionName(collectionName)
            .withDescription("Test " + collectionName + " search")
            .withShardsNum(2)
            .addFieldType(fieldType1)
            .addFieldType(fieldType2)
            .addFieldType(fieldType3)
            .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
    Assert.assertEquals(collection.getStatus().toString(), "1");
    Assert.assertEquals(
        collection.getData().getMsg(), "Varchar field max length must be specified");
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Float vector collection using each data type",
      dataProvider = "dataTypeProvider")
  public void createCollectionWithEachDataType(
      DataType pk, DataType fieldType, DataType vectorType) {
    String collectionName = "coll" + MathUtil.getRandomString(10);
    FieldType fieldType1 =
        FieldType.newBuilder()
            .withName("book_id")
            .withDataType(pk)
            .withPrimaryKey(true)
            .withAutoID(false)
            .withMaxLength(10)
            .build();
    FieldType fieldType2 =
        FieldType.newBuilder()
            .withName("word_count")
            .withDataType(fieldType)
            .withMaxLength(10)
            .build();
    FieldType fieldType3 =
        FieldType.newBuilder()
            .withName("book_intro")
            .withDataType(vectorType)
            .withDimension(128)
            .build();
    CreateCollectionParam createCollectionReq =
        CreateCollectionParam.newBuilder()
            .withCollectionName(collectionName)
            .withDescription("Test " + collectionName + " search")
            .withShardsNum(2)
            .addFieldType(fieldType1)
            .addFieldType(fieldType2)
            .addFieldType(fieldType3)
            .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
    Assert.assertEquals(collection.getStatus().toString(), "0");
    Assert.assertEquals(collection.getData().getMsg(), "Success");
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(collectionName).build());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Create collection with max fields")
  public void createCollectionWithMaxFields() {
    maxFieldCollection = "coll_" + MathUtil.getRandomString(10);
    FieldType fieldType1 =
        FieldType.newBuilder()
            .withName("book_id")
            .withDataType(DataType.Int64)
            .withPrimaryKey(true)
            .withAutoID(false)
            .build();

    FieldType fieldType3 =
        FieldType.newBuilder()
            .withName("book_intro")
            .withDataType(DataType.FloatVector)
            .withDimension(128)
            .build();
    CreateCollectionParam.Builder builder = CreateCollectionParam.newBuilder();
    builder
        .withCollectionName(maxFieldCollection)
        .withDescription("Test " + maxFieldCollection + " search")
        .withShardsNum(2)
        .addFieldType(fieldType1)
        .addFieldType(fieldType3);

    for (int i = 0; i < 254; i++) {
      FieldType field =
          FieldType.newBuilder().withName("field_" + i).withDataType(DataType.Int64).build();
      builder.addFieldType(field);
    }
    R<RpcStatus> collection = milvusClient.createCollection(builder.build());
    Assert.assertEquals(collection.getStatus().toString(), "0");
    Assert.assertEquals(collection.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Create collection with max fields")
  public void createCollectionWith257Fields() {
    maxFieldCollection = "coll_" + MathUtil.getRandomString(10);
    FieldType fieldType1 =
        FieldType.newBuilder()
            .withName("book_id")
            .withDataType(DataType.Int64)
            .withPrimaryKey(true)
            .withAutoID(false)
            .build();

    FieldType fieldType3 =
        FieldType.newBuilder()
            .withName("book_intro")
            .withDataType(DataType.FloatVector)
            .withDimension(128)
            .build();
    CreateCollectionParam.Builder builder = CreateCollectionParam.newBuilder();
    builder
        .withCollectionName(maxFieldCollection)
        .withDescription("Test " + maxFieldCollection + " search")
        .withShardsNum(2)
        .addFieldType(fieldType1)
        .addFieldType(fieldType3);

    for (int i = 0; i < 255; i++) {
      FieldType field =
          FieldType.newBuilder().withName("field_" + i).withDataType(DataType.Int64).build();
      builder.addFieldType(field);
    }
    R<RpcStatus> collection = milvusClient.createCollection(builder.build());
    Assert.assertEquals(collection.getStatus().intValue(), 1);
    Assert.assertTrue(
        collection
            .getException()
            .getMessage()
            .contains("maximum field's number should be limited to 256"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Create collection with multiple PK")
  public void createCollectionWithMultiPK() {
    String collection = "coll_" + MathUtil.getRandomString(10);
    FieldType fieldType1 =
        FieldType.newBuilder()
            .withName("book_id")
            .withDataType(DataType.Int64)
            .withPrimaryKey(true)
            .withAutoID(false)
            .build();
    FieldType fieldType2 =
        FieldType.newBuilder()
            .withName("word_count")
            .withDataType(DataType.Int64)
            .withPrimaryKey(true)
            .withAutoID(false)
            .build();
    FieldType fieldType3 =
        FieldType.newBuilder()
            .withName("book_intro")
            .withDataType(DataType.FloatVector)
            .withDimension(128)
            .build();
    R<RpcStatus> collectionR =
        milvusClient.createCollection(
            CreateCollectionParam.newBuilder()
                .withCollectionName(collection)
                .withShardsNum(2)
                .addFieldType(fieldType1)
                .addFieldType(fieldType2)
                .addFieldType(fieldType3)
                .build());
    Assert.assertEquals(collectionR.getStatus().intValue(), 1);
    Assert.assertTrue(collectionR.getException().getMessage().contains("there are more than one primary key"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Create collection with multiple thread")
  public void createCollectionWithMultiThread() {
    ExecutorService executorService= Executors.newFixedThreadPool(5);
    List<Future> futures=new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      int finalI = i;
      Callable callable= () -> {
        String collection="Col_"+ finalI +MathUtil.getRandomString(10);
        Integer resultCode;
        FieldType fieldType1 =
                FieldType.newBuilder()
                        .withName("book_id")
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withAutoID(false)
                        .build();
        FieldType fieldType2 =
                FieldType.newBuilder()
                        .withName("word_count")
                        .withDataType(DataType.Int64).build();
        FieldType fieldType3 =
                FieldType.newBuilder()
                        .withName("book_intro")
                        .withDataType(DataType.FloatVector)
                        .withDimension(128)
                        .build();
        R<RpcStatus> collectionR =
                milvusClient.createCollection(
                        CreateCollectionParam.newBuilder()
                                .withCollectionName(collection)
                                .withShardsNum(2)
                                .addFieldType(fieldType1)
                                .addFieldType(fieldType2)
                                .addFieldType(fieldType3)
                                .build());
        resultCode=collectionR.getStatus();
        milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(collection).build());
        return resultCode;
      };
      Future submit = executorService.submit(callable);
      futures.add(submit);
    }
    futures.forEach(x->{
      try {
        Assert.assertEquals(x.get().toString(),"0");
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Create collection with array fields")
  public void createCollectionWithArrayField(){
    arrayFieldCollection = "Collection_" + MathUtil.getRandomString(10);
    FieldType fieldType1 =
            FieldType.newBuilder()
                    .withName("int64_field")
                    .withDataType(DataType.Int64)
                    .withPrimaryKey(true)
                    .withAutoID(false)
                    .build();
    FieldType fieldType2 =
            FieldType.newBuilder()
                    .withName("float_vector")
                    .withDataType(DataType.FloatVector)
                    .withDimension(128)
                    .build();
    FieldType fieldType3=
            FieldType.newBuilder()
                    .withName("str_array_field")
                    .withDataType(DataType.Array)
                    .withElementType(DataType.VarChar)
                    .withMaxLength(256)
                    .withMaxCapacity(300)
                    .build();
    FieldType fieldType4=
            FieldType.newBuilder()
                    .withName("int_array_field")
                    .withDataType(DataType.Array)
                    .withElementType(DataType.Int64)
                    .withMaxLength(256)
                    .withMaxCapacity(300)
                    .build();
    FieldType fieldType5=
            FieldType.newBuilder()
                    .withName("float_array_field")
                    .withDataType(DataType.Array)
                    .withElementType(DataType.Float)
                    .withMaxLength(256)
                    .withMaxCapacity(300)
                    .build();
    CreateCollectionParam createCollectionReq =
            CreateCollectionParam.newBuilder()
                    .withCollectionName(arrayFieldCollection)
                    .withDescription("Test" + arrayFieldCollection + "search")
                    .withShardsNum(2)
                    .addFieldType(fieldType1)
                    .addFieldType(fieldType2)
                    .addFieldType(fieldType3)
                    .addFieldType(fieldType4)
                    .addFieldType(fieldType5)
                    .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
    Assert.assertEquals(collection.getStatus().toString(), "0");
    Assert.assertEquals(collection.getData().getMsg(), "Success");
  }

  @Test(description = "Create collection with float16 field",groups = {"Smoke"})
  public void createCollectionWithFloat16Vector(){
    float16FieldCollection = "Collection_" + MathUtil.getRandomString(10);
    FieldType fieldType1 =
            FieldType.newBuilder()
                    .withName("int64_field")
                    .withDataType(DataType.Int64)
                    .withPrimaryKey(true)
                    .withAutoID(false)
                    .build();
    FieldType fieldType2 =
            FieldType.newBuilder()
                    .withName("float_vector")
                    .withDataType(DataType.Float16Vector)
                    .withDimension(128)
                    .build();
    CollectionSchemaParam schemaParam = CollectionSchemaParam.newBuilder().addFieldType(fieldType1).addFieldType(fieldType2).build();
    CreateCollectionParam createCollectionReq =
            CreateCollectionParam.newBuilder()
                    .withCollectionName(float16FieldCollection)
                    .withDescription("Test" + float16FieldCollection + "search")
                    .withShardsNum(2)
                    .withSchema(schemaParam)
                    .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
    Assert.assertEquals(collection.getStatus().toString(), "0");
    Assert.assertEquals(collection.getData().getMsg(), "Success");
  }

  @Test(description = "Create collection with bf16 field",groups = {"Smoke"})
  public void createCollectionWithBf16Vector(){
    bf16FieldCollection = "Collection_" + MathUtil.getRandomString(10);
    FieldType fieldType1 =
            FieldType.newBuilder()
                    .withName("int64_field")
                    .withDataType(DataType.Int64)
                    .withPrimaryKey(true)
                    .withAutoID(false)
                    .build();
    FieldType fieldType2 =
            FieldType.newBuilder()
                    .withName("float_vector")
                    .withDataType(DataType.BFloat16Vector)
                    .withDimension(128)
                    .build();
    CollectionSchemaParam schemaParam = CollectionSchemaParam.newBuilder().addFieldType(fieldType1).addFieldType(fieldType2).build();
    CreateCollectionParam createCollectionReq =
            CreateCollectionParam.newBuilder()
                    .withCollectionName(bf16FieldCollection)
                    .withDescription("Test" + bf16FieldCollection + "search")
                    .withShardsNum(2)
                    .withSchema(schemaParam)
                    .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
    Assert.assertEquals(collection.getStatus().toString(), "0");
    Assert.assertEquals(collection.getData().getMsg(), "Success");
  }

  @Test(description = "Create collection with sparse vector field",groups = {"Smoke"})
  public void createCollectionWithSparseVector(){
    sparseVectorCollection = "Collection_" + MathUtil.getRandomString(10);
    FieldType fieldType1 =
            FieldType.newBuilder()
                    .withName("int64_field")
                    .withDataType(DataType.Int64)
                    .withPrimaryKey(true)
                    .withAutoID(false)
                    .build();
    FieldType fieldType2 =
            FieldType.newBuilder()
                    .withName("float_vector")
                    .withDataType(DataType.SparseFloatVector)
                    .build();
    CollectionSchemaParam schemaParam = CollectionSchemaParam.newBuilder().addFieldType(fieldType1).addFieldType(fieldType2).build();
    CreateCollectionParam createCollectionReq =
            CreateCollectionParam.newBuilder()
                    .withCollectionName(sparseVectorCollection)
                    .withDescription("Test" + sparseVectorCollection + "search")
                    .withShardsNum(2)
                    .withSchema(schemaParam)
                    .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
    Assert.assertEquals(collection.getStatus().toString(), "0");
    Assert.assertEquals(collection.getData().getMsg(), "Success");
  }

  @Test(description = "Create collection with multi vector ",groups = {"Smoke"})
  public void createCollectionWithMultiVector(){
    multiVectorCollection = "Collection_" + MathUtil.getRandomString(10);
    FieldType fieldType1 =
            FieldType.newBuilder()
                    .withName("book_id")
                    .withDataType(DataType.Int64)
                    .withPrimaryKey(true)
                    .withAutoID(false)
                    .build();
    FieldType fieldType2 =
            FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
    FieldType fieldType3 =
            FieldType.newBuilder()
                    .withName(CommonData.defaultVectorField)
                    .withDataType(DataType.FloatVector)
                    .withDimension(CommonData.dim)
                    .build();
    FieldType fieldType4 =
            FieldType.newBuilder()
                    .withName(CommonData.defaultBinaryVectorField)
                    .withDataType(DataType.BinaryVector)
                    .withDimension(CommonData.dim)
                    .build();
    FieldType fieldType5 =
            FieldType.newBuilder()
                    .withName(CommonData.defaultSparseVectorField)
                    .withDataType(DataType.SparseFloatVector)
                    .build();
    CollectionSchemaParam collectionSchemaParam = CollectionSchemaParam.newBuilder()
            .addFieldType(fieldType1)
            .addFieldType(fieldType2)
            .addFieldType(fieldType3)
            .addFieldType(fieldType4)
            .addFieldType(fieldType5)
            .build();
    CreateCollectionParam createCollectionReq =
            CreateCollectionParam.newBuilder()
                    .withCollectionName(multiVectorCollection)
                    .withDescription("Test" + multiVectorCollection + "search")
                    .withShardsNum(1)
                    .withSchema(collectionSchemaParam)
                    .build();
    R<RpcStatus> collection = BaseTest.milvusClient.createCollection(createCollectionReq);
    Assert.assertEquals(collection.getStatus().toString(), "0");
    Assert.assertEquals(collection.getData().getMsg(), "Success");
  }


}
