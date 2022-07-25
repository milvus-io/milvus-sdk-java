package com.zilliz.milvustest.collection;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.exception.ParamException;
import io.milvus.grpc.DataType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
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

@Epic("Collection")
@Feature("CreateCollection")
public class CreateCollectionTest extends BaseTest {
  public String commonCollection;
  public String binaryVectorCollection;

  public String stringPKCollection;

  @DataProvider(name = "collectionByDataProvider")
  public Object[][] provideCollectionName() {
    return new String[][] {{"collection_" + MathUtil.getRandomString(10)}};
  }

  @AfterClass(description = "delete test datas after CreateCollectionTest",alwaysRun=true)
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
  @Test(description = "Create collection success", dataProvider = "collectionByDataProvider",groups = {"Smoke"})
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
}
