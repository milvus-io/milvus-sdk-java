package com.zilliz.milvustest.common;

import com.zilliz.milvustest.MilvustestApplication;
import com.zilliz.milvustest.util.MathUtil;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.MutationResult;
import io.milvus.param.*;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.credential.CreateCredentialParam;
import io.milvus.param.credential.DeleteCredentialParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.partition.CreatePartitionParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.context.web.WebAppConfiguration;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.asserts.SoftAssert;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest(classes = MilvustestApplication.class)
@WebAppConfiguration
public class BaseTest extends AbstractTestNGSpringContextTests {
  public static final Logger logger= LoggerFactory.getLogger(BaseTest.class);
  public static final SoftAssert softAssert = new SoftAssert();
  public static final MilvusServiceClient milvusClient =
      new MilvusServiceClient(
          ConnectParam.newBuilder()
              .withHost(
                  System.getProperty("milvusHost") == null
                      ? PropertyFilesUtil.getRunValue("milvusHost")
                      : System.getProperty("milvusHost"))
              .withPort(
                  Integer.parseInt(
                      System.getProperty("milvusPort") == null
                          ? PropertyFilesUtil.getRunValue("milvusPort")
                          : System.getProperty("milvusPort")))
             //.withAuthorization("root","1qaz@WSX")
             //.withAuthorization("root","Milvus")
              //.withAuthorization("root", "Lyp0107!")
             //.withAuthorization(CommonData.defaultUserName,CommonData.defaultPassword)
              //.withSecure(true)
              .build());

  @BeforeSuite(alwaysRun = true)
  public void initCollection() {
    logger.info(
        "**************************************************BeforeSuit**********************");
    initEvn();
    // check collection is existed
    List<String> collections =
        new ArrayList<String>() {
          {
            add(CommonData.defaultCollection);
            add(CommonData.defaultBinaryCollection);
            add(CommonData.defaultStringPKCollection);
            add(CommonData.defaultStringPKBinaryCollection);
          }
        };
    checkCollection(collections);
    // create collection with  float Vector
    initFloatVectorCollection();
    // create collection with binary vector
    initBinaryVectorCollection();
    // create String PK collection with  float Vector
    initStringPKCollection();
    // create String PK collection with  binary Vector
    initStringPKBinaryCollection();

  }

  @AfterSuite(alwaysRun = true)
  public void cleanTestData() {
    logger.info(
        "**************************************************AfterSuit**********************");
    logger.info("drop Default Collection");
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(CommonData.defaultCollection).build());
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .build());
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder()
                    .withCollectionName(CommonData.defaultStringPKCollection)
                    .build());
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder()
                    .withCollectionName(CommonData.defaultStringPKBinaryCollection)
                    .build());
    logger.info("delete Default Credential:" + CommonData.defaultUserName);
    milvusClient.deleteCredential(
        DeleteCredentialParam.newBuilder().withUsername(CommonData.defaultUserName).build());
    milvusClient.close();
  }

  public void initEvn() {
    logger.info("Initializing the Environment");
    // delete allure-result
    logger.info("Deletes all history json in the  allure-result folder");
    MathUtil.delAllFile("allure-results");
    // write environment for allure
    String filename = "./allure-results/environment.properties";
    Path path = Paths.get(filename);
    String contentStr =
        "milvus.url="
            + PropertyFilesUtil.getRunValue("milvusHost")
            + "\n"
            + "milvus.version="
            + PropertyFilesUtil.getRunValue("milvusV")
            + "\n"
            + "milvus-jdk-java.version="
            + PropertyFilesUtil.getRunValue("milvusJdkJavaV")
            + "";
    try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
      writer.write(contentStr);
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
  }

  // generate collection with binary vector
  public void initBinaryVectorCollection() {
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
            .withName(CommonData.defaultBinaryVectorField)
            .withDataType(DataType.BinaryVector)
            .withDimension(128)
            .build();
    CreateCollectionParam createCollectionReq =
        CreateCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withDescription("Test " + CommonData.defaultBinaryCollection + " search")
            .withShardsNum(2)
            .addFieldType(fieldType1)
            .addFieldType(fieldType2)
            .addFieldType(fieldType3)
            .build();
    logger.info("Create binary vector collection:" + CommonData.defaultBinaryCollection);
    milvusClient.createCollection(createCollectionReq);
    logger.info(
        CommonData.defaultBinaryCollection
            + "Create Partition:"
            + CommonData.defaultBinaryPartition);
    milvusClient.createPartition(
        CreatePartitionParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withPartitionName(CommonData.defaultBinaryPartition)
            .build());
    logger.info(
        (CommonData.defaultBinaryCollection + "Create Alies:" + CommonData.defaultBinaryAlias));
    milvusClient.createAlias(
        CreateAliasParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withAlias(CommonData.defaultBinaryAlias)
            .build());
    logger.info(
        CommonData.defaultBinaryCollection + "Create Index:" + CommonData.defaultBinaryIndex);
    milvusClient.createIndex(
        CreateIndexParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withFieldName(CommonData.defaultBinaryVectorField)
            .withIndexName(CommonData.defaultBinaryIndex)
            .withMetricType(MetricType.JACCARD)
            .withIndexType(IndexType.BIN_IVF_FLAT)
            .withExtraParam(CommonData.defaultExtraParam)
            .withSyncMode(Boolean.FALSE)
            .build());
    logger.info("insert data");

    milvusClient.insert(
        InsertParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withPartitionName(CommonData.defaultBinaryPartition)
            .withFields(CommonFunction.generateBinaryData(2000))
            .build());
  }

  // generate collection with float vector
  public void initFloatVectorCollection() {
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
            .withDimension(128)
            .build();
    CreateCollectionParam createCollectionReq =
        CreateCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withDescription("Test " + CommonData.defaultCollection + " search")
            .withShardsNum(2)
            .addFieldType(fieldType1)
            .addFieldType(fieldType2)
            .addFieldType(fieldType3)
            .build();
    logger.info("Create Default Collection:" + CommonData.defaultCollection);
    milvusClient.createCollection(createCollectionReq);

    logger.info((CommonData.defaultCollection + "Create Alies:" + CommonData.defaultAlias));
    milvusClient.createAlias(
        CreateAliasParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withAlias(CommonData.defaultAlias)
            .build());

    logger.info(CommonData.defaultCollection + "Create Partition:" + CommonData.defaultPartition);
    milvusClient.createPartition(
        CreatePartitionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionName(CommonData.defaultPartition)
            .build());

    logger.info(CommonData.defaultCollection + "Create Index:" + CommonData.defaultIndex);
    milvusClient.createIndex(
        CreateIndexParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withFieldName(CommonData.defaultVectorField)
            .withIndexName(CommonData.defaultIndex)
            .withMetricType(MetricType.L2)
            .withIndexType(IndexType.IVF_FLAT)
            .withExtraParam(CommonData.defaultExtraParam)
            .withSyncMode(Boolean.FALSE)
            .build());

    logger.info("insert data");
    List<InsertParam.Field> fields = CommonFunction.generateData(2000);
    milvusClient.insert(
        InsertParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionName(CommonData.defaultPartition)
            .withFields(fields)
            .build());
    logger.info("Create Default Credential:" + CommonData.defaultUserName);
    milvusClient.createCredential(
        CreateCredentialParam.newBuilder()
            .withUsername(CommonData.defaultUserName)
            .withPassword(CommonData.defaultPassword)
            .build());
  }

  // generate Collection with String primary key and float vector
  public void initStringPKCollection() {
    FieldType strFieldType1 =
        FieldType.newBuilder()
            .withName("book_name")
            .withDataType(DataType.VarChar)
            .withMaxLength(20)
            .withPrimaryKey(true)
            .withAutoID(false)
            .build();
    FieldType strFieldType2 =
        FieldType.newBuilder()
            .withName("book_content")
            .withDataType(DataType.VarChar)
            .withMaxLength(20)
            .build();
    FieldType strFieldType3 =
        FieldType.newBuilder()
            .withName(CommonData.defaultVectorField)
            .withDataType(DataType.FloatVector)
            .withDimension(128)
            .build();
    CreateCollectionParam createStrCollectionReq =
        CreateCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withDescription("Test" + CommonData.defaultStringPKCollection + "search")
            .withShardsNum(2)
            .addFieldType(strFieldType1)
            .addFieldType(strFieldType2)
            .addFieldType(strFieldType3)
            .build();
    BaseTest.milvusClient.createCollection(createStrCollectionReq);
    logger.info(
        "Create default String pk collection with String type field:" + CommonData.defaultStringPKCollection);

    logger.info((CommonData.defaultStringPKCollection + "Create Alies:" + CommonData.defaultStringPKAlias));
    milvusClient.createAlias(
            CreateAliasParam.newBuilder()
                    .withCollectionName(CommonData.defaultStringPKCollection)
                    .withAlias(CommonData.defaultStringPKAlias)
                    .build());

    logger.info(CommonData.defaultStringPKCollection + "Create Partition:" + CommonData.defaultStringPKPartition);
    milvusClient.createPartition(
            CreatePartitionParam.newBuilder()
                    .withCollectionName(CommonData.defaultStringPKCollection)
                    .withPartitionName(CommonData.defaultStringPKPartition)
                    .build());

    logger.info(CommonData.defaultStringPKCollection + "Create Index:" + CommonData.defaultIndex);
    milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                    .withCollectionName(CommonData.defaultStringPKCollection)
                    .withFieldName(CommonData.defaultVectorField)
                    .withIndexName(CommonData.defaultIndex)
                    .withMetricType(MetricType.L2)
                    .withIndexType(IndexType.IVF_FLAT)
                    .withExtraParam(CommonData.defaultExtraParam)
                    .withSyncMode(Boolean.FALSE)
                    .build());

    logger.info("insert data");
    List<InsertParam.Field> fields = CommonFunction.generateStringData(2000);
    milvusClient.insert(
            InsertParam.newBuilder()
                    .withCollectionName(CommonData.defaultStringPKCollection)
                    .withPartitionName(CommonData.defaultStringPKPartition)
                    .withFields(fields)
                    .build());

  }

  // generate Collection with String primary key and binary vector
  public void initStringPKBinaryCollection() {
    FieldType strFieldType1 =
            FieldType.newBuilder()
                    .withName("book_name")
                    .withDataType(DataType.VarChar)
                    .withMaxLength(20)
                    .withPrimaryKey(true)
                    .withAutoID(false)
                    .build();
    FieldType strFieldType2 =
            FieldType.newBuilder()
                    .withName("book_content")
                    .withDataType(DataType.VarChar)
                    .withMaxLength(20)
                    .build();
    FieldType strFieldType3 =
            FieldType.newBuilder()
                    .withName(CommonData.defaultBinaryVectorField)
                    .withDataType(DataType.BinaryVector)
                    .withDimension(128)
                    .build();
    CreateCollectionParam createStrCollectionReq =
            CreateCollectionParam.newBuilder()
                    .withCollectionName(CommonData.defaultStringPKBinaryCollection)
                    .withDescription("Test" + CommonData.defaultStringPKBinaryCollection + "search")
                    .withShardsNum(2)
                    .addFieldType(strFieldType1)
                    .addFieldType(strFieldType2)
                    .addFieldType(strFieldType3)
                    .build();
    BaseTest.milvusClient.createCollection(createStrCollectionReq);
    logger.info(
            "Create default String pk and  binary vector collection with String type field:" + CommonData.defaultStringPKBinaryCollection);

    logger.info(CommonData.defaultStringPKBinaryCollection + "Create Partition:" + CommonData.defaultStringPKBinaryPartition);
    milvusClient.createPartition(
            CreatePartitionParam.newBuilder()
                    .withCollectionName(CommonData.defaultStringPKBinaryCollection)
                    .withPartitionName(CommonData.defaultStringPKBinaryPartition)
                    .build());

    logger.info(CommonData.defaultStringPKBinaryCollection + "Create Index:" + CommonData.defaultBinaryIndex);
    milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                    .withCollectionName(CommonData.defaultStringPKBinaryCollection)
                    .withFieldName(CommonData.defaultBinaryVectorField)
                    .withIndexName(CommonData.defaultBinaryIndex)
                    .withMetricType(MetricType.JACCARD)
                    .withIndexType(IndexType.BIN_IVF_FLAT)
                    .withExtraParam(CommonData.defaultExtraParam)
                    .withSyncMode(Boolean.FALSE)
                    .build());

    logger.info("insert data");
    List<InsertParam.Field> fields = CommonFunction.generateStringPKBinaryData(2000);
    R<MutationResult> insert = milvusClient.insert(
            InsertParam.newBuilder()
                    .withCollectionName(CommonData.defaultStringPKBinaryCollection)
                    .withPartitionName(CommonData.defaultStringPKBinaryPartition)
                    .withFields(fields)
                    .build());
    logger.info("String pk and binary vector collection insert data"+insert.getStatus());

  }
  public void checkCollection(List<String> collections) {
    collections.forEach(
        x -> {
          R<Boolean> booleanR =
              milvusClient.hasCollection(
                  HasCollectionParam.newBuilder().withCollectionName(x).build());
          if (booleanR.getData()) {
            milvusClient.dropCollection(
                DropCollectionParam.newBuilder().withCollectionName(x).build());
          }
        });
  }
}
