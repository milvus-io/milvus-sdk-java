package com.zilliz.milvustest.common;

import com.zilliz.milvustest.entity.FileBody;
import com.zilliz.milvustest.entity.MilvusEntity;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.DataType;
import io.milvus.param.IndexType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.partition.CreatePartitionParam;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Component
public class CommonFunction {
  static Logger logger = Logger.getLogger(CommonFunction.class);

  // int PK, float vector collection
  public static String createNewCollection() {
    String collectionName = "Collection_" + MathUtil.getRandomString(10);
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
            .withCollectionName(collectionName)
            .withDescription("Test" + collectionName + "search")
            .withShardsNum(2)
            .addFieldType(fieldType1)
            .addFieldType(fieldType2)
            .addFieldType(fieldType3)
            .build();
    R<RpcStatus> collection = BaseTest.milvusClient.createCollection(createCollectionReq);
    logger.info("create collection:" + collectionName);
    return collectionName;
  }
  public static String createNewCollectionWithAutoPK() {
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
                    .withDimension(128)
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
    logger.info("create collection:" + collectionName);
    return collectionName;
  }

  // int PK,  binary vector collection
  public static String createBinaryCollection() {
    String collectionName = "Collection_" + MathUtil.getRandomString(10);
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
            .withCollectionName(collectionName)
            .withDescription("Test" + collectionName + "search")
            .withShardsNum(2)
            .addFieldType(fieldType1)
            .addFieldType(fieldType2)
            .addFieldType(fieldType3)
            .build();
    R<RpcStatus> collection = BaseTest.milvusClient.createCollection(createCollectionReq);
    logger.info("create collection:" + collectionName);
    return collectionName;
  }

  // String pk,float vector collection
  public static String createStringPKCollection() {
    String collectionName = "Collection_" + MathUtil.getRandomString(10);
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
            .withMaxLength(20)
            .build();
    FieldType fieldType3 =
        FieldType.newBuilder()
            .withName(CommonData.defaultVectorField)
            .withDataType(DataType.FloatVector)
            .withDimension(128)
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
    logger.info("生成collection:" + collectionName);
    return collectionName;
  }

  // String PK, binary vector collection
  public static String createStringPKAndBinaryCollection() {
    String collectionName = "Collection_" + MathUtil.getRandomString(10);
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
            .withMaxLength(20)
            .build();
    FieldType fieldType3 =
        FieldType.newBuilder()
            .withName(CommonData.defaultBinaryVectorField)
            .withDataType(DataType.BinaryVector)
            .withDimension(128)
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
    logger.info("Create String pk and binary vector collection:" + collectionName);
    return collectionName;
  }

  public static MilvusEntity createCollectionWithAll() {
    MilvusEntity milvusEntity = new MilvusEntity();
    milvusEntity.setCollection("Collection_" + MathUtil.getRandomString(10));
    milvusEntity.setAlias("alias_" + MathUtil.getRandomString(5));
    milvusEntity.setPartition("partition_" + MathUtil.getRandomString(10));
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
            .withCollectionName(milvusEntity.getCollection())
            .withDescription("Test" + milvusEntity.getCollection() + "search")
            .withShardsNum(2)
            .addFieldType(fieldType1)
            .addFieldType(fieldType2)
            .addFieldType(fieldType3)
            .build();
    BaseTest.milvusClient.createCollection(createCollectionReq);
    BaseTest.milvusClient.createPartition(
        CreatePartitionParam.newBuilder()
            .withCollectionName(milvusEntity.getCollection())
            .withPartitionName(milvusEntity.getPartition())
            .build());
    BaseTest.milvusClient.createAlias(
        CreateAliasParam.newBuilder()
            .withCollectionName(milvusEntity.getCollection())
            .withAlias(milvusEntity.getAlias())
            .build());

    return milvusEntity;
  }

  public static List<InsertParam.Field> generateData(int num) {
    Random ran = new Random();
    List<Long> book_id_array = new ArrayList<>();
    List<Long> word_count_array = new ArrayList<>();
    List<List<Float>> book_intro_array = new ArrayList<>();
    for (long i = 0L; i < num; ++i) {
      book_id_array.add(i);
      word_count_array.add(i + 10000);
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
    // logger.info("generateTestData"+ JacksonUtil.serialize(fields));
    return fields;
  }
  public static List<InsertParam.Field> generateDataWithAutoPK(int num) {
    Random ran = new Random();
    List<Long> word_count_array = new ArrayList<>();
    List<List<Float>> book_intro_array = new ArrayList<>();
    for (long i = 0L; i < num; ++i) {
      word_count_array.add(i + 10000);
      List<Float> vector = new ArrayList<>();
      for (int k = 0; k < 128; ++k) {
        vector.add(ran.nextFloat());
      }
      book_intro_array.add(vector);
    }
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(
            new InsertParam.Field(
                    CommonData.defaultVectorField, book_intro_array));
    // logger.info("generateTestData"+ JacksonUtil.serialize(fields));
    return fields;
  }

  public static void insertDataIntoCollection(String collection, List<InsertParam.Field> fields) {
    BaseTest.milvusClient.insert(
        InsertParam.newBuilder().withCollectionName(collection).withFields(fields).build());
  }

  public static List<FileBody> generateDefaultFileBody() {
    List<FileBody> fileBodyList = new ArrayList<>();
    FileBody bookid = new FileBody();
    bookid.setFieldName("book_id");
    bookid.setFieldType(com.zilliz.milvustest.entity.FieldType.PK_FIELD);
    FileBody words = new FileBody();
    words.setFieldName("word_count");
    words.setFieldType(com.zilliz.milvustest.entity.FieldType.INT_FIELD);
    FileBody vectorField = new FileBody();
    vectorField.setFieldName(CommonData.defaultVectorField);
    vectorField.setFieldType(com.zilliz.milvustest.entity.FieldType.FLOAT_VECTOR_FIELD);
    fileBodyList.add(bookid);
    fileBodyList.add(words);
    fileBodyList.add(vectorField);
    return fileBodyList;
  }

  public static List<FileBody> generateDefaultStringFileBody() {
    List<FileBody> fileBodyList = new ArrayList<>();
    FileBody bookid = new FileBody();
    bookid.setFieldName("book_name");
    bookid.setFieldType(com.zilliz.milvustest.entity.FieldType.STRING_PK_FIELD);
    FileBody words = new FileBody();
    words.setFieldName("book_content");
    words.setFieldType(com.zilliz.milvustest.entity.FieldType.STRING_FIELD);
    FileBody vectorField = new FileBody();
    vectorField.setFieldName(CommonData.defaultVectorField);
    vectorField.setFieldType(com.zilliz.milvustest.entity.FieldType.FLOAT_VECTOR_FIELD);
    fileBodyList.add(bookid);
    fileBodyList.add(words);
    fileBodyList.add(vectorField);
    return fileBodyList;
  }

  public static List<InsertParam.Field> generateBinaryData(int num) {
    Random ran = new Random();
    List<Long> book_id_array = new ArrayList<>();
    List<Long> word_count_array = new ArrayList<>();
    for (long i = 0L; i < num; ++i) {
      book_id_array.add(i);
      word_count_array.add(i + 10000);
    }
    List<ByteBuffer> book_intro_array = generateBinaryVectors(num, 128);
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id", book_id_array));
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(
        new InsertParam.Field(
            CommonData.defaultBinaryVectorField, book_intro_array));
    //    logger.info("generateTestData" + JacksonUtil.serialize(fields));
    return fields;
  }

  public static List<ByteBuffer> generateBinaryVectors(int count, int dimension) {
    Random ran = new Random();
    List<ByteBuffer> vectors = new ArrayList<>();
    int byteCount = dimension / 8;
    for (int n = 0; n < count; ++n) {
      ByteBuffer vector = ByteBuffer.allocate(byteCount);
      // logger.info("generate No."+n+" binary vector" );
      for (int i = 0; i < byteCount; ++i) {
        vector.put((byte) ran.nextInt(Byte.MAX_VALUE));
        // logger.info("generateBinaryVector:"+(byte) ran.nextInt(Byte.MAX_VALUE));
      }
      vectors.add(vector);
    }
    return vectors;
  }

  public static List<InsertParam.Field> generateStringData(int num) {
    Random ran = new Random();
    List<String> book_name_array = new ArrayList<>();
    List<String> book_content_array = new ArrayList<>();
    List<List<Float>> book_intro_array = new ArrayList<>();
    for (long i = 0L; i < num; ++i) {
      book_name_array.add(MathUtil.genRandomStringAndChinese(10) + "-" + i);
      book_content_array.add(i + "-" + MathUtil.genRandomStringAndChinese(10));
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
    //    logger.info("Generate String and Chinese Data"+ JacksonUtil.serialize(fields));
    return fields;
  }

  public static List<InsertParam.Field> generateStringPKBinaryData(int num) {
    Random ran = new Random();
    List<String> book_name_array = new ArrayList<>();
    List<String> book_content_array = new ArrayList<>();
    for (long i = 0L; i < num; ++i) {
      book_name_array.add(MathUtil.genRandomStringAndChinese(10) + "-" + i);
      book_content_array.add(i + "-" + MathUtil.genRandomStringAndChinese(10));
    }
    List<ByteBuffer> book_intro_array = generateBinaryVectors(num, 128);
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_name", book_name_array));
    fields.add(new InsertParam.Field("book_content", book_content_array));
    fields.add(
        new InsertParam.Field(
            CommonData.defaultBinaryVectorField, book_intro_array));
    //    logger.info("generateTestData" + JacksonUtil.serialize(fields));
    return fields;
  }

  // provide extra param
  public static String provideExtraParam(IndexType indexType) {
    String extraParm;
    switch (indexType) {
      case FLAT:
        extraParm = "{}";
        break;
      case IVF_FLAT:
        extraParm = "{\"nlist\":128}";
        break;
      case IVF_SQ8:
        extraParm = "{\"nlist\":128}";
        break;
      case IVF_PQ:
        extraParm = "{\"nlist\":128, \"m\":16, \"nbits\":8}";
        break;
      case ANNOY:
        extraParm = "{\"n_trees\":16}";
        break;
      case HNSW:
        extraParm = "{\"M\":16,\"efConstruction\":64}";
        break;
      case RHNSW_FLAT:
        extraParm = "{\"M\":16,\"efConstruction\":64}";
        break;
      case RHNSW_PQ:
        extraParm = "{\"M\":16,\"efConstruction\":64, \"PQM\":16}";
        break;
      case RHNSW_SQ:
        extraParm = "{\"M\":16,\"efConstruction\":64}";
        break;
      default:
        extraParm = "";
        break;
    }
    return extraParm;
  }
}
