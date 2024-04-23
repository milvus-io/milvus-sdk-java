package com.zilliz.milvustest.common;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.zilliz.milvustest.entity.FileBody;
import com.zilliz.milvustest.entity.MilvusEntity;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.DataType;
import io.milvus.param.*;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.partition.CreatePartitionParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;


import java.nio.ByteBuffer;
import java.util.*;

import static com.zilliz.milvustest.common.BaseTest.milvusClient;

@Component
public class CommonFunction {
  public static Logger logger = LoggerFactory.getLogger(CommonFunction.class);

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

  public static String createNewCollectionWithPartitionKey(int partitionKeyNum) {
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
    FieldType fieldType4 = FieldType.newBuilder()
            .withName(CommonData.defaultPartitionField)
            .withDataType(DataType.VarChar)
            .withMaxLength(128)
            .withPartitionKey(true)
            .build();
    CreateCollectionParam createCollectionReq =
            CreateCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withDescription("Test" + collectionName + "search")
                    .withShardsNum(2)
                    .addFieldType(fieldType1)
                    .addFieldType(fieldType2)
                    .addFieldType(fieldType3)
                    .addFieldType(fieldType4)
                    .withPartitionsNum(partitionKeyNum)
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

  public static String createFloat16Collection(){
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
                    .withName(CommonData.defaultFloat16VectorField)
                    .withDataType(DataType.Float16Vector)
                    .withDimension(CommonData.dim)
                    .build();
    CollectionSchemaParam collectionSchemaParam = CollectionSchemaParam.newBuilder().addFieldType(fieldType1).addFieldType(fieldType2).addFieldType(fieldType3).build();
    CreateCollectionParam createCollectionReq =
            CreateCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withDescription("Test" + collectionName + "search")
                    .withShardsNum(2)
                    .withSchema(collectionSchemaParam)
                    .build();
    R<RpcStatus> collection = BaseTest.milvusClient.createCollection(createCollectionReq);
    logger.info("create collection:" + collectionName);
    return collectionName;
  }
  public static String createBf16Collection(){
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
                    .withName(CommonData.defaultBF16VectorField)
                    .withDataType(DataType.BFloat16Vector)
                    .withDimension(CommonData.dim)
                    .build();
    CollectionSchemaParam collectionSchemaParam = CollectionSchemaParam.newBuilder().addFieldType(fieldType1).addFieldType(fieldType2).addFieldType(fieldType3).build();
    CreateCollectionParam createCollectionReq =
            CreateCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withDescription("Test" + collectionName + "search")
                    .withShardsNum(2)
                    .withSchema(collectionSchemaParam)
                    .build();
    R<RpcStatus> collection = BaseTest.milvusClient.createCollection(createCollectionReq);
    logger.info("create collection:" + collectionName);
    return collectionName;
  }

  public static String createSparseFloatVectorCollection(){
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
                    .withName(CommonData.defaultSparseVectorField)
                    .withDataType(DataType.SparseFloatVector)
                    .build();
    CollectionSchemaParam collectionSchemaParam = CollectionSchemaParam.newBuilder().addFieldType(fieldType1).addFieldType(fieldType2).addFieldType(fieldType3).build();
    CreateCollectionParam createCollectionReq =
            CreateCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withDescription("Test" + collectionName + "search")
                    .withShardsNum(2)
                    .withSchema(collectionSchemaParam)
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

  public static String createMultiVectorCollection(){
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
    FieldType fieldType6 =
            FieldType.newBuilder()
                    .withName(CommonData.defaultFloat16VectorField)
                    .withDataType(DataType.Float16Vector)
                    .withDimension(CommonData.dim)
                    .build();
    FieldType fieldType7 =
            FieldType.newBuilder()
                    .withName(CommonData.defaultBF16VectorField)
                    .withDataType(DataType.BFloat16Vector)
                    .withDimension(CommonData.dim)
                    .build();
    CollectionSchemaParam collectionSchemaParam = CollectionSchemaParam.newBuilder()
            .addFieldType(fieldType1)
            .addFieldType(fieldType2)
            .addFieldType(fieldType3)
            .addFieldType(fieldType4)
            .addFieldType(fieldType5)
            .addFieldType(fieldType6)
//            .addFieldType(fieldType7)
            .build();
    CreateCollectionParam createCollectionReq =
            CreateCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withDescription("Test" + collectionName + "search")
                    .withShardsNum(2)
                    .withSchema(collectionSchemaParam)
                    .build();
    R<RpcStatus> collection = BaseTest.milvusClient.createCollection(createCollectionReq);
    System.out.println("create c:"+collection);
    logger.info("create multi vector collection:" + collectionName);
    return collectionName;
  }

  public static List<InsertParam.Field> generateData(int num) {
    Random ran = new Random();
    List<Long> book_id_array = new ArrayList<>();
    List<Long> word_count_array = new ArrayList<>();
    List<List<Float>> book_intro_array = new ArrayList<>();
    for (long i = 0L; i < num; ++i) {
      book_id_array.add(i);
      word_count_array.add(1L);
      List<Float> vector = new ArrayList<>();
      for (int k = 0; k < 128; ++k) {
        vector.add(ran.nextFloat());
      }
      book_intro_array.add(vector);
    }
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id", book_id_array));
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(new InsertParam.Field(CommonData.defaultVectorField, book_intro_array));
    // logger.info("generateTestData"+ JacksonUtil.serialize(fields));
    return fields;
  }

  public static SortedMap<Long, Float> generateSparseVector() {
    Random ran = new Random();
    SortedMap<Long, Float> sparse = new TreeMap<>();
    int dim = ran.nextInt(CommonData.dim) + 1;
    for (int i = 0; i < dim; ++i) {
      sparse.put((long)ran.nextInt(1000000), ran.nextFloat());
    }
    return sparse;
  }
  public static List<InsertParam.Field> generateDataWithSparseFloatVector(int num) {
    Random ran = new Random();
    List<Long> book_id_array = new ArrayList<>();
    List<Long> word_count_array = new ArrayList<>();
    List<SortedMap<Long, Float>> book_intro_array = new ArrayList<>();
    for (long i = 0L; i < num; ++i) {
      book_id_array.add(i);
      word_count_array.add(1L);
      SortedMap<Long, Float> sparse = generateSparseVector();
      book_intro_array.add(sparse);
    }
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id", book_id_array));
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(new InsertParam.Field(CommonData.defaultSparseVectorField, book_intro_array));
    return fields;
  }

  public static List<InsertParam.Field> generateDataWithMultiVector(int num) {
    Random ran = new Random();
    List<Long> book_id_array = new ArrayList<>();
    List<Long> word_count_array = new ArrayList<>();

    List<SortedMap<Long, Float>> sparseVectorList = new ArrayList<>();
    for (long i = 0L; i < num; ++i) {
      book_id_array.add(i);
      word_count_array.add(1L);
      SortedMap<Long, Float> sparse = generateSparseVector();
      sparseVectorList.add(sparse);
    }
    List<List<Float>> floatVector = generateFloatVectors(num,CommonData.dim);
    List<ByteBuffer> binaryVectorList = generateBinaryVectors(num, CommonData.dim);
    List<ByteBuffer> float16VectorList = generateFloat16Vectors(CommonData.dim,num);
    List<ByteBuffer> bf16VectorList = generateBF16Vectors(CommonData.dim, num);
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id", book_id_array));
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(new InsertParam.Field(CommonData.defaultVectorField, floatVector));
    fields.add(new InsertParam.Field(CommonData.defaultSparseVectorField, sparseVectorList));
    fields.add(new InsertParam.Field(CommonData.defaultBinaryVectorField, binaryVectorList));
    fields.add(new InsertParam.Field(CommonData.defaultFloat16VectorField,float16VectorList));
//    fields.add(new InsertParam.Field(CommonData.defaultBF16VectorField,bf16VectorList));
    return fields;
  }
  public static List<InsertParam.Field> generateDataWithBF16Vector(int num) {
    List<Long> book_id_array = new ArrayList<>();
    List<Long> word_count_array = new ArrayList<>();
    List<ByteBuffer> book_intro_array = generateBF16Vectors(CommonData.dim, num);
    for (long i = 0L; i < num; ++i) {
      book_id_array.add(i);
      word_count_array.add(1L);
    }
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id", book_id_array));
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(new InsertParam.Field(CommonData.defaultBF16VectorField, book_intro_array));
    // logger.info("generateTestData"+ JacksonUtil.serialize(fields));
    return fields;
  }

  public static List<InsertParam.Field> generateDataWithFloat16Vector(int num) {
    Random ran = new Random();
    List<Long> book_id_array = new ArrayList<>();
    List<Long> word_count_array = new ArrayList<>();
    List<ByteBuffer> book_intro_array = generateFloat16Vectors(CommonData.dim, num);
    for (long i = 0L; i < num; ++i) {
      book_id_array.add(i);
      word_count_array.add(1L);
    }
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id", book_id_array));
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(new InsertParam.Field(CommonData.defaultFloat16VectorField, book_intro_array));
    // logger.info("generateTestData"+ JacksonUtil.serialize(fields));
    return fields;
  }

  public static List<ByteBuffer> generateFloat16Vectors(int VECTOR_DIM,int count) {
    Random ran = new Random();
    List<ByteBuffer> vectors = new ArrayList<>();
    int byteCount = VECTOR_DIM*2;
    for (int n = 0; n < count; ++n) {
      ByteBuffer vector = ByteBuffer.allocate(byteCount);
      for (int i = 0; i < VECTOR_DIM; ++i) {
        short halfFloatValue = MathUtil.floatToFloat16(count+0.1f);
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putShort(halfFloatValue);
        buffer.flip();
        vector.put(buffer.get(0));
        vector.put(buffer.get(1));
      }
      vectors.add(vector);
    }

    return vectors;
  }
  public static List<ByteBuffer> generateBF16Vectors(int VECTOR_DIM,int count) {
    List<ByteBuffer> vectors = new ArrayList<>();
    int byteCount = VECTOR_DIM*2;
    for (int n = 0; n < count; ++n) {
      ByteBuffer vector = ByteBuffer.allocate(byteCount);
      for (int i = 0; i < VECTOR_DIM; ++i) {
        short halfFloatValue = MathUtil.floatToBF16(count+0.1f);
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putShort(halfFloatValue);
        buffer.flip();
        vector.put(buffer.get(0));
        vector.put(buffer.get(1));
      }
      vectors.add(vector);
    }
    return vectors;
  }
  public static List<InsertParam.Field> generateDataWithPartitionKey(int num) {
    Random ran = new Random();
    List<Long> book_id_array = new ArrayList<>();
    List<Long> word_count_array = new ArrayList<>();
    List<String> book_name_array=new ArrayList<>();
    List<List<Float>> book_intro_array = new ArrayList<>();
    for (long i = 0L; i < num; ++i) {
      book_id_array.add(i);
      word_count_array.add(i + 10000);
      book_name_array.add("part"+i/1000);
      List<Float> vector = new ArrayList<>();
      for (int k = 0; k < 128; ++k) {
        vector.add(ran.nextFloat());
      }
      book_intro_array.add(vector);
    }
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id", book_id_array));
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(new InsertParam.Field(CommonData.defaultPartitionField, book_name_array));
    fields.add(new InsertParam.Field(CommonData.defaultVectorField, book_intro_array));
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
    fields.add(new InsertParam.Field(CommonData.defaultVectorField, book_intro_array));
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
    fields.add(new InsertParam.Field(CommonData.defaultBinaryVectorField, book_intro_array));
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

  public static List<List<Float>> generateFloatVectors(int count, int dimension) {
    Random ran = new Random();
    List<List<Float>> vectors = new ArrayList<>();
    for(int i = 0; i < count; i++) {
      List<Float> item= Arrays.asList(MathUtil.generateFloat(dimension));
      vectors.add(item);
    }
    return vectors;
  }

  public static List<InsertParam.Field> generateStringData(int num) {
    Random ran = new Random();
    List<String> book_name_array = new ArrayList<>();
    List<String> book_content_array = new ArrayList<>();
    List<List<Float>> book_intro_array = new ArrayList<>();
    for (long i = 0L; i < num; ++i) {
      book_name_array.add(MathUtil.genRandomStringAndChinese(5) + "-" + i);
      book_content_array.add(i + "-" + MathUtil.genRandomStringAndChinese(5));
      List<Float> vector = new ArrayList<>();
      for (int k = 0; k < 128; ++k) {
        vector.add(ran.nextFloat());
      }
      book_intro_array.add(vector);
    }
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_name", book_name_array));
    fields.add(new InsertParam.Field("book_content", book_content_array));
    fields.add(new InsertParam.Field(CommonData.defaultVectorField, book_intro_array));
    //    logger.info("Generate String and Chinese Data"+ JacksonUtil.serialize(fields));
    return fields;
  }

  public static List<InsertParam.Field> generateStringPKBinaryData(int num) {
    Random ran = new Random();
    List<String> book_name_array = new ArrayList<>();
    List<String> book_content_array = new ArrayList<>();
    for (long i = 0L; i < num; ++i) {
      book_name_array.add(MathUtil.genRandomStringAndChinese(5) + "-" + i);
      book_content_array.add(i + "-" + MathUtil.genRandomStringAndChinese(5));
    }
    List<ByteBuffer> book_intro_array = generateBinaryVectors(num, 128);
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_name", book_name_array));
    fields.add(new InsertParam.Field("book_content", book_content_array));
    fields.add(new InsertParam.Field(CommonData.defaultBinaryVectorField, book_intro_array));
    //    logger.info("generateTestData" + JacksonUtil.serialize(fields));
    return fields;
  }

  // provide extra param
  public static String provideExtraParam(IndexType indexType) {
    String extraParam;
    switch (indexType) {
      case FLAT:
        extraParam = "{}";
        break;
      case IVF_FLAT:
        extraParam = "{\"nlist\":32,\"nprobe\":32}";
        break;
      case IVF_SQ8:
        extraParam = "{\"nlist\":128}";
        break;
      case IVF_PQ:
        extraParam = "{\"nlist\":128, \"m\":16, \"nbits\":8}";
        break;
/*      case ANNOY:
        extraParm = "{\"n_trees\":16}";
        break;*/
      case HNSW:
        extraParam = "{\"M\":16,\"efConstruction\":64}";
        break;
     /* case RHNSW_FLAT:
        extraParam = "{\"M\":16,\"efConstruction\":64}";
        break;*/
/*      case RHNSW_PQ:
        extraParam = "{\"M\":16,\"efConstruction\":64, \"PQM\":16}";
        break;
      case RHNSW_SQ:
        extraParam = "{\"M\":16,\"efConstruction\":64}";
        break;*/
      case BIN_IVF_FLAT:
        extraParam = "{\"nlist\": 128}";
        break;
      case SCANN:
        extraParam="{\"nlist\":1024,\"with_raw_data\":"+true+"}";
        break;
      case GPU_IVF_FLAT:
        extraParam="{\"nlist\": 64}";
        break;
      case GPU_IVF_PQ:
        extraParam="{\"nlist\": 64, \"m\": 16, \"nbits\": 8}";
        break;
      case SPARSE_INVERTED_INDEX:
      case SPARSE_WAND:
        extraParam="{\"drop_ratio_search\":0.2}";
        break;
      default:
        extraParam = "{\"nlist\":128}";
        break;
    }
    return extraParam;
  }

  public static String createNewCollectionWithDynamicField() {
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
                    .withEnableDynamicField(true)
                    .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
    logger.info("create collection:" + collectionName);
    return collectionName;
  }
  public static String createNewCollectionWithJSONField() {
    String collectionName = "Collection_" + MathUtil.getRandomString(10);
    FieldType fieldType1 =
            FieldType.newBuilder()
                    .withName("int64_field")
                    .withDataType(DataType.Int64)
                    .withPrimaryKey(true)
                    .withAutoID(false)
                    .build();
    FieldType fieldType2 =
            FieldType.newBuilder().withName("float_field").withDataType(DataType.Float).build();
    FieldType fieldType3 =
            FieldType.newBuilder()
                    .withName("float_vector")
                    .withDataType(DataType.FloatVector)
                    .withDimension(128)
                    .build();
    FieldType fieldType4 = FieldType.newBuilder()
            .withName("boolean_field")
            .withDataType(DataType.Bool)
            .build();
    FieldType fieldType5 = FieldType.newBuilder()
            .withName("string_field")
            .withDataType(DataType.VarChar)
            .withMaxLength(100)
            .build();
    FieldType fieldType6 = FieldType.newBuilder()
            .withName("json_field")
            .withDataType(DataType.JSON)
            .build();
//    FieldType fieldType7 = FieldType.newBuilder()
//            .withName("array_field")
//            .withDataType(DataType.Array)
//            .build();
    CreateCollectionParam createCollectionReq =
            CreateCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withDescription("Test" + collectionName + "search")
                    .withShardsNum(2)
                    .addFieldType(fieldType1)
                    .addFieldType(fieldType2)
                    .addFieldType(fieldType3)
                    .addFieldType(fieldType4)
                    .addFieldType(fieldType5)
                    .addFieldType(fieldType6)
//                    .addFieldType(fieldType7)
                    .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
    logger.info("create collection:" + collectionName);
    return collectionName;
  }

  public static String createNewCollectionWithArrayField(){
    String collectionName = "Collection_" + MathUtil.getRandomString(10);
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
                    .withCollectionName(collectionName)
                    .withDescription("Test" + collectionName + "search")
                    .withShardsNum(2)
                    .addFieldType(fieldType1)
                    .addFieldType(fieldType2)
                    .addFieldType(fieldType3)
                    .addFieldType(fieldType4)
                    .addFieldType(fieldType5)
                    .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
    logger.info("create collection:" + collectionName);
    return collectionName;
  }

  public static List<JSONObject> generateJsonData(int num){
    List<JSONObject> jsonList=new ArrayList<>();
    Random ran = new Random();
    for (int i = 0; i < num; i++) {
      JSONObject row=new JSONObject();
      row.put("int64_field",(long)i);
      row.put("float_field",(float)i);
      List<Float> vector=new ArrayList<>();
      for (int k = 0; k < 128; ++k) {
        vector.add(ran.nextFloat());
      }
      row.put("float_vector",vector);
      row.put("string_field","Str"+i);
      row.put("boolean_field",i % 2 == 0);
      JSONObject jsonObject=new JSONObject();
      jsonObject.put("int64_field", (long)i);
      jsonObject.put("float_field", (float)i);
      jsonObject.put("string_field", "Str"+i);
      jsonObject.put("boolean_field",i % 2 == 0);
      jsonObject.put("int8", (short)i);
      jsonObject.put("floatF", (float)i);
      jsonObject.put("doubleF", (double)i);
      jsonObject.put("bool", i % 2 == 0);
      jsonObject.put("array_field",Arrays.asList(i,i+1,i+2));
      // $innerJson
      JSONObject innerJson = new JSONObject();
      innerJson.put("int64", (long)i);
      innerJson.put("varchar", "Str"+i);
      innerJson.put("int16", i);
      innerJson.put("int32", i);
      innerJson.put("int8", (short)i);
      innerJson.put("floatF", (float)i);
      innerJson.put("doubleF", (double)i);
      innerJson.put("bool", i % 2 == 0);
      jsonObject.put("inner_json",innerJson);
      row.put("json_field",jsonObject);
      jsonList.add(row);
    }
    return jsonList;
  }

  public static List<JSONObject> generateJsonDataWithArrayField(int num){
    List<JSONObject> jsonList=new ArrayList<>();
    Random ran = new Random();
    for (int i = 0; i < num; i++) {
      JSONObject row=new JSONObject();
      row.put("int64_field",(long)i);
      List<Float> vector=new ArrayList<>();
      for (int k = 0; k < 128; ++k) {
        vector.add(ran.nextFloat());
      }
      row.put("float_vector",vector);
      row.put("str_array_field", Lists.newArrayList("str"+i,"str"+(i+1),"str"+(i+2)));
      row.put("int_array_field",Lists.newArrayList((long)i,(long)(i+1),(long)(i+2)));
      row.put("float_array_field",Lists.newArrayList((float)(i+0.1),(float)(i+0.2),(float)(i+0.2)));
      jsonList.add(row);
    }
    return jsonList;
  }
  public static List<InsertParam.Field> generateDataWithDynamicFiledColumn(int num) {
    Random ran = new Random();
    List<Long> book_id_array = new ArrayList<>();
    List<Long> word_count_array = new ArrayList<>();
    List<List<Float>> book_intro_array = new ArrayList<>();
    List<String> dynamic_extra_array=new ArrayList<>();
    for (long i = 0L; i < num; ++i) {
      book_id_array.add(i);
      word_count_array.add(i + 10000);
      dynamic_extra_array.add("String"+i);
      List<Float> vector = new ArrayList<>();
      for (int k = 0; k < 128; ++k) {
        vector.add(ran.nextFloat());
      }
      book_intro_array.add(vector);
    }
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id", book_id_array));
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(new InsertParam.Field(CommonData.defaultVectorField, book_intro_array));
    fields.add(new InsertParam.Field("extra_field", dynamic_extra_array));
    // logger.info("generateTestData"+ JacksonUtil.serialize(fields));
    return fields;
  }
  public static List<JSONObject> generateDataWithDynamicFiledRow(int num) {
    List<JSONObject> jsonList = new ArrayList<>();
    Random ran = new Random();
    for (int i = 0; i < num; i++) {
      JSONObject row = new JSONObject();
      row.put("book_id", (long) i);
      row.put("word_count", (long) i);
      row.put("extra_field", "String" + i);
      row.put("extra_field2",  i);
      // $innerJson
      JSONObject innerJson = new JSONObject();
      innerJson.put("int64", (long) i);
      innerJson.put("varchar", "varchar"+i);
      innerJson.put("int16", i);
      innerJson.put("int32", i);
      innerJson.put("int8", (short)i);
      innerJson.put("float", (float)i);
      innerJson.put("double", (double)i);
      innerJson.put("bool", i % 2 == 0);
      row.put("json_field", innerJson);
      List<Float> vector = new ArrayList<>();
      for (int k = 0; k < 128; ++k) {
        vector.add(ran.nextFloat());
      }
      row.put("book_intro", vector);
      jsonList.add(row);
    }
    return jsonList;
  }
  public static List<JSONObject> generateVarcharPKDataWithDynamicFiledRow(int num) {
    List<JSONObject> jsonList = new ArrayList<>();
    Random ran = new Random();
    for (int i = 0; i < num; i++) {
      JSONObject row = new JSONObject();
      row.put("book_name",  "StringPK"+i);
      row.put("word_count", (long) i);
      row.put("extra_field", "String" + i);
      row.put("extra_field2",  i);
      // $innerJson
      JSONObject innerJson = new JSONObject();
      innerJson.put("int64", (long) i);
      innerJson.put("varchar", "varchar"+i);
      innerJson.put("int16", i);
      innerJson.put("int32", i);
      innerJson.put("int8", (short)i);
      innerJson.put("float", (float)i);
      innerJson.put("double", (double)i);
      innerJson.put("bool", i % 2 == 0);
      row.put("json_field", innerJson);
      List<Float> vector = new ArrayList<>();
      for (int k = 0; k < 128; ++k) {
        vector.add(ran.nextFloat());
      }
      row.put("book_intro", vector);
      jsonList.add(row);
    }
    return jsonList;
  }
  public static void createIndexWithoutLoad(String collection, IndexType indexType, MetricType metricType, String fieldName){
    milvusClient.flush(FlushParam.newBuilder().withCollectionNames(Lists.newArrayList(collection)).build());
    R<RpcStatus> rpcStatusR =
            milvusClient.createIndex(
                    CreateIndexParam.newBuilder()
                            .withCollectionName(collection)
                            .withFieldName(fieldName)
                            .withMetricType(metricType)
                            .withIndexType(indexType)
                            .withIndexName("idx_"+fieldName)
                            .withExtraParam(CommonFunction.provideExtraParam(indexType))
                            .withSyncMode(Boolean.FALSE)
                            .build());


  }
  public static void createIndexWithLoad(String collection, IndexType indexType, MetricType metricType, String fieldName){
    milvusClient.flush(FlushParam.newBuilder().withCollectionNames(Lists.newArrayList(collection)).withSyncFlush(true).build());
    R<RpcStatus> rpcStatusR =
            milvusClient.createIndex(
                    CreateIndexParam.newBuilder()
                            .withCollectionName(collection)
                            .withFieldName(fieldName)
                            .withIndexName("idx_"+fieldName)
                            .withMetricType(metricType)
                            .withIndexType(indexType)
                            .withExtraParam(CommonFunction.provideExtraParam(indexType))
                            .withSyncMode(Boolean.FALSE)
                            .build());
    System.out.println("Create index" + rpcStatusR);
    milvusClient.loadCollection(LoadCollectionParam.newBuilder()
            .withCollectionName(collection)
            .withSyncLoad(true)
            .withSyncLoadWaitingTimeout(30L)
            .withSyncLoadWaitingInterval(50L)
            .build());

  }

  public static void prepareCollectionForSearch(String collection,String database){
    List<InsertParam.Field> fields = generateData(2000);
    milvusClient.insert(InsertParam.newBuilder()
            .withCollectionName(collection)
            .withDatabaseName(database)
            .withFields(fields).build());
    milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                    .withCollectionName(collection)
                    .withFieldName(CommonData.defaultVectorField)
                    .withIndexName(CommonData.defaultIndex)
                    .withMetricType(MetricType.L2)
                    .withIndexType(IndexType.HNSW)
                    .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                    .withSyncMode(Boolean.FALSE)
                    .withDatabaseName(database)
                    .build());
    milvusClient.loadCollection(LoadCollectionParam.newBuilder()
            .withCollectionName(collection)
            .withDatabaseName(database)
            .withSyncLoad(true)
            .withSyncLoadWaitingTimeout(30L)
            .withSyncLoadWaitingInterval(50L)
            .build());

  }

  public static void clearCollection(String collection,String database){
    milvusClient.releaseCollection(ReleaseCollectionParam.newBuilder()
            .withCollectionName(collection).build());
    milvusClient.dropCollection(DropCollectionParam.newBuilder()
            .withCollectionName(collection)
            .withDatabaseName(database).build());
  }

  public static void insertDataIntoCollection(String collection,String database,int num){
    List<InsertParam.Field> fields = generateData(num);
    milvusClient.insert(InsertParam.newBuilder()
            .withCollectionName(collection)
            .withDatabaseName(database)
            .withFields(fields).build());
  }

  public  static  CollectionSchemaParam provideJsonCollectionSchema(){
    FieldType fieldType1 =
            FieldType.newBuilder()
                    .withName("int64_field")
                    .withDataType(DataType.Int64)
                    .withPrimaryKey(true)
                    .withAutoID(false)
                    .build();
    FieldType fieldType2 =
            FieldType.newBuilder().withName("float_field").withDataType(DataType.Float).build();
    FieldType fieldType3 =
            FieldType.newBuilder()
                    .withName("float_vector")
                    .withDataType(DataType.FloatVector)
                    .withDimension(128)
                    .build();
    FieldType fieldType4 = FieldType.newBuilder()
            .withName("boolean_field")
            .withDataType(DataType.Bool)
            .build();
    FieldType fieldType5 = FieldType.newBuilder()
            .withName("string_field")
            .withDataType(DataType.VarChar)
            .withMaxLength(100)
            .build();
    FieldType fieldType6 = FieldType.newBuilder()
            .withName("json_field")
            .withDataType(DataType.JSON)
            .build();
//    FieldType fieldType7 = FieldType.newBuilder()
//            .withName("array_field")
//            .withDataType(DataType.Array)
//            .build();
    return CollectionSchemaParam.newBuilder()
            .addFieldType(fieldType1)
            .addFieldType(fieldType2)
            .addFieldType(fieldType3)
            .addFieldType(fieldType4)
            .addFieldType(fieldType5)
            .addFieldType(fieldType6).build();
  }

  // 实现float到float16的转换
  private static short floatToFloat16(float value) {
    int floatBits = Float.floatToIntBits(value);
    int sign = (floatBits >> 16) & 0x8000; // 符号位
    int exponent = ((floatBits >> 23) & 0xFF) - 127 + 15; // 指数位
    int significand = floatBits & 0x7FFFFF; // 尾数位

    if (exponent <= 0) {
      if (exponent < -10) {
        // 浮点数太小，直接转为0
        return (short) sign;
      }
      // 将尾数位向右移动
      significand = (significand | 0x800000) >> (1 - exponent);
      // 考虑舍入
      if ((significand & 0x1000) == 0x1000) {
        significand += 0x2000;
        if ((significand & 0x800000) == 0x800000) {
          significand = 0;
          exponent++;
        }
      }
      exponent = 0;
    } else if (exponent == 0xFF - 127 + 15) {
      if (significand == 0) {
        // 正无穷大
        return (short) (sign | 0x7C00);
      } else {
        // NaN
        significand >>= 13;
        return (short) (sign | 0x7C00 | significand | (significand == 0 ? 1 : 0));
      }
    } else {
      // 正常情况下，只需要移动尾数位
      if ((significand & 0x1000) == 0x1000) {
        significand += 0x2000;
        if ((significand & 0x800000) == 0x800000) {
          significand = 0;
          exponent++;
        }
      }
      if (exponent > 30) {
        // 浮点数太大，转为正无穷大
        return (short) (sign | 0x7C00);
      }
    }

    return (short) (sign | (exponent << 10) | (significand >> 13));
  }

}
