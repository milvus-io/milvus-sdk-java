package com.zilliz.milvustest.vdc;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @Author yongpeng.li @Date 2022/8/23 14:41
 */
@Epic("Collection")
@Feature("VDC")
public class VectorSearch extends BaseTest {


  @Test(description = "AutoIndex,DB:Bigdata")
  public void col_AutoIndex() {
    // Check if the collection exists
    String collectionName = "Col_AutoIndex";
    int dim = 128;
    Random ran = new Random();
    R<Boolean> bookR =
            milvusClient.hasCollection(
                    HasCollectionParam.newBuilder().withCollectionName(collectionName).build());
    if (bookR.getData()) {
      R<RpcStatus> dropR =
              milvusClient.dropCollection(
                      DropCollectionParam.newBuilder().withCollectionName(collectionName).build());
      System.out.println(
              "Collection "
                      + collectionName
                      + " is existed,Drop collection: "
                      + dropR.getData().getMsg());
    }
    // create a collection with customized primary field: book_id_field

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
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionParam);
    Assert.assertEquals(collection.getStatus().intValue(),0);
    logger.info("create collection " + collectionName + " successfully");

    // insert data with customized ids

    int singleNum = 10000;
    int insertRounds = 2;
    long insertTotalTime = 0L;
    for (int r = 0; r < insertRounds; r++) {
      List<Long> book_id_array = new ArrayList<>();
      List<Long> word_count_array = new ArrayList<>();
      List<List<Float>> book_intro_array = new ArrayList<>();
      for (long i = r * singleNum; i < (r + 1) * singleNum; ++i) {
        book_id_array.add(i);
        word_count_array.add(i + 10000);
        List<Float> vector = new ArrayList<>();
        for (int k = 0; k < dim; ++k) {
          vector.add(ran.nextFloat());
        }
        book_intro_array.add(vector);
      }
      List<InsertParam.Field> fields = new ArrayList<>();
      fields.add(new InsertParam.Field(bookIdField.getName(), book_id_array));
      fields.add(new InsertParam.Field(wordCountField.getName(), word_count_array));
      fields.add(new InsertParam.Field(bookIntroField.getName(), book_intro_array));
      InsertParam insertParam =
              InsertParam.newBuilder().withCollectionName(collectionName).withFields(fields).build();
      long startTime = System.currentTimeMillis();
      R<MutationResult> insertR = milvusClient.insert(insertParam);
      long endTime = System.currentTimeMillis();
      insertTotalTime += (endTime - startTime) / 1000.0;
    }
    logger.info(
            "totally insert "
                    + singleNum * insertRounds
                    + " entities cost "
                    + insertTotalTime
                    + " seconds");
    // flush
    R<FlushResponse> flush = milvusClient.flush(
            FlushParam.newBuilder()
                    .withCollectionNames(Arrays.asList(collectionName))
                    .withSyncFlush(true)
                    .withSyncFlushWaitingInterval(500L)
                    .withSyncFlushWaitingTimeout(30L)
                    .build());
    Assert.assertEquals(flush.getStatus().intValue(),0);
    // build index
    final IndexType INDEX_TYPE = IndexType.AUTOINDEX; // IndexType
    final String INDEX_PARAM = "{\"nlist\":128}"; // ExtraParam
    long startIndexTime = System.currentTimeMillis();
    R<RpcStatus> indexR =
            milvusClient.createIndex(
                    CreateIndexParam.newBuilder()
                            .withCollectionName(collectionName)
                            .withFieldName(bookIntroField.getName())
                            .withIndexType(INDEX_TYPE)
                            .withMetricType(MetricType.L2)
                            .withExtraParam(INDEX_PARAM)
                            .withSyncMode(Boolean.TRUE)
                            .withSyncWaitingInterval(500L)
                            .withSyncWaitingTimeout(30L)
                            .build());
    Assert.assertEquals(indexR.getStatus().intValue(),0);
    long endIndexTime = System.currentTimeMillis();
    logger.info(
            "collection "
                    + collectionName
                    + " build index in "
                    + (endIndexTime - startIndexTime) / 1000.0
                    + " seconds");
    // load collection
    long startLoadTime = System.currentTimeMillis();
    R<RpcStatus> load = milvusClient.loadCollection(
            LoadCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withSyncLoad(true)
                    .withSyncLoadWaitingInterval(500L)
                    .withSyncLoadWaitingTimeout(30L)
                    .build());
    Assert.assertEquals(load.getStatus().intValue(),0);
    long endLoadTime = System.currentTimeMillis();
    logger.info(
            "collection "
                    + collectionName
                    + " load in "
                    + (endLoadTime - startLoadTime) / 1000.0
                    + " seconds");
    // query
    String query_PARAM = "book_id in [2,4,6,8]";
    List<String> outFields = Arrays.asList("book_id", "word_count", "book_intro");
    QueryParam queryParam =
            QueryParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withOutFields(outFields)
                    .withExpr(query_PARAM)
                    .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    Assert.assertEquals(queryResultsR.getStatus().intValue(), -3);
    Assert.assertTrue(queryResultsR.getMessage().contains("permission deny"));
    // search
    final Integer SEARCH_K = 2; // TopK
    final String SEARCH_PARAM = "{\"nprobe\":1}"; // Params
    List<String> search_output_fields = Arrays.asList("book_id", "word_count");
    for (int i = 0; i < 10; i++) {
      List<Float> floatList = new ArrayList<>();
      for (int k = 0; k < dim; ++k) {
        floatList.add(ran.nextFloat());
      }
      logger.info(floatList.toString());
      List<List<Float>> search_vectors = Arrays.asList(floatList);
      SearchParam searchParam =
              SearchParam.newBuilder()
                      .withCollectionName(collectionName)
                      .withMetricType(MetricType.L2)
                      .withOutFields(search_output_fields)
                      .withTopK(SEARCH_K)
                      .withVectors(search_vectors)
                      .withVectorFieldName("book_intro")
                      .withRoundDecimal(4)
                      .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                      .withParams(SEARCH_PARAM)
                      .build();
      long startSearchTime = System.currentTimeMillis();
      R<SearchResults> search = milvusClient.search(searchParam);
      Assert.assertEquals(search.getStatus().intValue(),0);
      logger.info("search result:" + search.getData());
      long endSearchTime = System.currentTimeMillis();
     logger.info(
              "search " + i + " latency: " + (endSearchTime - startSearchTime) / 1000.0 + " seconds");
    }
  }
  @Test
  public void col_Empty_Entity() {
    // Check if the collection exists
    String collectionName = "Col_Empty_Entity";
    int dim = 128;
    Random ran = new Random();
    R<Boolean> bookR =
            milvusClient.hasCollection(
                    HasCollectionParam.newBuilder().withCollectionName(collectionName).build());
    if (bookR.getData()) {
      R<RpcStatus> dropR =
              milvusClient.dropCollection(
                      DropCollectionParam.newBuilder().withCollectionName(collectionName).build());
      logger.info(
              "Collection "
                      + collectionName
                      + " is existed,Drop collection: "
                      + dropR.getData().getMsg());
    }

    // create a collection with customized primary field: book_id_field

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
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionParam);
    Assert.assertEquals(collection.getStatus().intValue(),0);
    logger.info("create collection " + collectionName + " successfully");


    // flush
    R<FlushResponse> flush = milvusClient.flush(
            FlushParam.newBuilder()
                    .withCollectionNames(Arrays.asList(collectionName))
                    .withSyncFlush(true)
                    .withSyncFlushWaitingInterval(500L)
                    .withSyncFlushWaitingTimeout(30L)
                    .build());
    Assert.assertEquals(flush.getStatus().intValue(),0);

    // build index
    final IndexType INDEX_TYPE = IndexType.AUTOINDEX; // IndexType
    final String INDEX_PARAM = "{\"nlist\":128}"; // ExtraParam
    long startIndexTime = System.currentTimeMillis();
    R<RpcStatus> indexR =
            milvusClient.createIndex(
                    CreateIndexParam.newBuilder()
                            .withCollectionName(collectionName)
                            .withFieldName(bookIntroField.getName())
                            .withIndexType(INDEX_TYPE)
                            .withMetricType(MetricType.L2)
                            .withExtraParam(INDEX_PARAM)
                            .withSyncMode(Boolean.TRUE)
                            .withSyncWaitingInterval(500L)
                            .withSyncWaitingTimeout(30L)
                            .build());
    Assert.assertEquals(indexR.getStatus().intValue(),0);

    long endIndexTime = System.currentTimeMillis();
    logger.info(
            "collection "
                    + collectionName
                    + " build index in "
                    + (endIndexTime - startIndexTime) / 1000.0
                    + " seconds");
    // load collection
    long startLoadTime = System.currentTimeMillis();
    R<RpcStatus> rpcStatusR = milvusClient.loadCollection(
            LoadCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withSyncLoad(true)
                    .withSyncLoadWaitingInterval(500L)
                    .withSyncLoadWaitingTimeout(30L)
                    .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(),0);

    long endLoadTime = System.currentTimeMillis();
    logger.info(
            "collection "
                    + collectionName
                    + " load in "
                    + (endLoadTime - startLoadTime) / 1000.0
                    + " seconds");


  }
  @Test
  public void col_AutoIndex_IP() {
    // Check if the collection exists
    String collectionName = "Col_AutoIndex_IP";
    R<Boolean> bookR =
            milvusClient.hasCollection(
                    HasCollectionParam.newBuilder().withCollectionName(collectionName).build());
    if (bookR.getData()) {
      R<RpcStatus> dropR =
              milvusClient.dropCollection(
                      DropCollectionParam.newBuilder().withCollectionName(collectionName).build());
      logger.info(
              "Collection "
                      + collectionName
                      + " is existed,Drop collection: "
                      + dropR.getData().getMsg());
    }

    // create a collection with customized primary field: book_id_field
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
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionParam);
    Assert.assertEquals(collection.getStatus().intValue(),0);

    logger.info("create collection " + collectionName + " successfully");

    // insert data with customized ids
    Random ran = new Random();
    int singleNum = 1000;
    int insertRounds = 1000;
    long insertTotalTime = 0L;
    for (int r = 0; r < insertRounds; r++) {
      List<Long> book_id_array = new ArrayList<>();
      List<Long> word_count_array = new ArrayList<>();
      List<List<Float>> book_intro_array = new ArrayList<>();
      for (long i = r * singleNum; i < (r + 1) * singleNum; ++i) {
        book_id_array.add(i);
        word_count_array.add(i + 10000);
        List<Float> vector = new ArrayList<>();
        for (int k = 0; k < dim; ++k) {
          vector.add(ran.nextFloat());
        }
        book_intro_array.add(vector);
      }
      List<InsertParam.Field> fields = new ArrayList<>();
      fields.add(new InsertParam.Field(bookIdField.getName(), book_id_array));
      fields.add(new InsertParam.Field(wordCountField.getName(), word_count_array));
      fields.add(new InsertParam.Field(bookIntroField.getName(), book_intro_array));
      InsertParam insertParam =
              InsertParam.newBuilder().withCollectionName(collectionName).withFields(fields).build();
      long startTime = System.currentTimeMillis();
      R<MutationResult> insertR = milvusClient.insert(insertParam);
      Assert.assertEquals(insertR.getStatus().intValue(),0);
      long endTime = System.currentTimeMillis();
      insertTotalTime += (endTime - startTime) / 1000.0;
    }
    logger.info(
            "totally insert "
                    + singleNum * insertRounds
                    + " entities cost "
                    + insertTotalTime
                    + " seconds");
    // flush
    R<FlushResponse> flush = milvusClient.flush(
            FlushParam.newBuilder()
                    .withCollectionNames(Arrays.asList(collectionName))
                    .withSyncFlush(true)
                    .withSyncFlushWaitingInterval(500L)
                    .withSyncFlushWaitingTimeout(30L)
                    .build());
    Assert.assertEquals(flush.getStatus().intValue(),0);
    // build index
    final IndexType INDEX_TYPE = IndexType.AUTOINDEX; // IndexType
    final String INDEX_PARAM = "{\"n_trees\":16}"; // ExtraParam
    long startIndexTime = System.currentTimeMillis();
    R<RpcStatus> indexR =
            milvusClient.createIndex(
                    CreateIndexParam.newBuilder()
                            .withCollectionName(collectionName)
                            .withFieldName(bookIntroField.getName())
                            .withIndexType(INDEX_TYPE)
                            .withMetricType(MetricType.IP)
                            .withExtraParam(INDEX_PARAM)
                            .withSyncMode(Boolean.TRUE)
                            .withSyncWaitingInterval(500L)
                            .withSyncWaitingTimeout(30L)
                            .build());
    Assert.assertEquals(indexR.getStatus().intValue(),0);
    long endIndexTime = System.currentTimeMillis();
    logger.info(
            "collection "
                    + collectionName
                    + " build index in "
                    + (endIndexTime - startIndexTime) / 1000.0
                    + " seconds");
    // load collection
    long startLoadTime = System.currentTimeMillis();
    R<RpcStatus> rpcStatusR = milvusClient.loadCollection(
            LoadCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withSyncLoad(true)
                    .withSyncLoadWaitingInterval(500L)
                    .withSyncLoadWaitingTimeout(30L)
                    .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(),0);
    long endLoadTime = System.currentTimeMillis();
    logger.info(
            "collection "
                    + collectionName
                    + " load in "
                    + (endLoadTime - startLoadTime) / 1000.0
                    + " seconds");

    // search
    final Integer SEARCH_K = 2; // TopK
    final String SEARCH_PARAM = "{\"search_k\":-1}"; // Params
    List<String> search_output_fields = Arrays.asList("book_id", "word_count");
    for (int i = 0; i < 10; i++) {
      List<Float> floatList = new ArrayList<>();
      for (int k = 0; k < dim; ++k) {
        floatList.add(ran.nextFloat());
      }
      logger.info(floatList.toString());
      List<List<Float>> search_vectors = Arrays.asList(floatList);
      SearchParam searchParam =
              SearchParam.newBuilder()
                      .withCollectionName(collectionName)
                      .withMetricType(MetricType.IP)
                      .withOutFields(search_output_fields)
                      .withTopK(SEARCH_K)
                      .withVectors(search_vectors)
                      .withVectorFieldName(bookIntroField.getName())
                      .withParams(SEARCH_PARAM)
                      .build();
      long startSearchTime = System.currentTimeMillis();
      R<SearchResults> search = milvusClient.search(searchParam);
      Assert.assertEquals(search.getStatus().intValue(),0);
      logger.info("search result:" + search.getData());
      long endSearchTime = System.currentTimeMillis();
      logger.info(
              "search " + i + " latency: " + (endSearchTime - startSearchTime) / 1000.0 + " seconds");
    }
  }
  @Test
  public void col_Multi_Field() {
    // Check if the collection exists
    String collectionName = "Col_Multiple_Field";
    int dim = 128;
    Random ran = new Random();
    R<Boolean> bookR =
            milvusClient.hasCollection(
                    HasCollectionParam.newBuilder().withCollectionName(collectionName).build());
    if (bookR.getData()) {
      R<RpcStatus> dropR =
              milvusClient.dropCollection(
                      DropCollectionParam.newBuilder().withCollectionName(collectionName).build());
      logger.info(
              "Collection "
                      + collectionName
                      + " is existed,Drop collection: "
                      + dropR.getData().getMsg());
    }

    // create a collection with customized primary field: book_id_field

    FieldType PKField =
            FieldType.newBuilder()
                    .withName("PKField")
                    .withDataType(DataType.Int64)
                    .withPrimaryKey(true)
                    .withAutoID(false)
                    .build();
    FieldType int8Field = FieldType.newBuilder().withName("int8Field").withDataType(DataType.Int8).build();
    FieldType int16Field = FieldType.newBuilder().withName("int16Field").withDataType(DataType.Int16).build();
    FieldType int32Field = FieldType.newBuilder().withName("int32Field").withDataType(DataType.Int32).build();
    FieldType int64Field = FieldType.newBuilder().withName("int64Field").withDataType(DataType.Int64).build();
    FieldType doubleField = FieldType.newBuilder().withName("doubleField").withDataType(DataType.Double).build();
    FieldType floatField = FieldType.newBuilder().withName("floatField").withDataType(DataType.Float).build();
    FieldType varCharField = FieldType.newBuilder().withName("varCharField").withDataType(DataType.VarChar)
            .withMaxLength(20).build();
    FieldType booleanField = FieldType.newBuilder().withName("booleanField").withDataType(DataType.Bool).build();
    FieldType floatVectorField =
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
                    .addFieldType(PKField)
                    .addFieldType(int8Field)
                    .addFieldType(int16Field)
                    .addFieldType(int32Field)
                    .addFieldType(int64Field)
                    .addFieldType(doubleField)
                    .addFieldType(floatField)
                    .addFieldType(varCharField)
                    .addFieldType(booleanField)
                    .addFieldType(floatVectorField)
                    .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionParam);
    Assert.assertEquals(collection.getStatus().intValue(),0);
    logger.info("create collection " + collectionName + " successfully");

    // insert data with customized ids

    int singleNum = 10000;
    int insertRounds = 2;
    long insertTotalTime = 0L;
    for (int r = 0; r < insertRounds; r++) {
      List<Long>  PKField_array= new ArrayList<>();
      List<Short> int8Field_array = new ArrayList<>();
      List<Short> int16Field_array = new ArrayList<>();
      List<Integer> int32Field_array = new ArrayList<>();
      List<Long> int64Field_array = new ArrayList<>();
      List<Double> doubleField_array = new ArrayList<>();
      List<Float> floatField_array = new ArrayList<>();
      List<String> varCharField_array = new ArrayList<>();
      List<Boolean> booleanField_array = new ArrayList<>();
      List<List<Float>> floatVectorField_array = new ArrayList<>();

      for (long i = r * singleNum; i < (r + 1) * singleNum; ++i) {
        PKField_array.add(i);
        int8Field_array.add((short) ran.nextInt(1000));
        int16Field_array.add((short) ran.nextInt(1000));
        int32Field_array.add(ran.nextInt());
        int64Field_array.add(ran.nextLong());
        doubleField_array.add(ran.nextDouble());
        floatField_array.add(ran.nextFloat());
        varCharField_array.add(MathUtil.genRandomStringAndChinese(10));
        booleanField_array.add(ran.nextBoolean());
        List<Float> vector = new ArrayList<>();
        for (int k = 0; k < dim; ++k) {
          vector.add(ran.nextFloat());
        }
        floatVectorField_array.add(vector);
      }
      List<InsertParam.Field> fields = new ArrayList<>();
      fields.add(new InsertParam.Field(PKField.getName(), PKField_array));
      fields.add(new InsertParam.Field(int8Field.getName(), int8Field_array));
      fields.add(new InsertParam.Field(int16Field.getName(), int16Field_array));
      fields.add(new InsertParam.Field(int32Field.getName(), int32Field_array));
      fields.add(new InsertParam.Field(int64Field.getName(), int64Field_array));
      fields.add(new InsertParam.Field(doubleField.getName(), doubleField_array));
      fields.add(new InsertParam.Field(floatField.getName(), floatField_array));
      fields.add(new InsertParam.Field(varCharField.getName(), varCharField_array));
      fields.add(new InsertParam.Field(booleanField.getName(), booleanField_array));
      fields.add(new InsertParam.Field(floatVectorField.getName(), floatVectorField_array));
      InsertParam insertParam =
              InsertParam.newBuilder().withCollectionName(collectionName).withFields(fields).build();
      long startTime = System.currentTimeMillis();
      R<MutationResult> insertR = milvusClient.insert(insertParam);
      Assert.assertEquals(insertR.getStatus().intValue(),0);
      long endTime = System.currentTimeMillis();
      insertTotalTime += (endTime - startTime) / 1000.0;
    }
    logger.info(
            "totally insert "
                    + singleNum * insertRounds
                    + " entities cost "
                    + insertTotalTime
                    + " seconds");
    // flush
    R<FlushResponse> flush = milvusClient.flush(
            FlushParam.newBuilder()
                    .withCollectionNames(Arrays.asList(collectionName))
                    .withSyncFlush(true)
                    .withSyncFlushWaitingInterval(500L)
                    .withSyncFlushWaitingTimeout(30L)
                    .build());
    Assert.assertEquals(flush.getStatus().intValue(),0);
    // build index
    final IndexType INDEX_TYPE = IndexType.AUTOINDEX; // IndexType
    final String INDEX_PARAM = "{\"nlist\":128}"; // ExtraParam
    long startIndexTime = System.currentTimeMillis();
    R<RpcStatus> indexR =
            milvusClient.createIndex(
                    CreateIndexParam.newBuilder()
                            .withCollectionName(collectionName)
                            .withFieldName(floatVectorField.getName())
                            .withIndexType(INDEX_TYPE)
                            .withMetricType(MetricType.L2)
                            .withExtraParam(INDEX_PARAM)
                            .withSyncMode(Boolean.TRUE)
                            .withSyncWaitingInterval(500L)
                            .withSyncWaitingTimeout(30L)
                            .build());
    Assert.assertEquals(indexR.getStatus().intValue(),0);
    long endIndexTime = System.currentTimeMillis();
    logger.info(
            "collection "
                    + collectionName
                    + " build index in "
                    + (endIndexTime - startIndexTime) / 1000.0
                    + " seconds");
    // load collection
    long startLoadTime = System.currentTimeMillis();
    R<RpcStatus> rpcStatusR = milvusClient.loadCollection(
            LoadCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withSyncLoad(true)
                    .withSyncLoadWaitingInterval(500L)
                    .withSyncLoadWaitingTimeout(30L)
                    .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(),0);
    long endLoadTime = System.currentTimeMillis();
    logger.info(
            "collection "
                    + collectionName
                    + " load in "
                    + (endLoadTime - startLoadTime) / 1000.0
                    + " seconds");
    // search
    final Integer SEARCH_K = 2; // TopK
    final String SEARCH_PARAM = "{\"nprobe\":1}"; // Params
    List<String> search_output_fields = Arrays.asList(int8Field.getName(), int16Field.getName());
    for (int i = 0; i < 10; i++) {
      List<Float> floatList = new ArrayList<>();
      for (int k = 0; k < dim; ++k) {
        floatList.add(ran.nextFloat());
      }
      logger.info(floatList.toString());
      List<List<Float>> search_vectors = Arrays.asList(floatList);
      SearchParam searchParam =
              SearchParam.newBuilder()
                      .withCollectionName(collectionName)
                      .withMetricType(MetricType.L2)
                      .withOutFields(search_output_fields)
                      .withTopK(SEARCH_K)
                      .withVectors(search_vectors)
                      .withVectorFieldName(floatVectorField.getName())
                      .withRoundDecimal(4)
                      .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                      .withParams(SEARCH_PARAM)
                      .build();
      long startSearchTime = System.currentTimeMillis();
      R<SearchResults> search = milvusClient.search(searchParam);
      Assert.assertEquals(search.getStatus().intValue(),0);
      logger.info("search result:" + search.getData());
      long endSearchTime = System.currentTimeMillis();
      logger.info(
              "search " + i + " latency: " + (endSearchTime - startSearchTime) / 1000.0 + " seconds");
    }
  }
}
