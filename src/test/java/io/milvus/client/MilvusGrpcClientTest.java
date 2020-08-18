/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.client;

import com.google.common.util.concurrent.ListenableFuture;
import io.milvus.client.InsertParam.Builder;
import io.milvus.client.Response.Status;
import org.apache.commons.text.RandomStringGenerator;
import org.json.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MilvusClientTest {

  private MilvusClient client;

  private RandomStringGenerator generator;

  private String randomCollectionName;
  private int size;
  private int dimension;

  // Helper function that generates random float vectors
  static List<List<Float>> generateFloatVectors(int vectorCount, int dimension) {
    SplittableRandom splittableRandom = new SplittableRandom();
    List<List<Float>> vectors = new ArrayList<>(vectorCount);
    for (int i = 0; i < vectorCount; ++i) {
      splittableRandom = splittableRandom.split();
      DoubleStream doubleStream = splittableRandom.doubles(dimension);
      List<Float> vector =
          doubleStream.boxed().map(Double::floatValue).collect(Collectors.toList());
      vectors.add(vector);
    }
    return vectors;
  }

  // Helper function that generates random binary vectors
  static List<ByteBuffer> generateBinaryVectors(int vectorCount, int dimension) {
    Random random = new Random();
    List<ByteBuffer> vectors = new ArrayList<>(vectorCount);
    final int dimensionInByte = dimension / 8;
    for (int i = 0; i < vectorCount; ++i) {
      ByteBuffer byteBuffer = ByteBuffer.allocate(dimensionInByte);
      random.nextBytes(byteBuffer.array());
      vectors.add(byteBuffer);
    }
    return vectors;
  }

  // Helper function that normalizes a vector if you are using IP (Inner Product) as your metric
  // type
  static List<Float> normalizeVector(List<Float> vector) {
    float squareSum = vector.stream().map(x -> x * x).reduce((float) 0, Float::sum);
    final float norm = (float) Math.sqrt(squareSum);
    vector = vector.stream().map(x -> x / norm).collect(Collectors.toList());
    return vector;
  }

  // Helper function that generates default fields list for a collection
  static List<Map<String, Object>> generateDefaultFields(int dimension) {
    List<Map<String, Object>> fieldList = new ArrayList<>();
    Map<String, Object> intField = new HashMap<>();
    intField.put("field", "int64");
    intField.put("type", DataType.INT64);

    Map<String, Object> floatField = new HashMap<>();
    floatField.put("field", "float");
    floatField.put("type", DataType.FLOAT);

    Map<String, Object> vecField = new HashMap<>();
    vecField.put("field", "float_vec");
    vecField.put("type", DataType.VECTOR_FLOAT);
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("dim", dimension);
    vecField.put("params", jsonObject.toString());

    fieldList.add(intField);
    fieldList.add(floatField);
    fieldList.add(vecField);
    return fieldList;
  }

  // Helper function that generates entity field values for inserting into a collection
  static List<Map<String, Object>> generateDefaultFieldValues(int vectorCount, int dimension) {
    List<Map<String, Object>> fieldList = new ArrayList<>();
    Map<String, Object> intField = new HashMap<>();
    intField.put("field", "int64");
    intField.put("type", DataType.INT64);

    Map<String, Object> floatField = new HashMap<>();
    floatField.put("field", "float");
    floatField.put("type", DataType.FLOAT);

    Map<String, Object> vecField = new HashMap<>();
    vecField.put("field", "float_vec");
    vecField.put("type", DataType.VECTOR_FLOAT);

    List<Long> intValues = new ArrayList<>(vectorCount);
    List<Float> floatValues = new ArrayList<>(vectorCount);
    List<List<Float>> vectors = generateFloatVectors(vectorCount, dimension);
    for (int i = 0; i < vectorCount; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    intField.put("values", intValues);
    floatField.put("values", floatValues);
    vecField.put("values", vectors);

    fieldList.add(intField);
    fieldList.add(floatField);
    fieldList.add(vecField);
    return fieldList;
  }

  // Helper function that generate a simple DSL statement with vector filtering only
  static String generateSimpleDSL(Long topK, String query) {
    return String.format(
        "{\"bool\": {"
            + "\"must\": [{"
            + "    \"vector\": {"
            + "        \"float_vec\": {"
            + "            \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"float\", \"query\": %s, \"params\": {\"nprobe\": 20}"
            + "    }}}]}}", topK, query);
  }

  // Helper function that generate a complex DSL statement with scalar field filtering
  static String generateComplexDSL(Long topK, String query) {
    return String.format(
        "{\"bool\": {"
            + "\"must\": [{"
            + "    \"range\": {"
            + "        \"float\": {\"GT\": -10, \"LT\": 100}"
            + "    }},{"
            + "    \"vector\": {"
            + "        \"float_vec\": {"
            + "            \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"float\", \"query\": %s, \"params\": {\"nprobe\": 20}"
            + "    }}}]}}",
        topK, query);
  }

  // Helper function that generate a complex DSL statement with scalar field filtering
  static String generateComplexDSLBinary(Long topK, String placeholder) {
    return String.format(
        "{\"bool\": {"
            + "\"must\": [{"
            + "    \"range\": {"
            + "        \"float\": {\"GT\": -10, \"LT\": 100}"
            + "    }},{"
            + "    \"vector\": {"
            + "        \"binary_vec\": {"
            + "            \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"binary\", \"query\": %s, \"params\": {\"nprobe\": 20}"
            + "    }}}]}}",
        topK, placeholder);
  }

  @org.junit.jupiter.api.BeforeEach
  void setUp() throws Exception {

    client = new MilvusGrpcClient();
    ConnectParam connectParam =
        new ConnectParam.Builder().withHost("localhost").withPort(19530).build();
    client.connect(connectParam);

    generator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();
    randomCollectionName = generator.generate(10);
    size = 100000;
    dimension = 128;
    CollectionMapping collectionMapping =
        new CollectionMapping.Builder(randomCollectionName)
            .withFields(generateDefaultFields(dimension))
            .withParamsInJson("{\"segment_row_count\": 50000, \"auto_id\": false}")
            .build();

    assertTrue(client.createCollection(collectionMapping).ok());
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() throws InterruptedException {
    assertTrue(client.dropCollection(randomCollectionName).ok());
    client.disconnect();
  }

  @org.junit.jupiter.api.Test
  void idleTest() throws InterruptedException, ConnectFailedException {
    MilvusClient client = new MilvusGrpcClient();
    ConnectParam connectParam =
        new ConnectParam.Builder()
            .withHost("localhost")
            .withIdleTimeout(1, TimeUnit.SECONDS)
            .build();
    client.connect(connectParam);
    TimeUnit.SECONDS.sleep(2);
    // A new RPC would take the channel out of idle mode
    assertTrue(client.listCollections().ok());
  }

  @org.junit.jupiter.api.Test
  void setInvalidConnectParam() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          ConnectParam connectParam = new ConnectParam.Builder().withPort(66666).build();
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          ConnectParam connectParam =
              new ConnectParam.Builder().withConnectTimeout(-1, TimeUnit.MILLISECONDS).build();
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          ConnectParam connectParam =
              new ConnectParam.Builder().withKeepAliveTime(-1, TimeUnit.MILLISECONDS).build();
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          ConnectParam connectParam =
              new ConnectParam.Builder().withKeepAliveTimeout(-1, TimeUnit.MILLISECONDS).build();
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          ConnectParam connectParam =
              new ConnectParam.Builder().withIdleTimeout(-1, TimeUnit.MILLISECONDS).build();
        });
  }

  @org.junit.jupiter.api.Test
  void connectUnreachableHost() {
    MilvusClient client = new MilvusGrpcClient();
    ConnectParam connectParam = new ConnectParam.Builder().withHost("250.250.250.250").build();
    assertThrows(ConnectFailedException.class, () -> client.connect(connectParam));
  }

  @org.junit.jupiter.api.Test
  void createInvalidCollection() {
    // invalid collection name
    String invalidCollectionName = "╯°□°）╯";
    List<Map<String, Object>> defaultField = generateDefaultFields(dimension);
    CollectionMapping invalidCollectionMapping =
        new CollectionMapping.Builder(invalidCollectionName)
            .withFields(defaultField)
            .build();
    Response createCollectionResponse = client.createCollection(invalidCollectionMapping);
    assertFalse(createCollectionResponse.ok());
    assertEquals(Response.Status.ILLEGAL_COLLECTION_NAME, createCollectionResponse.getStatus());

    // invalid field
    defaultField.get(0).remove("type");
    invalidCollectionMapping =
        new CollectionMapping.Builder("validCollectionName")
            .withFields(defaultField)
            .build();
    createCollectionResponse = client.createCollection(invalidCollectionMapping);
    assertFalse(createCollectionResponse.ok());

    // invalid segment_row_count
    invalidCollectionMapping =
        new CollectionMapping.Builder("validCollectionName")
            .withFields(generateDefaultFields(dimension))
            .withParamsInJson("{\"segment_row_count\": -1000}")
            .build();
    createCollectionResponse = client.createCollection(invalidCollectionMapping);
    assertFalse(createCollectionResponse.ok());
    assertEquals(Status.ILLEGAL_ARGUMENT, createCollectionResponse.getStatus());
  }

  @org.junit.jupiter.api.Test
  void hasCollection() {
    HasCollectionResponse hasCollectionResponse = client.hasCollection(randomCollectionName);
    assertTrue(hasCollectionResponse.ok());
    assertTrue(hasCollectionResponse.hasCollection());
  }

  @org.junit.jupiter.api.Test
  void dropCollection() {
    String nonExistingCollectionName = generator.generate(10);
    Response dropCollectionResponse = client.dropCollection(nonExistingCollectionName);
    assertFalse(dropCollectionResponse.ok());
    assertEquals(Response.Status.COLLECTION_NOT_EXISTS, dropCollectionResponse.getStatus());
  }

  @Test
  void partitionTest() {
    final String tag1 = "tag1";
    Response createPartitionResponse = client.createPartition(randomCollectionName, tag1);
    assertTrue(createPartitionResponse.ok());

    final String tag2 = "tag2";
    createPartitionResponse = client.createPartition(randomCollectionName, tag2);
    assertTrue(createPartitionResponse.ok());

    ListPartitionsResponse listPartitionsResponse = client.listPartitions(randomCollectionName);
    assertTrue(listPartitionsResponse.ok());
    assertEquals(3, listPartitionsResponse.getPartitionList().size()); // two tags plus _default

    List<Map<String, Object>> defaultFieldValues1 = generateDefaultFieldValues(size, dimension);
    List<Long> entityIds1 = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .withFields(defaultFieldValues1)
            .withEntityIds(entityIds1)
            .withPartitionTag(tag1)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<Map<String, Object>> defaultFieldValues2 = generateDefaultFieldValues(size, dimension);
    List<Long> entityIds2 = LongStream.range(size, size * 2).boxed().collect(Collectors.toList());
    insertParam =
        new Builder(randomCollectionName)
            .withFields(defaultFieldValues2)
            .withEntityIds(entityIds2)
            .withPartitionTag(tag2)
            .build();
    insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());

    assertTrue(client.flush(randomCollectionName).ok());

    assertEquals(size * 2,
        client.countEntities(randomCollectionName).getCollectionEntityCount());

    final int searchSize = 1;
    final long topK = 10;

    assertTrue(defaultFieldValues1.get(2).get("values") instanceof List);
    List<List<Float>> vectors1 = (List<List<Float>>) defaultFieldValues1.get(2).get("values");
    List<List<Float>> vectorsToSearch1 = vectors1.subList(0, searchSize);
    List<String> partitionTags1 = new ArrayList<>();
    partitionTags1.add(tag1);
    SearchParam searchParam1 =
        new SearchParam.Builder(randomCollectionName)
            .withDSL(generateSimpleDSL(topK, vectorsToSearch1.toString()))
            .withPartitionTags(partitionTags1)
            .build();
    SearchResponse searchResponse1 = client.search(searchParam1);
    assertTrue(searchResponse1.ok());
    List<List<Long>> resultIdsList1 = searchResponse1.getResultIdsList();
    assertEquals(searchSize, resultIdsList1.size());
    assertTrue(entityIds1.containsAll(resultIdsList1.get(0)));

    List<List<Float>> vectors2 = (List<List<Float>>) defaultFieldValues2.get(2).get("values");
    List<List<Float>> vectorsToSearch2 = vectors2.subList(0, searchSize);
    List<String> partitionTags2 = new ArrayList<>();
    partitionTags2.add(tag2);
    SearchParam searchParam2 =
        new SearchParam.Builder(randomCollectionName)
            .withDSL(generateSimpleDSL(topK, vectorsToSearch2.toString()))
            .withPartitionTags(partitionTags2)
            .build();
    SearchResponse searchResponse2 = client.search(searchParam2);
    assertTrue(searchResponse2.ok());
    List<List<Long>> resultIdsList2 = searchResponse2.getResultIdsList();
    assertEquals(searchSize, resultIdsList2.size());
    assertTrue(entityIds2.containsAll(resultIdsList2.get(0)));

    assertTrue(Collections.disjoint(resultIdsList1, resultIdsList2));

    HasPartitionResponse testHasPartition = client.hasPartition(randomCollectionName, tag1);
    assertTrue(testHasPartition.hasPartition());

    Response dropPartitionResponse = client.dropPartition(randomCollectionName, tag1);
    assertTrue(dropPartitionResponse.ok());

    testHasPartition = client.hasPartition(randomCollectionName, tag1);
    assertFalse(testHasPartition.hasPartition());

    dropPartitionResponse = client.dropPartition(randomCollectionName, tag2);
    assertTrue(dropPartitionResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void createIndex() {
    insert();
    assertTrue(client.flush(randomCollectionName).ok());

    Index index =
        new Index.Builder(randomCollectionName, "float_vec")
            .withParamsInJson("{\"index_type\": \"IVF_SQ8\", \"metric_type\": \"L2\", "
                + "\"params\": {\"nlist\": 2048}}")
            .build();

    Response createIndexResponse = client.createIndex(index);
    assertTrue(createIndexResponse.ok());

    // also test drop index here
    Response dropIndexResponse = client.dropIndex(index);
    assertTrue(dropIndexResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void createIndexAsync() throws ExecutionException, InterruptedException {
    insert();
    assertTrue(client.flush(randomCollectionName).ok());

    Index index =
        new Index.Builder(randomCollectionName, "float_vec")
            .withParamsInJson("{\"index_type\": \"IVF_SQ8\", \"metric_type\": \"L2\", "
                + "\"params\": {\"nlist\": 2048}}")
            .build();

    ListenableFuture<Response> createIndexResponseFuture = client.createIndexAsync(index);
    Response createIndexResponse = createIndexResponseFuture.get();
    assertTrue(createIndexResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void insert() {
    List<Map<String, Object>> defaultFieldValues = generateDefaultFieldValues(size, dimension);
    List<Long> entityIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .withFields(defaultFieldValues)
            .withEntityIds(entityIds)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    assertEquals(size, insertResponse.getEntityIds().size());
  }

  @org.junit.jupiter.api.Test
  void insertAsync() throws ExecutionException, InterruptedException {
    List<Map<String, Object>> defaultFieldValues = generateDefaultFieldValues(size, dimension);
    List<Long> entityIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .withFields(defaultFieldValues)
            .withEntityIds(entityIds)
            .build();
    ListenableFuture<InsertResponse> insertResponseFuture = client.insertAsync(insertParam);
    InsertResponse insertResponse = insertResponseFuture.get();
    assertTrue(insertResponse.ok());
    assertEquals(size, insertResponse.getEntityIds().size());
  }

  @org.junit.jupiter.api.Test
  void insertBinary() {
    final int binaryDimension = 10000;

    String binaryCollectionName = generator.generate(10);
    List<Map<String, Object>> fieldList = new ArrayList<>();
    Map<String, Object> binaryField = new HashMap<>();
    binaryField.put("field", "binary_vec");
    binaryField.put("type", DataType.VECTOR_BINARY);
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("dim", binaryDimension);
    binaryField.put("params", jsonObject.toString());
    fieldList.add(binaryField);
    CollectionMapping collectionMapping =
        new CollectionMapping.Builder(binaryCollectionName)
            .withFields(fieldList)
            .build();
    assertTrue(client.createCollection(collectionMapping).ok());

    List<Map<String, Object>> insertList = new ArrayList<>();
    Map<String, Object> insertBinaryField = new HashMap<>();
    insertBinaryField.put("field", "binary_vec");
    insertBinaryField.put("type", DataType.VECTOR_BINARY);
    List<ByteBuffer> vectors = generateBinaryVectors(size, binaryDimension);
    insertBinaryField.put("values", vectors);
    insertList.add(insertBinaryField);

    InsertParam insertParam =
        new Builder(binaryCollectionName)
            .withFields(insertList)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    assertEquals(size, insertResponse.getEntityIds().size());

    assertTrue(client.dropCollection(binaryCollectionName).ok());
  }

  @org.junit.jupiter.api.Test
  void search() {
    List<Map<String, Object>> fieldList = new ArrayList<>();
    Map<String, Object> intField = new HashMap<>();
    intField.put("field", "int64");
    intField.put("type", DataType.INT64);

    Map<String, Object> floatField = new HashMap<>();
    floatField.put("field", "float");
    floatField.put("type", DataType.FLOAT);

    Map<String, Object> vecField = new HashMap<>();
    vecField.put("field", "float_vec");
    vecField.put("type", DataType.VECTOR_FLOAT);

    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    vectors = vectors.stream().map(MilvusClientTest::normalizeVector).collect(Collectors.toList());
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    intField.put("values", intValues);
    floatField.put("values", floatValues);
    vecField.put("values", vectors);
    fieldList.add(intField);
    fieldList.add(floatField);
    fieldList.add(vecField);

    List<Long> insertIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .withFields(fieldList)
            .withEntityIds(insertIds)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<Long> entityIds = insertResponse.getEntityIds();
    assertEquals(size, entityIds.size());

    assertTrue(client.flush(randomCollectionName).ok());

    final int searchSize = 5;
    List<List<Float>> vectorsToSearch = vectors.subList(0, searchSize);

    final long topK = 10;
    SearchParam searchParam =
        new SearchParam.Builder(randomCollectionName)
            .withDSL(generateComplexDSL(topK, vectorsToSearch.toString()))
            .withParamsInJson("{\"fields\": [\"int64\", \"float\"]}")
            .build();
    SearchResponse searchResponse = client.search(searchParam);
    assertTrue(searchResponse.ok());
    List<List<Long>> resultIdsList = searchResponse.getResultIdsList();
    assertEquals(searchSize, resultIdsList.size());
    List<List<Float>> resultDistancesList = searchResponse.getResultDistancesList();
    assertEquals(searchSize, resultDistancesList.size());
    List<List<SearchResponse.QueryResult>> queryResultsList = searchResponse.getQueryResultsList();
    assertEquals(searchSize, queryResultsList.size());

    final double epsilon = 0.001;
    for (int i = 0; i < searchSize; i++) {
      SearchResponse.QueryResult firstQueryResult = queryResultsList.get(i).get(0);
      assertEquals(entityIds.get(i), firstQueryResult.getEntityId());
      assertEquals(entityIds.get(i), resultIdsList.get(i).get(0));
      assertTrue(Math.abs(firstQueryResult.getDistance()) < epsilon);
      assertTrue(Math.abs(resultDistancesList.get(i).get(0)) < epsilon);
    }
  }

  @org.junit.jupiter.api.Test
  void searchAsync() throws ExecutionException, InterruptedException {
    List<Map<String, Object>> fieldList = new ArrayList<>();
    Map<String, Object> intField = new HashMap<>();
    intField.put("field", "int64");
    intField.put("type", DataType.INT64);

    Map<String, Object> floatField = new HashMap<>();
    floatField.put("field", "float");
    floatField.put("type", DataType.FLOAT);

    Map<String, Object> vecField = new HashMap<>();
    vecField.put("field", "float_vec");
    vecField.put("type", DataType.VECTOR_FLOAT);

    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    vectors = vectors.stream().map(MilvusClientTest::normalizeVector).collect(Collectors.toList());
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    intField.put("values", intValues);
    floatField.put("values", floatValues);
    vecField.put("values", vectors);
    fieldList.add(intField);
    fieldList.add(floatField);
    fieldList.add(vecField);

    List<Long> insertIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .withFields(fieldList)
            .withEntityIds(insertIds)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<Long> entityIds = insertResponse.getEntityIds();
    assertEquals(size, entityIds.size());

    assertTrue(client.flush(randomCollectionName).ok());

    final int searchSize = 5;
    List<List<Float>> vectorsToSearch = vectors.subList(0, searchSize);

    final long topK = 10;
    SearchParam searchParam =
        new SearchParam.Builder(randomCollectionName)
            .withDSL(generateComplexDSL(topK, vectorsToSearch.toString()))
            .withParamsInJson("{\"fields\": [\"int64\", \"float\"]}")
            .build();
    ListenableFuture<SearchResponse> searchResponseFuture = client.searchAsync(searchParam);
    SearchResponse searchResponse = searchResponseFuture.get();
    assertTrue(searchResponse.ok());
    List<List<Long>> resultIdsList = searchResponse.getResultIdsList();
    assertEquals(searchSize, resultIdsList.size());
    List<List<Float>> resultDistancesList = searchResponse.getResultDistancesList();
    assertEquals(searchSize, resultDistancesList.size());
    List<List<SearchResponse.QueryResult>> queryResultsList = searchResponse.getQueryResultsList();
    assertEquals(searchSize, queryResultsList.size());

    final double epsilon = 0.001;
    for (int i = 0; i < searchSize; i++) {
      SearchResponse.QueryResult firstQueryResult = queryResultsList.get(i).get(0);
      assertEquals(entityIds.get(i), firstQueryResult.getEntityId());
      assertEquals(entityIds.get(i), resultIdsList.get(i).get(0));
      assertTrue(Math.abs(firstQueryResult.getDistance()) < epsilon);
      assertTrue(Math.abs(resultDistancesList.get(i).get(0)) < epsilon);
    }
  }

  @org.junit.jupiter.api.Test
  void searchBinary() {
    final int binaryDimension = 10000;

    String binaryCollectionName = generator.generate(10);
    // field list for collection
    List<Map<String, Object>> fieldList = new ArrayList<>();
    Map<String, Object> intField = new HashMap<>();
    intField.put("field", "int64");
    intField.put("type", DataType.INT64);

    Map<String, Object> floatField = new HashMap<>();
    floatField.put("field", "float");
    floatField.put("type", DataType.FLOAT);

    Map<String, Object> binaryField = new HashMap<>();
    binaryField.put("field", "binary_vec");
    binaryField.put("type", DataType.VECTOR_BINARY);
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("dim", binaryDimension);
    binaryField.put("params", jsonObject.toString());

    fieldList.add(intField);
    fieldList.add(floatField);
    fieldList.add(binaryField);
    CollectionMapping collectionMapping =
        new CollectionMapping.Builder(binaryCollectionName)
            .withFields(fieldList)
            .build();
    assertTrue(client.createCollection(collectionMapping).ok());

    // field list for insert
    List<Map<String, Object>> insertList = new ArrayList<>();
    Map<String, Object> intInsertField = new HashMap<>();
    intInsertField.put("field", "int64");
    intInsertField.put("type", DataType.INT64);

    Map<String, Object> floatInsertField = new HashMap<>();
    floatInsertField.put("field", "float");
    floatInsertField.put("type", DataType.FLOAT);

    Map<String, Object> vecInsertField = new HashMap<>();
    vecInsertField.put("field", "binary_vec");
    vecInsertField.put("type", DataType.VECTOR_BINARY);

    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<ByteBuffer> vectors = generateBinaryVectors(size, binaryDimension);
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    intInsertField.put("values", intValues);
    floatInsertField.put("values", floatValues);
    vecInsertField.put("values", vectors);
    insertList.add(intInsertField);
    insertList.add(floatInsertField);
    insertList.add(vecInsertField);

    InsertParam insertParam =
        new Builder(binaryCollectionName)
            .withFields(insertList)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<Long> entityIds = insertResponse.getEntityIds();
    assertEquals(size, entityIds.size());

    assertTrue(client.flush(binaryCollectionName).ok());

    final int searchSize = 5;
    List<ByteBuffer> vectorsToSearch = vectors.subList(0, searchSize);
    Map<String, List<ByteBuffer>> binaryEntities = new HashMap<>();
    binaryEntities.put("myPlaceholder", vectorsToSearch);

    final long topK = 10;
    SearchParam searchParam =
        new SearchParam.Builder(binaryCollectionName)
            .withDSL(generateComplexDSLBinary(topK, "myPlaceholder"))
            .withBinaryEntities(binaryEntities)
            .withParamsInJson("{\"fields\": [\"int64\", \"float\"]}")
            .build();
    SearchResponse searchResponse = client.search(searchParam);
    assertTrue(searchResponse.ok());
    List<List<Long>> resultIdsList = searchResponse.getResultIdsList();
    assertEquals(searchSize, resultIdsList.size());
    List<List<Float>> resultDistancesList = searchResponse.getResultDistancesList();
    assertEquals(searchSize, resultDistancesList.size());
    List<List<SearchResponse.QueryResult>> queryResultsList = searchResponse.getQueryResultsList();
    assertEquals(searchSize, queryResultsList.size());

    for (int i = 0; i < searchSize; i++) {
      SearchResponse.QueryResult firstQueryResult = queryResultsList.get(i).get(0);
      assertEquals(entityIds.get(i), firstQueryResult.getEntityId());
      assertEquals(entityIds.get(i), resultIdsList.get(i).get(0));
    }

    assertTrue(client.dropCollection(binaryCollectionName).ok());
  }

  @org.junit.jupiter.api.Test
  void getCollectionInfo() {
    GetCollectionInfoResponse getCollectionInfoResponse =
        client.getCollectionInfo(randomCollectionName);
    assertTrue(getCollectionInfoResponse.ok());
    assertTrue(getCollectionInfoResponse.getCollectionMapping().isPresent());
    assertEquals(
        getCollectionInfoResponse.getCollectionMapping().get().getCollectionName(),
        randomCollectionName);

    String nonExistingCollectionName = generator.generate(10);
    getCollectionInfoResponse = client.getCollectionInfo(nonExistingCollectionName);
    assertFalse(getCollectionInfoResponse.ok());
    assertFalse(getCollectionInfoResponse.getCollectionMapping().isPresent());
  }

  @org.junit.jupiter.api.Test
  void listCollections() {
    ListCollectionsResponse listCollectionsResponse = client.listCollections();
    assertTrue(listCollectionsResponse.ok());
    assertTrue(listCollectionsResponse.getCollectionNames().contains(randomCollectionName));
  }

  @org.junit.jupiter.api.Test
  void serverStatus() {
    Response serverStatusResponse = client.getServerStatus();
    assertTrue(serverStatusResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void serverVersion() {
    Response serverVersionResponse = client.getServerVersion();
    assertTrue(serverVersionResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void countEntities() {
    insert();
    assertTrue(client.flush(randomCollectionName).ok());

    CountEntitiesResponse countEntitiesResponse = client.countEntities(randomCollectionName);
    assertTrue(countEntitiesResponse.ok());
    assertEquals(size, countEntitiesResponse.getCollectionEntityCount());
  }

  @org.junit.jupiter.api.Test
  void loadCollection() {
    insert();
    assertTrue(client.flush(randomCollectionName).ok());

    Response loadCollectionResponse = client.loadCollection(randomCollectionName);
    assertTrue(loadCollectionResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void getCollectionStats() {
    insert();

    assertTrue(client.flush(randomCollectionName).ok());

    Response getCollectionStatsResponse = client.getCollectionStats(randomCollectionName);
    assertTrue(getCollectionStatsResponse.ok());

    String jsonString = getCollectionStatsResponse.getMessage();
    JSONObject jsonInfo = new JSONObject(jsonString);
    assertEquals(jsonInfo.getInt("row_count"), size);

    JSONArray partitions = jsonInfo.getJSONArray("partitions");
    JSONObject partitionInfo = partitions.getJSONObject(0);
    assertEquals(partitionInfo.getString("tag"), "_default");
    assertEquals(partitionInfo.getInt("row_count"), size);

    JSONArray segments = partitionInfo.getJSONArray("segments");
    JSONObject segmentInfo = segments.getJSONObject(0);
    assertEquals(segmentInfo.getInt("row_count"), size);
  }

  @org.junit.jupiter.api.Test
  void getEntityByID() {
    List<Map<String, Object>> fieldList = new ArrayList<>();
    Map<String, Object> intField = new HashMap<>();
    intField.put("field", "int64");
    intField.put("type", DataType.INT64);

    Map<String, Object> floatField = new HashMap<>();
    floatField.put("field", "float");
    floatField.put("type", DataType.FLOAT);

    Map<String, Object> vecField = new HashMap<>();
    vecField.put("field", "float_vec");
    vecField.put("type", DataType.VECTOR_FLOAT);

    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    intField.put("values", intValues);
    floatField.put("values", floatValues);
    vecField.put("values", vectors);

    fieldList.add(intField);
    fieldList.add(floatField);
    fieldList.add(vecField);

    List<Long> insertIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .withFields(fieldList)
            .withEntityIds(insertIds)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<Long> entityIds = insertResponse.getEntityIds();
    assertEquals(size, entityIds.size());

    assertTrue(client.flush(randomCollectionName).ok());

    GetEntityByIDResponse getEntityByIDResponse =
        client.getEntityByID(randomCollectionName, entityIds.subList(0, 100));
    assertTrue(getEntityByIDResponse.ok());
    assertEquals(getEntityByIDResponse.getValidIds(), entityIds.subList(0, 100));
    int vecIndex = 0;
    List<Map<String, Object>> fieldsMap = getEntityByIDResponse.getFieldsMap();
    assertTrue(fieldsMap.get(vecIndex).get("float_vec") instanceof List);
    List<Float> first = (List<Float>) (fieldsMap.get(vecIndex).get("float_vec"));

    assertArrayEquals(first.toArray(), vectors.get(0).toArray());
  }

  @org.junit.jupiter.api.Test
  void getEntityIds() {
    insert();

    assertTrue(client.flush(randomCollectionName).ok());

    Response getCollectionStatsResponse = client.getCollectionStats(randomCollectionName);
    assertTrue(getCollectionStatsResponse.ok());

    JSONObject jsonInfo = new JSONObject(getCollectionStatsResponse.getMessage());
    JSONObject segmentInfo =
        jsonInfo
            .getJSONArray("partitions")
            .getJSONObject(0)
            .getJSONArray("segments")
            .getJSONObject(0);

    ListIDInSegmentResponse listIDInSegmentResponse =
        client.listIDInSegment(randomCollectionName, segmentInfo.getLong("id"));
    assertTrue(listIDInSegmentResponse.ok());
    assertFalse(listIDInSegmentResponse.getIds().isEmpty());
  }

  @org.junit.jupiter.api.Test
  void deleteEntityByID() {
    List<Map<String, Object>> defaultFieldValues = generateDefaultFieldValues(size, dimension);
    List<Long> insertIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .withFields(defaultFieldValues)
            .withEntityIds(insertIds)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    assertEquals(size, insertResponse.getEntityIds().size());

    assertTrue(client.flush(randomCollectionName).ok());

    assertTrue(client.deleteEntityByID(randomCollectionName,
        insertResponse.getEntityIds().subList(0, 100)).ok());
    assertTrue(client.flush(randomCollectionName).ok());
    assertEquals(client.countEntities(randomCollectionName).getCollectionEntityCount(), size - 100);
  }

  @org.junit.jupiter.api.Test
  void flush() {
    assertTrue(client.flush(randomCollectionName).ok());
  }

  @org.junit.jupiter.api.Test
  void flushAsync() throws ExecutionException, InterruptedException {
    assertTrue(client.flushAsync(randomCollectionName).get().ok());
  }

  @org.junit.jupiter.api.Test
  void compact() {
    List<Map<String, Object>> defaultFieldValues = generateDefaultFieldValues(size, dimension);
    List<Long> insertIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .withFields(defaultFieldValues)
            .withEntityIds(insertIds)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    assertEquals(size, insertResponse.getEntityIds().size());

    assertTrue(client.flush(randomCollectionName).ok());

    Response getCollectionStatsResponse = client.getCollectionStats(randomCollectionName);
    assertTrue(getCollectionStatsResponse.ok());

    JSONObject jsonInfo = new JSONObject(getCollectionStatsResponse.getMessage());
    JSONObject segmentInfo =
        jsonInfo
            .getJSONArray("partitions")
            .getJSONObject(0)
            .getJSONArray("segments")
            .getJSONObject(0);

    long previousSegmentSize = segmentInfo.getLong("data_size");

    assertTrue(
        client.deleteEntityByID(randomCollectionName,
            insertResponse.getEntityIds().subList(0, size / 2)).ok());
    assertTrue(client.flush(randomCollectionName).ok());
    assertTrue(client.compact(
        new CompactParam.Builder(randomCollectionName).withThreshold(0.2).build()).ok());

    getCollectionStatsResponse = client.getCollectionStats(randomCollectionName);
    assertTrue(getCollectionStatsResponse.ok());
    jsonInfo = new JSONObject(getCollectionStatsResponse.getMessage());
    segmentInfo =
        jsonInfo
            .getJSONArray("partitions")
            .getJSONObject(0)
            .getJSONArray("segments")
            .getJSONObject(0);

    long currentSegmentSize = segmentInfo.getLong("data_size");
    assertTrue(currentSegmentSize < previousSegmentSize);
  }

  @org.junit.jupiter.api.Test
  void compactAsync() throws ExecutionException, InterruptedException {
    List<Map<String, Object>> defaultFieldValues = generateDefaultFieldValues(size, dimension);
    List<Long> insertIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .withFields(defaultFieldValues)
            .withEntityIds(insertIds)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    assertEquals(size, insertResponse.getEntityIds().size());

    assertTrue(client.flush(randomCollectionName).ok());

    Response getCollectionStatsResponse = client.getCollectionStats(randomCollectionName);
    assertTrue(getCollectionStatsResponse.ok());

    JSONObject jsonInfo = new JSONObject(getCollectionStatsResponse.getMessage());
    JSONObject segmentInfo =
        jsonInfo
            .getJSONArray("partitions")
            .getJSONObject(0)
            .getJSONArray("segments")
            .getJSONObject(0);

    long previousSegmentSize = segmentInfo.getLong("data_size");

    assertTrue(
        client.deleteEntityByID(randomCollectionName,
            insertResponse.getEntityIds().subList(0, size / 2)).ok());
    assertTrue(client.flush(randomCollectionName).ok());
    assertTrue(client.compactAsync(
        new CompactParam.Builder(randomCollectionName).withThreshold(0.8).build()).get().ok());

    getCollectionStatsResponse = client.getCollectionStats(randomCollectionName);
    assertTrue(getCollectionStatsResponse.ok());
    jsonInfo = new JSONObject(getCollectionStatsResponse.getMessage());
    segmentInfo =
        jsonInfo
            .getJSONArray("partitions")
            .getJSONObject(0)
            .getJSONArray("segments")
            .getJSONObject(0);

    long currentSegmentSize = segmentInfo.getLong("data_size");
    assertFalse(currentSegmentSize < previousSegmentSize); // threshold 0.8 > 0.5, no compact
  }
}
