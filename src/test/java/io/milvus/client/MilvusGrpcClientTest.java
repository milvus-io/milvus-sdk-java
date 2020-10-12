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

import com.google.common.collect.ImmutableSet;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.milvus.client.exception.ClientSideMilvusException;
import io.milvus.client.exception.ServerSideMilvusException;
import io.milvus.client.exception.UnsupportedServerVersion;
import io.milvus.grpc.ErrorCode;
import org.apache.commons.lang3.RandomStringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@EnabledIfSystemProperty(named = "with-containers", matches = "true")
class ContainerMilvusClientTest extends MilvusClientTest {
  @Container
  private GenericContainer milvusContainer =
      new GenericContainer(System.getProperty("docker_image_name", "milvusdb/milvus:0.11.0-cpu"))
          .withExposedPorts(19530);

  @Container
  private static GenericContainer milvusContainer2 =
      new GenericContainer(System.getProperty("docker_image_name", "milvusdb/milvus:0.11.0-cpu"))
          .withExposedPorts(19530);

  @Override
  protected ConnectParam.Builder connectParamBuilder() {
    return connectParamBuilder(milvusContainer);
  }

  @org.junit.jupiter.api.Test
  void loadBalancing() {
    NameResolverProvider testNameResolverProvider = new StaticNameResolverProvider(
        new InetSocketAddress(milvusContainer.getHost(), milvusContainer.getFirstMappedPort()),
        new InetSocketAddress(milvusContainer2.getHost(), milvusContainer2.getFirstMappedPort()));

    NameResolverRegistry.getDefaultRegistry().register(testNameResolverProvider);

    ConnectParam connectParam = connectParamBuilder()
        .withTarget(testNameResolverProvider.getDefaultScheme() + ":///test")
        .build();

    MilvusClient loadBalancingClient = new MilvusGrpcClient(connectParam);
    assertEquals(50, IntStream.range(0, 100)
            .filter(i -> loadBalancingClient.hasCollection(randomCollectionName))
            .count());
  }
}

@Testcontainers
@DisabledIfSystemProperty(named = "with-containers", matches = "true")
class MilvusClientTest {

  private MilvusClient client;

  protected String randomCollectionName;
  private int size;
  private int dimension;

  protected ConnectParam.Builder connectParamBuilder() {
    return connectParamBuilder("localhost", 19530);
  }

  protected ConnectParam.Builder connectParamBuilder(GenericContainer milvusContainer) {
    return connectParamBuilder(milvusContainer.getHost(), milvusContainer.getFirstMappedPort());
  }

  protected ConnectParam.Builder connectParamBuilder(String host, int port) {
    return new ConnectParam.Builder().withHost(host).withPort(port);
  }

  protected void assertErrorCode(ErrorCode errorCode, Runnable runnable) {
    assertEquals(errorCode, assertThrows(ServerSideMilvusException.class, runnable::run).getErrorCode());
  }

  protected void assertGrpcStatusCode(Status.Code statusCode, Runnable runnable) {
    ClientSideMilvusException error = assertThrows(ClientSideMilvusException.class, runnable::run);
    assertTrue(error.getCause() instanceof StatusRuntimeException);
    assertEquals(statusCode, ((StatusRuntimeException) error.getCause()).getStatus().getCode());
  }

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
            + "    \"must\": [{"
            + "        \"vector\": {"
            + "            \"float_vec\": {"
            + "                \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"float\", \"query\": %s, \"params\": {\"nprobe\": 20}"
            + "}}}]}]}}",
        topK, query);
  }

  // Helper function that generate a complex DSL statement with scalar field filtering
  static String generateComplexDSLBinary(Long topK, String query) {
    return String.format(
        "{\"bool\": {"
            + "\"must\": [{"
            + "    \"vector\": {"
            + "        \"binary_vec\": {"
            + "            \"topk\": %d, \"metric_type\": \"JACCARD\", \"type\": \"binary\", \"query\": %s, \"params\": {\"nprobe\": 20}"
            + "    }}}]}}",
        topK, query);
  }

  @org.junit.jupiter.api.BeforeEach
  void setUp() throws Exception {

    ConnectParam connectParam = connectParamBuilder().build();
    client = new MilvusGrpcClient(connectParam);

    randomCollectionName = RandomStringUtils.randomAlphabetic(10);
    size = 100000;
    dimension = 128;

    CollectionMapping collectionMapping = CollectionMapping
        .create(randomCollectionName)
        .addField("int64", DataType.INT64)
        .addField("float", DataType.FLOAT)
        .addVectorField("float_vec", DataType.VECTOR_FLOAT, dimension)
        .setParamsInJson(new JsonBuilder()
            .param("segment_row_limit", 50000)
            .param("auto_id", false)
            .build());

    client.createCollection(collectionMapping);
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() {
    client.dropCollection(randomCollectionName);
    client.close();
  }

  @org.junit.jupiter.api.Test
  void idleTest() throws InterruptedException {
    ConnectParam connectParam = connectParamBuilder()
        .withIdleTimeout(1, TimeUnit.SECONDS)
        .build();
    MilvusClient client = new MilvusGrpcClient(connectParam);
    TimeUnit.SECONDS.sleep(2);
    // A new RPC would take the channel out of idle mode
    client.listCollections();
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
    ConnectParam connectParam = connectParamBuilder("250.250.250.250", 19530).build();
    assertThrows(ClientSideMilvusException.class, () -> new MilvusGrpcClient(connectParam));
  }

  @org.junit.jupiter.api.Test
  void unsupportedServerVersion() {
    GenericContainer unsupportedMilvusContainer =
        new GenericContainer("milvusdb/milvus:0.9.1-cpu-d052920-e04ed5")
            .withExposedPorts(19530);
    try {
      unsupportedMilvusContainer.start();
      ConnectParam connectParam = connectParamBuilder(unsupportedMilvusContainer).build();
      assertThrows(UnsupportedServerVersion.class, () -> new MilvusGrpcClient(connectParam));
    } finally {
      unsupportedMilvusContainer.stop();
    }
  }

  @org.junit.jupiter.api.Test
  void grpcTimeout() {
    insert();
    MilvusClient timeoutClient = client.withTimeout(1, TimeUnit.MILLISECONDS);
    Index index = Index.create(randomCollectionName, "float_vec")
        .setIndexType(IndexType.IVF_FLAT)
        .setMetricType(MetricType.L2)
        .setParamsInJson(new JsonBuilder().param("nlist", 2048).build());
    assertGrpcStatusCode(Status.Code.DEADLINE_EXCEEDED, () -> timeoutClient.createIndex(index));
  }

  @org.junit.jupiter.api.Test
  void createInvalidCollection() {
    // invalid collection name
    CollectionMapping invalidCollectionName = CollectionMapping
        .create("╯°□°）╯")
        .addVectorField("float_vec", DataType.VECTOR_FLOAT, dimension);

    assertErrorCode(ErrorCode.ILLEGAL_COLLECTION_NAME, () -> client.createCollection(invalidCollectionName));

    // invalid field
    CollectionMapping withoutField = CollectionMapping.create("validCollectionName");
    assertThrows(ClientSideMilvusException.class, () -> client.createCollection(withoutField));

    // invalid segment_row_limit
    CollectionMapping invalidSegmentRowCount = CollectionMapping
        .create("validCollectionName")
        .addField("int64", DataType.INT64)
        .addField("float", DataType.FLOAT)
        .addVectorField("float_vec", DataType.VECTOR_FLOAT, dimension)
        .setParamsInJson(new JsonBuilder().param("segment_row_limit", -1000).build());

    assertErrorCode(ErrorCode.ILLEGAL_ARGUMENT, () -> client.createCollection(invalidSegmentRowCount));
  }

  @org.junit.jupiter.api.Test
  void hasCollection() {
    assertTrue(client.hasCollection(randomCollectionName));
  }

  @org.junit.jupiter.api.Test
  void dropCollection() {
    String nonExistingCollectionName = RandomStringUtils.randomAlphabetic(10);
    assertErrorCode(ErrorCode.COLLECTION_NOT_EXISTS, () -> client.dropCollection(nonExistingCollectionName));
  }

  @org.junit.jupiter.api.Test
  void partitionTest() {
    final String tag1 = "tag1";
    client.createPartition(randomCollectionName, tag1);

    final String tag2 = "tag2";
    client.createPartition(randomCollectionName, tag2);

    List<String> partitionList = client.listPartitions(randomCollectionName);
    assertEquals(3, partitionList.size()); // two tags plus _default

    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }

    List<Long> entityIds1 = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam = InsertParam
        .create(randomCollectionName)
        .addField("int64", DataType.INT64, intValues)
        .addField("float", DataType.FLOAT, floatValues)
        .addVectorField("float_vec", DataType.VECTOR_FLOAT, vectors)
        .setEntityIds(entityIds1)
        .setPartitionTag(tag1);
    client.insert(insertParam);

    List<Long> entityIds2 = LongStream.range(size, size * 2).boxed().collect(Collectors.toList());
    insertParam = InsertParam
        .create(randomCollectionName)
        .addField("int64", DataType.INT64, intValues)
        .addField("float", DataType.FLOAT, floatValues)
        .addVectorField("float_vec", DataType.VECTOR_FLOAT, vectors)
        .setEntityIds(entityIds2)
        .setPartitionTag(tag2);
    client.insert(insertParam);

    client.flush(randomCollectionName);

    assertEquals(size * 2, client.countEntities(randomCollectionName));

    final int searchSize = 1;
    final long topK = 10;

    List<List<Float>> vectorsToSearch1 = vectors.subList(0, searchSize);
    List<String> partitionTags1 = new ArrayList<>();
    partitionTags1.add(tag1);
    SearchParam searchParam1 = SearchParam
        .create(randomCollectionName)
        .setDsl(generateSimpleDSL(topK, vectorsToSearch1.toString()))
        .setPartitionTags(partitionTags1);
    SearchResult searchResult1 = client.search(searchParam1);
    List<List<Long>> resultIdsList1 = searchResult1.getResultIdsList();
    assertEquals(searchSize, resultIdsList1.size());
    assertTrue(entityIds1.containsAll(resultIdsList1.get(0)));

    List<List<Float>> vectorsToSearch2 = vectors.subList(0, searchSize);
    List<String> partitionTags2 = new ArrayList<>();
    partitionTags2.add(tag2);
    SearchParam searchParam2 = SearchParam.create(randomCollectionName)
        .setDsl(generateSimpleDSL(topK, vectorsToSearch2.toString()))
        .setPartitionTags(partitionTags2);
    SearchResult searchResult2 = client.search(searchParam2);
    List<List<Long>> resultIdsList2 = searchResult2.getResultIdsList();
    assertEquals(searchSize, resultIdsList2.size());
    assertTrue(entityIds2.containsAll(resultIdsList2.get(0)));

    assertTrue(Collections.disjoint(resultIdsList1, resultIdsList2));

    assertTrue(client.hasPartition(randomCollectionName, tag1));

    client.dropPartition(randomCollectionName, tag1);
    assertFalse(client.hasPartition(randomCollectionName, tag1));

    client.dropPartition(randomCollectionName, tag2);
    assertFalse(client.hasPartition(randomCollectionName, tag2));
  }

  @org.junit.jupiter.api.Test
  void createIndex() {
    insert();
    client.flush(randomCollectionName);

    Index index = Index.create(randomCollectionName, "float_vec")
        .setIndexType(IndexType.IVF_SQ8)
        .setMetricType(MetricType.L2)
        .setParamsInJson(new JsonBuilder().param("nlist", 2048).build());

    client.createIndex(index);

    // also test drop index here
    client.dropIndex(randomCollectionName, "float_vec");
  }

  @org.junit.jupiter.api.Test
  void insert() {
    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }

    List<Long> entityIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam = InsertParam
        .create(randomCollectionName)
        .addField("int64", DataType.INT64, intValues)
        .addField("float", DataType.FLOAT, floatValues)
        .addVectorField("float_vec", DataType.VECTOR_FLOAT, vectors)
        .setEntityIds(entityIds);

    assertEquals(entityIds, client.insert(insertParam));
  }

  @org.junit.jupiter.api.Test
  void insertBinary() {
    final int binaryDimension = 10000;

    String binaryCollectionName = RandomStringUtils.randomAlphabetic(10);

    CollectionMapping collectionMapping = CollectionMapping
        .create(binaryCollectionName)
        .addVectorField("binary_vec", DataType.VECTOR_BINARY, binaryDimension);

    client.createCollection(collectionMapping);

    List<ByteBuffer> vectors = generateBinaryVectors(size, binaryDimension);
    InsertParam insertParam = InsertParam
        .create(binaryCollectionName)
        .addVectorField("binary_vec", DataType.VECTOR_BINARY, vectors);
    assertEquals(size, client.insert(insertParam).size());

    Index index = Index.create(binaryCollectionName, "binary_vec")
        .setIndexType(IndexType.BIN_IVF_FLAT)
        .setMetricType(MetricType.JACCARD)
        .setParamsInJson(new JsonBuilder().param("nlist", 100).build());

    client.createIndex(index);

    // also test drop index here
    client.dropIndex(binaryCollectionName, "binary_vec");

    client.dropCollection(binaryCollectionName);
  }

  @org.junit.jupiter.api.Test
  void search() {
    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    vectors = vectors.stream().map(MilvusClientTest::normalizeVector).collect(Collectors.toList());

    List<Long> insertIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam = InsertParam
        .create(randomCollectionName)
        .addField("int64", DataType.INT64, intValues)
        .addField("float", DataType.FLOAT, floatValues)
        .addVectorField("float_vec", DataType.VECTOR_FLOAT, vectors)
        .setEntityIds(insertIds);
    List<Long> entityIds = client.insert(insertParam);
    assertEquals(size, entityIds.size());

    client.flush(randomCollectionName);

    final int searchSize = 5;
    List<List<Float>> vectorsToSearch = vectors.subList(0, searchSize);

    final long topK = 10;
    SearchParam searchParam = SearchParam
        .create(randomCollectionName)
        .setDsl(generateComplexDSL(topK, vectorsToSearch.toString()))
        .setParamsInJson(new JsonBuilder().param("fields",
            new ArrayList<>(Arrays.asList("int64", "float_vec"))).build());
    SearchResult searchResult = client.search(searchParam);
    List<List<Long>> resultIdsList = searchResult.getResultIdsList();
    assertEquals(searchSize, resultIdsList.size());
    List<List<Float>> resultDistancesList = searchResult.getResultDistancesList();
    assertEquals(searchSize, resultDistancesList.size());
    List<List<SearchResult.QueryResult>> queryResultsList = searchResult.getQueryResultsList();
    assertEquals(searchSize, queryResultsList.size());

    final double epsilon = 0.001;
    for (int i = 0; i < searchSize; i++) {
      SearchResult.QueryResult firstQueryResult = queryResultsList.get(i).get(0);
      assertEquals(entityIds.get(i), firstQueryResult.getEntityId());
      assertEquals(entityIds.get(i), resultIdsList.get(i).get(0));
      assertTrue(Math.abs(firstQueryResult.getDistance()) < epsilon);
      assertTrue(Math.abs(resultDistancesList.get(i).get(0)) < epsilon);
    }
  }

  @org.junit.jupiter.api.Test
  void searchBinary() {
    final int binaryDimension = 64;

    String binaryCollectionName = RandomStringUtils.randomAlphabetic(10);
    CollectionMapping collectionMapping = CollectionMapping
        .create(binaryCollectionName)
        .addField("int64", DataType.INT64)
        .addField("float", DataType.FLOAT)
        .addVectorField("binary_vec", DataType.VECTOR_BINARY, binaryDimension);

    client.createCollection(collectionMapping);

    // field list for insert
    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    List<ByteBuffer> vectors = generateBinaryVectors(size, binaryDimension);

    InsertParam insertParam = InsertParam
        .create(binaryCollectionName)
        .addField("int64", DataType.INT64, intValues)
        .addField("float", DataType.FLOAT, floatValues)
        .addVectorField("binary_vec", DataType.VECTOR_BINARY, vectors);
    List<Long> entityIds = client.insert(insertParam);
    assertEquals(size, entityIds.size());

    client.flush(binaryCollectionName);

    final int searchSize = 5;
    List<String> vectorsToSearch = vectors.subList(0, searchSize)
        .stream().map(byteBuffer -> Arrays.toString(byteBuffer.array()))
        .collect(Collectors.toList());

    final long topK = 10;
    SearchParam searchParam = SearchParam
        .create(binaryCollectionName)
        .setDsl(generateComplexDSLBinary(topK, vectorsToSearch.toString()));
    SearchResult searchResult = client.search(searchParam);
    List<List<Long>> resultIdsList = searchResult.getResultIdsList();
    assertEquals(searchSize, resultIdsList.size());
    List<List<Float>> resultDistancesList = searchResult.getResultDistancesList();
    assertEquals(searchSize, resultDistancesList.size());
    List<List<SearchResult.QueryResult>> queryResultsList = searchResult.getQueryResultsList();
    assertEquals(searchSize, queryResultsList.size());

    for (int i = 0; i < searchSize; i++) {
      SearchResult.QueryResult firstQueryResult = queryResultsList.get(i).get(0);
      assertEquals(entityIds.get(i), firstQueryResult.getEntityId());
      assertEquals(entityIds.get(i), resultIdsList.get(i).get(0));
    }

    client.dropCollection(binaryCollectionName);
  }

  @org.junit.jupiter.api.Test
  void getCollectionInfo() {
    CollectionMapping collectionMapping = client.getCollectionInfo(randomCollectionName);
    assertEquals(randomCollectionName, collectionMapping.getCollectionName());

    for (Map<String, Object> field : collectionMapping.getFields()) {
      if (field.get("field").equals("float_vec")) {
        JSONObject params = new JSONObject(field.get("params").toString());
        assertTrue(params.has("dim"));
      }
    }

    String nonExistingCollectionName = RandomStringUtils.randomAlphabetic(10);
    assertErrorCode(ErrorCode.COLLECTION_NOT_EXISTS, () -> client.getCollectionInfo(nonExistingCollectionName));
  }

  @org.junit.jupiter.api.Test
  void listCollections() {
    List<String> collectionList = client.listCollections();
    assertTrue(collectionList.contains(randomCollectionName));
  }

  @org.junit.jupiter.api.Test
  void serverStatus() {
    JSONObject serverStatus = new JSONObject(client.getServerStatus());
    assertEquals(ImmutableSet.of("indexing", "require_restart", "server_time", "uptime"), serverStatus.keySet());
  }

  @org.junit.jupiter.api.Test
  void serverVersion() {
    assertEquals("0.11.0", client.getServerVersion());
  }

  @org.junit.jupiter.api.Test
  void countEntities() {
    insert();
    client.flush(randomCollectionName);
    assertEquals(size, client.countEntities(randomCollectionName));
  }

  @org.junit.jupiter.api.Test
  void loadCollection() {
    insert();
    client.flush(randomCollectionName);
    client.loadCollection(randomCollectionName);
  }

  @org.junit.jupiter.api.Test
  void getCollectionStats() {
    insert();

    client.flush(randomCollectionName);

    String collectionStats = client.getCollectionStats(randomCollectionName);
    JSONObject jsonInfo = new JSONObject(collectionStats);
    assertEquals(jsonInfo.getInt("row_count"), size);

    JSONArray partitions = jsonInfo.getJSONArray("partitions");
    JSONObject partitionInfo = partitions.getJSONObject(0);
    assertEquals(partitionInfo.getString("tag"), "_default");
    assertEquals(partitionInfo.getInt("row_count"), size);
  }

  @org.junit.jupiter.api.Test
  @SuppressWarnings("unchecked")
  void getEntityByID() {
    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    vectors = vectors.stream().map(MilvusClientTest::normalizeVector).collect(Collectors.toList());

    List<Long> insertIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam = InsertParam
        .create(randomCollectionName)
        .addField("int64", DataType.INT64, intValues)
        .addField("float", DataType.FLOAT, floatValues)
        .addVectorField("float_vec", DataType.VECTOR_FLOAT, vectors)
        .setEntityIds(insertIds);
    List<Long> entityIds = client.insert(insertParam);
    assertEquals(size, entityIds.size());

    client.flush(randomCollectionName);

    List<Map<String, Object>> fieldsMap =
        client.getEntityByID(randomCollectionName, entityIds.subList(0, 100));
    int vecIndex = 0;
    assertTrue(fieldsMap.get(vecIndex).get("float_vec") instanceof List);
    List<Float> first = (List<Float>) (fieldsMap.get(vecIndex).get("float_vec"));

    assertArrayEquals(first.toArray(), vectors.get(0).toArray());
  }

  @org.junit.jupiter.api.Test
  void getEntityByIDBinary() {
    final int binaryDimension = 64;

    String binaryCollectionName = RandomStringUtils.randomAlphabetic(10);
    CollectionMapping collectionMapping = CollectionMapping
        .create(binaryCollectionName)
        .addVectorField("binary_vec", DataType.VECTOR_BINARY, binaryDimension)
        .setParamsInJson(new JsonBuilder().param("auto_id", false).build());

    client.createCollection(collectionMapping);

    List<ByteBuffer> vectors = generateBinaryVectors(size, binaryDimension);
    List<Long> entityIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam = InsertParam
        .create(binaryCollectionName)
        .addVectorField("binary_vec", DataType.VECTOR_BINARY, vectors)
        .setEntityIds(entityIds);
    assertEquals(size, client.insert(insertParam).size());

    client.flush(binaryCollectionName);

    List<Map<String, Object>> fieldsMap =
        client.getEntityByID(binaryCollectionName, entityIds.subList(0, 100));
    assertEquals(100, fieldsMap.size());
    assertTrue(fieldsMap.get(0).get("binary_vec") instanceof ByteBuffer);
    ByteBuffer first = (ByteBuffer) (fieldsMap.get(0).get("binary_vec"));
    assertEquals(vectors.get(0), first);
  }

  @org.junit.jupiter.api.Test
  void getEntityIds() {
    insert();

    client.flush(randomCollectionName);

    String collectionStats = client.getCollectionStats(randomCollectionName);
    JSONObject jsonInfo = new JSONObject(collectionStats);
    JSONObject segmentInfo = jsonInfo
        .getJSONArray("partitions")
        .getJSONObject(0)
        .getJSONArray("segments")
        .getJSONObject(0);

    List<Long> entityIds = client.listIDInSegment(randomCollectionName, segmentInfo.getLong("id"));
    assertFalse(entityIds.isEmpty());
  }

  @org.junit.jupiter.api.Test
  void deleteEntityByID() {
    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    vectors = vectors.stream().map(MilvusClientTest::normalizeVector).collect(Collectors.toList());

    List<Long> insertIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam = InsertParam
        .create(randomCollectionName)
        .addField("int64", DataType.INT64, intValues)
        .addField("float", DataType.FLOAT, floatValues)
        .addVectorField("float_vec", DataType.VECTOR_FLOAT, vectors)
        .setEntityIds(insertIds);
    List<Long> entityIds = client.insert(insertParam);
    assertEquals(size, entityIds.size());

    client.flush(randomCollectionName);

    client.deleteEntityByID(randomCollectionName, entityIds.subList(0, 100));
    client.flush(randomCollectionName);
    assertEquals(size - 100, client.countEntities(randomCollectionName));
  }

  @org.junit.jupiter.api.Test
  void flush() {
    client.flush(randomCollectionName);
  }

  @org.junit.jupiter.api.Test
  void flushAsync() {
    client.flushAsync(randomCollectionName);
  }

  @org.junit.jupiter.api.Test
  void compact() {
    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    vectors = vectors.stream().map(MilvusClientTest::normalizeVector).collect(Collectors.toList());

    List<Long> insertIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam = InsertParam
        .create(randomCollectionName)
        .addField("int64", DataType.INT64, intValues)
        .addField("float", DataType.FLOAT, floatValues)
        .addVectorField("float_vec", DataType.VECTOR_FLOAT, vectors)
        .setEntityIds(insertIds);
    List<Long> entityIds = client.insert(insertParam);
    assertEquals(size, entityIds.size());

    client.flush(randomCollectionName);

    String collectionStats = client.getCollectionStats(randomCollectionName);
    JSONObject jsonInfo = new JSONObject(collectionStats);
    long previousSegmentSize = jsonInfo
        .getJSONArray("partitions")
        .getJSONObject(0)
        .getLong("data_size");

    client.deleteEntityByID(randomCollectionName, entityIds.subList(0, size / 2));
    client.flush(randomCollectionName);
    client.compact(CompactParam.create(randomCollectionName).setThreshold(0.2));

    collectionStats = client.getCollectionStats(randomCollectionName);

    jsonInfo = new JSONObject(collectionStats);
    long currentSegmentSize = jsonInfo
        .getJSONArray("partitions")
        .getJSONObject(0)
        .getLong("data_size");
    assertTrue(currentSegmentSize < previousSegmentSize);
  }

  @org.junit.jupiter.api.Test
  void compactAsync() throws ExecutionException, InterruptedException {
    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    vectors = vectors.stream().map(MilvusClientTest::normalizeVector).collect(Collectors.toList());

    List<Long> insertIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam = InsertParam
        .create(randomCollectionName)
        .addField("int64", DataType.INT64, intValues)
        .addField("float", DataType.FLOAT, floatValues)
        .addVectorField("float_vec", DataType.VECTOR_FLOAT, vectors)
        .setEntityIds(insertIds);
    List<Long> entityIds = client.insert(insertParam);
    assertEquals(size, entityIds.size());

    client.flush(randomCollectionName);

    String collectionStats = client.getCollectionStats(randomCollectionName);
    JSONObject jsonInfo = new JSONObject(collectionStats);
    JSONObject segmentInfo =
        jsonInfo
            .getJSONArray("partitions")
            .getJSONObject(0)
            .getJSONArray("segments")
            .getJSONObject(0);

    long previousSegmentSize = segmentInfo.getLong("data_size");

    client.deleteEntityByID(randomCollectionName, entityIds.subList(0, size / 2));
    client.flush(randomCollectionName);
    client.compactAsync(CompactParam.create(randomCollectionName).setThreshold(0.8)).get();

    collectionStats = client.getCollectionStats(randomCollectionName);
    jsonInfo = new JSONObject(collectionStats);
    segmentInfo = jsonInfo
        .getJSONArray("partitions")
        .getJSONObject(0)
        .getJSONArray("segments")
        .getJSONObject(0);

    long currentSegmentSize = segmentInfo.getLong("data_size");
    assertFalse(currentSegmentSize < previousSegmentSize); // threshold 0.8 > 0.5, no compact
  }
}
