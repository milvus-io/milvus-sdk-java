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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.milvus.client.InsertParam.Builder;
import io.milvus.client.exception.ClientSideMilvusException;
import io.milvus.client.exception.InitializationException;
import io.milvus.client.exception.ServerSideMilvusException;
import io.milvus.client.exception.UnsupportedServerVersion;
import io.milvus.grpc.ErrorCode;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.text.RandomStringGenerator;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;
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
            .filter(i -> loadBalancingClient.hasCollection(randomCollectionName).hasCollection())
            .count());
  }
}

@Testcontainers
@DisabledIfSystemProperty(named = "with-containers", matches = "true")
class MilvusClientTest {

  private MilvusClient client;

  private RandomStringGenerator generator;

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
  static List<List<Byte>> generateBinaryVectors(int vectorCount, int dimension) {
    Random random = new Random();
    List<List<Byte>> vectors = new ArrayList<>(vectorCount);
    final int dimensionInByte = dimension / 8;
    for (int i = 0; i < vectorCount; ++i) {
      ByteBuffer byteBuffer = ByteBuffer.allocate(dimensionInByte);
      random.nextBytes(byteBuffer.array());
      byte[] b = new byte[byteBuffer.remaining()];
      byteBuffer.get(b);
      vectors.add(Arrays.asList(ArrayUtils.toObject(b)));
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

    generator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();
    randomCollectionName = generator.generate(10);
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
    ConnectParam connectParam = connectParamBuilder("250.250.250.250", 19530).build();
    assertThrows(InitializationException.class, () -> new MilvusGrpcClient(connectParam));
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

    // invalid segment_row_count
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
    String nonExistingCollectionName = generator.generate(10);
    assertErrorCode(ErrorCode.COLLECTION_NOT_EXISTS, () -> client.dropCollection(nonExistingCollectionName));
  }

  @org.junit.jupiter.api.Test
  @SuppressWarnings("unchecked")
  void partitionTest() {
    final String tag1 = "tag1";
    client.createPartition(randomCollectionName, tag1);

    final String tag2 = "tag2";
    client.createPartition(randomCollectionName, tag2);

    ListPartitionsResponse listPartitionsResponse = client.listPartitions(randomCollectionName);
    assertTrue(listPartitionsResponse.ok());
    assertEquals(3, listPartitionsResponse.getPartitionList().size()); // two tags plus _default

    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    List<Long> entityIds1 = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .field(new FieldBuilder("int64", DataType.INT64)
                .values(intValues)
                .build())
            .field(new FieldBuilder("float", DataType.FLOAT)
                .values(floatValues)
                .build())
            .field(new FieldBuilder("float_vec", DataType.VECTOR_FLOAT)
                .values(vectors)
                .build())
            .withEntityIds(entityIds1)
            .withPartitionTag(tag1)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<Long> entityIds2 = LongStream.range(size, size * 2).boxed().collect(Collectors.toList());
    insertParam =
        new Builder(randomCollectionName)
            .field(new FieldBuilder("int64", DataType.INT64)
                .values(intValues)
                .build())
            .field(new FieldBuilder("float", DataType.FLOAT)
                .values(floatValues)
                .build())
            .field(new FieldBuilder("float_vec", DataType.VECTOR_FLOAT)
                .values(vectors)
                .build())
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

    List<List<Float>> vectorsToSearch1 = vectors.subList(0, searchSize);
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

    List<List<Float>> vectorsToSearch2 = vectors.subList(0, searchSize);
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

    assertTrue(client.hasPartition(randomCollectionName, tag1));

    Response dropPartitionResponse = client.dropPartition(randomCollectionName, tag1);
    assertTrue(dropPartitionResponse.ok());

    assertFalse(client.hasPartition(randomCollectionName, tag1));

    dropPartitionResponse = client.dropPartition(randomCollectionName, tag2);
    assertTrue(dropPartitionResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void createIndex() {
    insert();
    assertTrue(client.flush(randomCollectionName).ok());

    Index index = Index.create(randomCollectionName, "float_vec")
        .setIndexType(IndexType.IVF_SQ8)
        .setMetricType(MetricType.L2)
        .setParamsInJson(new JsonBuilder().param("nlist", 2048).build());

    client.createIndex(index);

    // also test drop index here
    Response dropIndexResponse = client.dropIndex(randomCollectionName, "float_vec");
    assertTrue(dropIndexResponse.ok());
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
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .field(new FieldBuilder("int64", DataType.INT64)
                .values(intValues)
                .build())
            .field(new FieldBuilder("float", DataType.FLOAT)
                .values(floatValues)
                .build())
            .field(new FieldBuilder("float_vec", DataType.VECTOR_FLOAT)
                .values(vectors)
                .build())
            .withEntityIds(entityIds)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    assertEquals(size, insertResponse.getEntityIds().size());
  }

  @org.junit.jupiter.api.Test
  void insertAsync() throws ExecutionException, InterruptedException {
    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    List<Long> entityIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .field(new FieldBuilder("int64", DataType.INT64)
                .values(intValues)
                .build())
            .field(new FieldBuilder("float", DataType.FLOAT)
                .values(floatValues)
                .build())
            .field(new FieldBuilder("float_vec", DataType.VECTOR_FLOAT)
                .values(vectors)
                .build())
            .withEntityIds(entityIds)
            .build();
    ListenableFuture<InsertResponse> insertResponseFuture = client.insertAsync(insertParam);
    Futures.addCallback(
        insertResponseFuture,
        new FutureCallback<InsertResponse>() {
          @Override
          public void onSuccess(@NullableDecl InsertResponse insertResponse) {
            assert insertResponse != null;
            assertTrue(insertResponse.ok());
            assertEquals(size, insertResponse.getEntityIds().size());
          }

          @Override
          public void onFailure(Throwable t) {
            System.out.println(t.getMessage());
          }
        }, MoreExecutors.directExecutor()
    );
  }

  @org.junit.jupiter.api.Test
  void insertBinary() {
    final int binaryDimension = 10000;

    String binaryCollectionName = generator.generate(10);

    CollectionMapping collectionMapping = CollectionMapping
        .create(binaryCollectionName)
        .addVectorField("binary_vec", DataType.VECTOR_BINARY, binaryDimension);

    client.createCollection(collectionMapping);

    List<List<Byte>> vectors = generateBinaryVectors(size, binaryDimension);
    InsertParam insertParam =
        new Builder(binaryCollectionName)
            .field(new FieldBuilder("binary_vec", DataType.VECTOR_BINARY)
                .values(vectors)
                .build())
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    assertEquals(size, insertResponse.getEntityIds().size());

    Index index = Index.create(binaryCollectionName, "binary_vec")
        .setIndexType(IndexType.BIN_IVF_FLAT)
        .setMetricType(MetricType.JACCARD)
        .setParamsInJson(new JsonBuilder().param("nlist", 100).build());

    client.createIndex(index);

    // also test drop index here
    Response dropIndexResponse = client.dropIndex(binaryCollectionName, "binary_vec");
    assertTrue(dropIndexResponse.ok());

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
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .field(new FieldBuilder("int64", DataType.INT64)
                .values(intValues)
                .build())
            .field(new FieldBuilder("float", DataType.FLOAT)
                .values(floatValues)
                .build())
            .field(new FieldBuilder("float_vec", DataType.VECTOR_FLOAT)
                .values(vectors)
                .build())
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
            .withParamsInJson(new JsonBuilder().param("fields",
                new ArrayList<>(Arrays.asList("int64", "float_vec"))).build())
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
    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    vectors = vectors.stream().map(MilvusClientTest::normalizeVector).collect(Collectors.toList());

    List<Long> insertIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .field(new FieldBuilder("int64", DataType.INT64)
                .values(intValues)
                .build())
            .field(new FieldBuilder("float", DataType.FLOAT)
                .values(floatValues)
                .build())
            .field(new FieldBuilder("float_vec", DataType.VECTOR_FLOAT)
                .values(vectors)
                .build())
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
            .withParamsInJson(new JsonBuilder().param("fields",
                new ArrayList<>(Arrays.asList("int64", "float"))).build())
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
    final int binaryDimension = 64;

    String binaryCollectionName = generator.generate(10);
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
    List<List<Byte>> vectors = generateBinaryVectors(size, binaryDimension);

    InsertParam insertParam =
        new Builder(binaryCollectionName)
            .field(new FieldBuilder("int64", DataType.INT64)
                .values(intValues)
                .build())
            .field(new FieldBuilder("float", DataType.FLOAT)
                .values(floatValues)
                .build())
            .field(new FieldBuilder("binary_vec", DataType.VECTOR_BINARY)
                .values(vectors)
                .build())
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<Long> entityIds = insertResponse.getEntityIds();
    assertEquals(size, entityIds.size());

    assertTrue(client.flush(binaryCollectionName).ok());

    final int searchSize = 5;
    List<List<Byte>> vectorsToSearch = vectors.subList(0, searchSize);

    final long topK = 10;
    SearchParam searchParam =
        new SearchParam.Builder(binaryCollectionName)
            .withDSL(generateComplexDSLBinary(topK, vectorsToSearch.toString()))
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

    client.dropCollection(binaryCollectionName);
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

    List<? extends Map<String, Object>> fields = getCollectionInfoResponse.getCollectionMapping()
        .get().getFields();
    for (Map<String, Object> field : fields) {
      if (field.get("field").equals("float_vec")) {
        JSONObject params = new JSONObject(field.get("params").toString());
        assertTrue(params.has("dim"));
      }
    }

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
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .field(new FieldBuilder("int64", DataType.INT64)
                .values(intValues)
                .build())
            .field(new FieldBuilder("float", DataType.FLOAT)
                .values(floatValues)
                .build())
            .field(new FieldBuilder("float_vec", DataType.VECTOR_FLOAT)
                .values(vectors)
                .build())
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
    int vecIndex = 0;
    List<Map<String, Object>> fieldsMap = getEntityByIDResponse.getFieldsMap();
    assertTrue(fieldsMap.get(vecIndex).get("float_vec") instanceof List);
    List<Float> first = (List<Float>) (fieldsMap.get(vecIndex).get("float_vec"));

    assertArrayEquals(first.toArray(), vectors.get(0).toArray());
  }

  @org.junit.jupiter.api.Test
  @SuppressWarnings("unchecked")
  void getEntityByIDBinary() {
    final int binaryDimension = 64;

    String binaryCollectionName = generator.generate(10);
    CollectionMapping collectionMapping = CollectionMapping
        .create(binaryCollectionName)
        .addVectorField("binary_vec", DataType.VECTOR_BINARY, binaryDimension)
        .setParamsInJson(new JsonBuilder().param("auto_id", false).build());

    client.createCollection(collectionMapping);

    List<List<Byte>> vectors = generateBinaryVectors(size, binaryDimension);
    List<Long> entityIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new Builder(binaryCollectionName)
            .field(new FieldBuilder("binary_vec", DataType.VECTOR_BINARY)
                .values(vectors)
                .build())
            .withEntityIds(entityIds)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    assertEquals(size, insertResponse.getEntityIds().size());

    assertTrue(client.flush(binaryCollectionName).ok());

    GetEntityByIDResponse getEntityByIDResponse =
        client.getEntityByID(binaryCollectionName, entityIds.subList(0, 100));
    assertTrue(getEntityByIDResponse.ok());
    assertEquals(getEntityByIDResponse.getFieldsMap().size(), 100);
    List<Map<String, Object>> fieldsMap = getEntityByIDResponse.getFieldsMap();
    assertTrue(fieldsMap.get(0).get("binary_vec") instanceof List);
    List<Byte> first = (List<Byte>) (fieldsMap.get(0).get("binary_vec"));

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
    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    vectors = vectors.stream().map(MilvusClientTest::normalizeVector).collect(Collectors.toList());

    List<Long> insertIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .field(new FieldBuilder("int64", DataType.INT64)
                .values(intValues)
                .build())
            .field(new FieldBuilder("float", DataType.FLOAT)
                .values(floatValues)
                .build())
            .field(new FieldBuilder("float_vec", DataType.VECTOR_FLOAT)
                .values(vectors)
                .build())
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
    List<Long> intValues = new ArrayList<>(size);
    List<Float> floatValues = new ArrayList<>(size);
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    for (int i = 0; i < size; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    vectors = vectors.stream().map(MilvusClientTest::normalizeVector).collect(Collectors.toList());

    List<Long> insertIds = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .field(new FieldBuilder("int64", DataType.INT64)
                .values(intValues)
                .build())
            .field(new FieldBuilder("float", DataType.FLOAT)
                .values(floatValues)
                .build())
            .field(new FieldBuilder("float_vec", DataType.VECTOR_FLOAT)
                .values(vectors)
                .build())
            .withEntityIds(insertIds)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    assertEquals(size, insertResponse.getEntityIds().size());

    assertTrue(client.flush(randomCollectionName).ok());

    Response getCollectionStatsResponse = client.getCollectionStats(randomCollectionName);
    assertTrue(getCollectionStatsResponse.ok());

    JSONObject jsonInfo = new JSONObject(getCollectionStatsResponse.getMessage());
    long previousSegmentSize =
        jsonInfo
            .getJSONArray("partitions")
            .getJSONObject(0)
            .getLong("data_size");

    assertTrue(
        client.deleteEntityByID(randomCollectionName,
            insertResponse.getEntityIds().subList(0, size / 2)).ok());
    assertTrue(client.flush(randomCollectionName).ok());
    assertTrue(client.compact(
        new CompactParam.Builder(randomCollectionName).withThreshold(0.2).build()).ok());

    getCollectionStatsResponse = client.getCollectionStats(randomCollectionName);
    assertTrue(getCollectionStatsResponse.ok());

    jsonInfo = new JSONObject(getCollectionStatsResponse.getMessage());
    long currentSegmentSize =
        jsonInfo
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
    InsertParam insertParam =
        new Builder(randomCollectionName)
            .field(new FieldBuilder("int64", DataType.INT64)
                .values(intValues)
                .build())
            .field(new FieldBuilder("float", DataType.FLOAT)
                .values(floatValues)
                .build())
            .field(new FieldBuilder("float_vec", DataType.VECTOR_FLOAT)
                .values(vectors)
                .build())
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
