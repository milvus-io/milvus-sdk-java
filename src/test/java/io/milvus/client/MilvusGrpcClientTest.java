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
import org.apache.commons.text.RandomStringGenerator;
import org.json.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.*;

class MilvusClientTest {

  private MilvusClient client;

  private RandomStringGenerator generator;

  private String randomCollectionName;
  private long size;
  private long dimension;

  // Helper function that generates random float vectors
  static List<List<Float>> generateFloatVectors(long vectorCount, long dimension) {
    SplittableRandom splittableRandom = new SplittableRandom();
    List<List<Float>> vectors = new ArrayList<>();
    for (long i = 0; i < vectorCount; ++i) {
      splittableRandom = splittableRandom.split();
      DoubleStream doubleStream = splittableRandom.doubles(dimension);
      List<Float> vector =
          doubleStream.boxed().map(Double::floatValue).collect(Collectors.toList());
      vectors.add(vector);
    }
    return vectors;
  }

  // Helper function that generates random binary vectors
  static List<ByteBuffer> generateBinaryVectors(long vectorCount, long dimension) {
    Random random = new Random();
    List<ByteBuffer> vectors = new ArrayList<>();
    final long dimensionInByte = dimension / 8;
    for (long i = 0; i < vectorCount; ++i) {
      ByteBuffer byteBuffer = ByteBuffer.allocate((int) dimensionInByte);
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
        new CollectionMapping.Builder(randomCollectionName, dimension)
            .withIndexFileSize(1024)
            .withMetricType(MetricType.IP)
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
    // Channel should be idle
    assertFalse(client.isConnected());
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
  void isConnected() {
    assertTrue(client.isConnected());
  }

  @org.junit.jupiter.api.Test
  void createInvalidCollection() {
    String invalidCollectionName = "╯°□°）╯";
    CollectionMapping invalidCollectionMapping =
        new CollectionMapping.Builder(invalidCollectionName, dimension).build();
    Response createCollectionResponse = client.createCollection(invalidCollectionMapping);
    assertFalse(createCollectionResponse.ok());
    assertEquals(Response.Status.ILLEGAL_COLLECTION_NAME, createCollectionResponse.getStatus());
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

  @org.junit.jupiter.api.Test
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

    List<List<Float>> vectors1 = generateFloatVectors(size, dimension);
    List<Long> vectorIds1 = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new InsertParam.Builder(randomCollectionName)
            .withFloatVectors(vectors1)
            .withVectorIds(vectorIds1)
            .withPartitionTag(tag1)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<List<Float>> vectors2 = generateFloatVectors(size, dimension);
    List<Long> vectorIds2 = LongStream.range(size, size * 2).boxed().collect(Collectors.toList());
    insertParam =
        new InsertParam.Builder(randomCollectionName)
            .withFloatVectors(vectors2)
            .withVectorIds(vectorIds2)
            .withPartitionTag(tag2)
            .build();
    insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());

    assertTrue(client.flush(randomCollectionName).ok());

    assertEquals(size * 2, client.countEntities(randomCollectionName).getCollectionEntityCount());

    final int searchSize = 1;
    final long topK = 10;

    List<List<Float>> vectorsToSearch1 = vectors1.subList(0, searchSize);
    List<String> partitionTags1 = new ArrayList<>();
    partitionTags1.add(tag1);
    SearchParam searchParam1 =
        new SearchParam.Builder(randomCollectionName)
            .withFloatVectors(vectorsToSearch1)
            .withTopK(topK)
            .withPartitionTags(partitionTags1)
            .withParamsInJson("{\"nprobe\": 20}")
            .build();
    SearchResponse searchResponse1 = client.search(searchParam1);
    assertTrue(searchResponse1.ok());
    List<List<Long>> resultIdsList1 = searchResponse1.getResultIdsList();
    assertEquals(searchSize, resultIdsList1.size());
    assertTrue(vectorIds1.containsAll(resultIdsList1.get(0)));

    List<List<Float>> vectorsToSearch2 = vectors2.subList(0, searchSize);
    List<String> partitionTags2 = new ArrayList<>();
    partitionTags2.add(tag2);
    SearchParam searchParam2 =
        new SearchParam.Builder(randomCollectionName)
            .withFloatVectors(vectorsToSearch2)
            .withTopK(topK)
            .withPartitionTags(partitionTags2)
            .withParamsInJson("{\"nprobe\": 20}")
            .build();
    SearchResponse searchResponse2 = client.search(searchParam2);
    assertTrue(searchResponse2.ok());
    List<List<Long>> resultIdsList2 = searchResponse2.getResultIdsList();
    assertEquals(searchSize, resultIdsList2.size());
    assertTrue(vectorIds2.containsAll(resultIdsList2.get(0)));

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
        new Index.Builder(randomCollectionName, IndexType.IVF_SQ8)
            .withParamsInJson("{\"nlist\": 16384}")
            .build();

    Response createIndexResponse = client.createIndex(index);
    assertTrue(createIndexResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void createIndexAsync() throws ExecutionException, InterruptedException {
    insert();
    assertTrue(client.flush(randomCollectionName).ok());

    Index index =
        new Index.Builder(randomCollectionName, IndexType.IVF_SQ8)
            .withParamsInJson("{\"nlist\": 16384}")
            .build();

    ListenableFuture<Response> createIndexResponseFuture = client.createIndexAsync(index);
    Response createIndexResponse = createIndexResponseFuture.get();
    assertTrue(createIndexResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void insert() {
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    InsertParam insertParam =
        new InsertParam.Builder(randomCollectionName).withFloatVectors(vectors).build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    assertEquals(size, insertResponse.getVectorIds().size());
  }

  @org.junit.jupiter.api.Test
  void insertAsync() throws ExecutionException, InterruptedException {
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    InsertParam insertParam =
        new InsertParam.Builder(randomCollectionName).withFloatVectors(vectors).build();
    ListenableFuture<InsertResponse> insertResponseFuture = client.insertAsync(insertParam);
    InsertResponse insertResponse = insertResponseFuture.get();
    assertTrue(insertResponse.ok());
    assertEquals(size, insertResponse.getVectorIds().size());
  }

  @org.junit.jupiter.api.Test
  void insertBinary() {
    final long binaryDimension = 10000;

    String binaryCollectionName = generator.generate(10);
    CollectionMapping collectionMapping =
        new CollectionMapping.Builder(binaryCollectionName, binaryDimension)
            .withIndexFileSize(1024)
            .withMetricType(MetricType.JACCARD)
            .build();

    assertTrue(client.createCollection(collectionMapping).ok());

    List<ByteBuffer> vectors = generateBinaryVectors(size, binaryDimension);
    InsertParam insertParam =
        new InsertParam.Builder(binaryCollectionName).withBinaryVectors(vectors).build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    assertEquals(size, insertResponse.getVectorIds().size());

    assertTrue(client.dropCollection(binaryCollectionName).ok());
  }

  @org.junit.jupiter.api.Test
  void search() {
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    vectors = vectors.stream().map(MilvusClientTest::normalizeVector).collect(Collectors.toList());
    InsertParam insertParam =
        new InsertParam.Builder(randomCollectionName).withFloatVectors(vectors).build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<Long> vectorIds = insertResponse.getVectorIds();
    assertEquals(size, vectorIds.size());

    assertTrue(client.flush(randomCollectionName).ok());

    final int searchSize = 5;
    List<List<Float>> vectorsToSearch = vectors.subList(0, searchSize);

    final long topK = 10;
    SearchParam searchParam =
        new SearchParam.Builder(randomCollectionName)
            .withFloatVectors(vectorsToSearch)
            .withTopK(topK)
            .withParamsInJson("{\"nprobe\": 20}")
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
      assertEquals(vectorIds.get(i), firstQueryResult.getVectorId());
      assertEquals(vectorIds.get(i), resultIdsList.get(i).get(0));
      assertTrue(Math.abs(1 - firstQueryResult.getDistance()) < epsilon);
      assertTrue(Math.abs(1 - resultDistancesList.get(i).get(0)) < epsilon);
    }
  }

  @org.junit.jupiter.api.Test
  void searchAsync() throws ExecutionException, InterruptedException {
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    vectors = vectors.stream().map(MilvusClientTest::normalizeVector).collect(Collectors.toList());
    InsertParam insertParam =
        new InsertParam.Builder(randomCollectionName).withFloatVectors(vectors).build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<Long> vectorIds = insertResponse.getVectorIds();
    assertEquals(size, vectorIds.size());

    assertTrue(client.flush(randomCollectionName).ok());

    final int searchSize = 5;
    List<List<Float>> vectorsToSearch = vectors.subList(0, searchSize);

    final long topK = 10;
    SearchParam searchParam =
        new SearchParam.Builder(randomCollectionName)
            .withFloatVectors(vectorsToSearch)
            .withTopK(topK)
            .withParamsInJson("{\"nprobe\": 20}")
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
      assertEquals(vectorIds.get(i), firstQueryResult.getVectorId());
      assertEquals(vectorIds.get(i), resultIdsList.get(i).get(0));
      assertTrue(Math.abs(1 - firstQueryResult.getDistance()) < epsilon);
      assertTrue(Math.abs(1 - resultDistancesList.get(i).get(0)) < epsilon);
    }
  }

  @org.junit.jupiter.api.Test
  void searchBinary() {
    final long binaryDimension = 10000;

    String binaryCollectionName = generator.generate(10);
    CollectionMapping collectionMapping =
        new CollectionMapping.Builder(binaryCollectionName, binaryDimension)
            .withIndexFileSize(1024)
            .withMetricType(MetricType.JACCARD)
            .build();

    assertTrue(client.createCollection(collectionMapping).ok());

    List<ByteBuffer> vectors = generateBinaryVectors(size, binaryDimension);
    InsertParam insertParam =
        new InsertParam.Builder(binaryCollectionName).withBinaryVectors(vectors).build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<Long> vectorIds = insertResponse.getVectorIds();
    assertEquals(size, vectorIds.size());

    assertTrue(client.flush(binaryCollectionName).ok());

    final int searchSize = 5;
    List<ByteBuffer> vectorsToSearch = vectors.subList(0, searchSize);

    final long topK = 10;
    SearchParam searchParam =
        new SearchParam.Builder(binaryCollectionName)
            .withBinaryVectors(vectorsToSearch)
            .withTopK(topK)
            .withParamsInJson("{\"nprobe\": 20}")
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
      assertEquals(vectorIds.get(i), firstQueryResult.getVectorId());
      assertEquals(vectorIds.get(i), resultIdsList.get(i).get(0));
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
  void getIndexInfo() {
    createIndex();

    GetIndexInfoResponse getIndexInfoResponse = client.getIndexInfo(randomCollectionName);
    assertTrue(getIndexInfoResponse.ok());
    assertTrue(getIndexInfoResponse.getIndex().isPresent());
    assertEquals(getIndexInfoResponse.getIndex().get().getCollectionName(), randomCollectionName);
    assertEquals(getIndexInfoResponse.getIndex().get().getIndexType(), IndexType.IVF_SQ8);
  }

  @org.junit.jupiter.api.Test
  void dropIndex() {
    Response dropIndexResponse = client.dropIndex(randomCollectionName);
    assertTrue(dropIndexResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void getCollectionStats() {
    insert();

    assertTrue(client.flush(randomCollectionName).ok());

    Response getCollectionStatsResponse = client.getCollectionStats(randomCollectionName);
    assertTrue(getCollectionStatsResponse.ok());

    String jsonString = getCollectionStatsResponse.getMessage();
    JSONObject jsonInfo = new JSONObject(jsonString);
    assertTrue(jsonInfo.getInt("row_count") == size);

    JSONArray partitions = jsonInfo.getJSONArray("partitions");
    JSONObject partitionInfo = partitions.getJSONObject(0);
    assertEquals(partitionInfo.getString("tag"), "_default");
    assertEquals(partitionInfo.getInt("row_count"), size);

    JSONArray segments = partitionInfo.getJSONArray("segments");
    JSONObject segmentInfo = segments.getJSONObject(0);
    assertEquals(segmentInfo.getString("index_name"), "IDMAP");
    assertEquals(segmentInfo.getInt("row_count"), size);
  }

  @org.junit.jupiter.api.Test
  void getEntityByID() {
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    InsertParam insertParam =
        new InsertParam.Builder(randomCollectionName).withFloatVectors(vectors).build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<Long> vectorIds = insertResponse.getVectorIds();
    assertEquals(size, vectorIds.size());

    assertTrue(client.flush(randomCollectionName).ok());

    GetEntityByIDResponse getEntityByIDResponse =
        client.getEntityByID(randomCollectionName, vectorIds.subList(0, 100));
    assertTrue(getEntityByIDResponse.ok());
    ByteBuffer bb = getEntityByIDResponse.getBinaryVectors().get(0);
    assertTrue(bb == null || bb.remaining() == 0);

    assertArrayEquals(
        getEntityByIDResponse.getFloatVectors().get(0).toArray(), vectors.get(0).toArray());
  }

  @org.junit.jupiter.api.Test
  void getVectorIds() {
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
        client.listIDInSegment(randomCollectionName, segmentInfo.getString("name"));
    assertTrue(listIDInSegmentResponse.ok());
    assertFalse(listIDInSegmentResponse.getIds().isEmpty());
  }

  @org.junit.jupiter.api.Test
  void deleteEntityByID() {
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    InsertParam insertParam =
        new InsertParam.Builder(randomCollectionName).withFloatVectors(vectors).build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<Long> vectorIds = insertResponse.getVectorIds();
    assertEquals(size, vectorIds.size());

    assertTrue(client.flush(randomCollectionName).ok());

    assertTrue(client.deleteEntityByID(randomCollectionName, vectorIds.subList(0, 100)).ok());
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
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    InsertParam insertParam =
        new InsertParam.Builder(randomCollectionName).withFloatVectors(vectors).build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<Long> vectorIds = insertResponse.getVectorIds();
    assertEquals(size, vectorIds.size());

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
        client.deleteEntityByID(randomCollectionName, vectorIds.subList(0, (int) size / 2)).ok());
    assertTrue(client.flush(randomCollectionName).ok());
    assertTrue(client.compact(randomCollectionName).ok());

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
    List<List<Float>> vectors = generateFloatVectors(size, dimension);
    InsertParam insertParam =
        new InsertParam.Builder(randomCollectionName).withFloatVectors(vectors).build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<Long> vectorIds = insertResponse.getVectorIds();
    assertEquals(size, vectorIds.size());

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
        client.deleteEntityByID(randomCollectionName, vectorIds.subList(0, (int) size / 2)).ok());
    assertTrue(client.flush(randomCollectionName).ok());
    assertTrue(client.compactAsync(randomCollectionName).get().ok());

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
}
