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

import org.apache.commons.text.RandomStringGenerator;

import java.util.*;
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

  // Helper function that generates random vectors
  static List<List<Float>> generateVectors(long vectorCount, long dimension) {
    SplitcollectionRandom splitcollectionRandom = new SplitcollectionRandom();
    List<List<Float>> vectors = new ArrayList<>();
    for (int i = 0; i < vectorCount; ++i) {
      splitcollectionRandom = splitcollectionRandom.split();
      DoubleStream doubleStream = splitcollectionRandom.doubles(dimension);
      List<Float> vector =
          doubleStream.boxed().map(Double::floatValue).collect(Collectors.toList());
      vectors.add(vector);
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
        new ConnectParam.Builder().withHost("localhost").withPort("19530").build();
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
    assertTrue(client.showCollections().ok());
  }

  @org.junit.jupiter.api.Test
  void setInvalidConnectParam() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          ConnectParam connectParam = new ConnectParam.Builder().withPort("66666").build();
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
    CollectionMapping invalidCollectionMapping = new CollectionMapping.Builder(invalidCollectionName, dimension).build();
    Response createCollectionResponse = client.createCollection(invalidCollectionMapping);
    assertFalse(createCollectionResponse.ok());
    assertEquals(Response.Status.ILLEGAL_TABLE_NAME, createCollectionResponse.getStatus());
  }

  @org.junit.jupiter.api.Test
  void hasCollection() {
    HasCollectionResponse hasCollectionResponse = client.hasCollection(randomCollectionName);
    assertTrue(hasCollectionResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void dropCollection() {
    String nonExistingCollectionName = generator.generate(10);
    Response dropCollectionResponse = client.dropCollection(nonExistingCollectionName);
    assertFalse(dropCollectionResponse.ok());
    assertEquals(Response.Status.TABLE_NOT_EXISTS, dropCollectionResponse.getStatus());
  }

  @org.junit.jupiter.api.Test
  void partitionTest() throws InterruptedException {
    final String partitionName1 = "partition1";
    final String tag1 = "tag1";

    Partition partition = new Partition.Builder(randomCollectionName, partitionName1, tag1).build();
    Response createPartitionResponse = client.createPartition(partition);
    assertTrue(createPartitionResponse.ok());

    final String partitionName2 = "partition2";
    final String tag2 = "tag2";

    Partition partition2 = new Partition.Builder(randomCollectionName, partitionName2, tag2).build();
    createPartitionResponse = client.createPartition(partition2);
    assertTrue(createPartitionResponse.ok());

    ShowPartitionsResponse showPartitionsResponse = client.showPartitions(randomCollectionName);
    assertTrue(showPartitionsResponse.ok());
    assertEquals(2, showPartitionsResponse.getPartitionList().size());

    List<List<Float>> vectors1 = generateVectors(size, dimension);
    List<Long> vectorIds1 = LongStream.range(0, size).boxed().collect(Collectors.toList());
    InsertParam insertParam =
        new InsertParam.Builder(randomCollectionName, vectors1)
            .withVectorIds(vectorIds1)
            .withPartitionTag(tag1)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<List<Float>> vectors2 = generateVectors(size, dimension);
    List<Long> vectorIds2 = LongStream.range(size, size * 2).boxed().collect(Collectors.toList());
    insertParam =
        new InsertParam.Builder(randomCollectionName, vectors2)
            .withVectorIds(vectorIds2)
            .withPartitionTag(tag2)
            .build();
    insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());

    TimeUnit.SECONDS.sleep(1);

    assertEquals(size * 2, client.getCollectionRowCount(randomCollectionName).getCollectionRowCount());

    final int searchSize = 1;
    final long topK = 10;

    List<List<Float>> vectorsToSearch1 = vectors1.subList(0, searchSize);
    List<String> partitionTags1 = new ArrayList<>();
    partitionTags1.add(tag1);
    SearchParam searchParam1 =
        new SearchParam.Builder(randomCollectionName, vectorsToSearch1)
            .withTopK(topK)
            .withNProbe(20)
            .withPartitionTags(partitionTags1)
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
        new SearchParam.Builder(randomCollectionName, vectorsToSearch2)
            .withTopK(topK)
            .withNProbe(20)
            .withPartitionTags(partitionTags2)
            .build();
    SearchResponse searchResponse2 = client.search(searchParam2);
    assertTrue(searchResponse2.ok());
    List<List<Long>> resultIdsList2 = searchResponse2.getResultIdsList();
    assertEquals(searchSize, resultIdsList2.size());
    assertTrue(vectorIds2.containsAll(resultIdsList2.get(0)));

    assertTrue(Collections.disjoint(resultIdsList1, resultIdsList2));

    Response dropPartitionResponse = client.dropPartition(partitionName1);
    assertTrue(dropPartitionResponse.ok());

    dropPartitionResponse = client.dropPartition(randomCollectionName, tag2);
    assertTrue(dropPartitionResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void createIndex() {
    Index index = new Index.Builder().withIndexType(IndexType.IVF_SQ8).withNList(16384).build();
    CreateIndexParam createIndexParam =
        new CreateIndexParam.Builder(randomCollectionName).withIndex(index).build();
    Response createIndexResponse = client.createIndex(createIndexParam);
    assertTrue(createIndexResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void insert() {
    List<List<Float>> vectors = generateVectors(size, dimension);
    InsertParam insertParam = new InsertParam.Builder(randomCollectionName, vectors).build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    assertEquals(size, insertResponse.getVectorIds().size());
  }

  @org.junit.jupiter.api.Test
  void search() throws InterruptedException {
    List<List<Float>> vectors = generateVectors(size, dimension);
    vectors = vectors.stream().map(MilvusClientTest::normalizeVector).collect(Collectors.toList());
    InsertParam insertParam = new InsertParam.Builder(randomCollectionName, vectors).build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    List<Long> vectorIds = insertResponse.getVectorIds();
    assertEquals(size, vectorIds.size());

    TimeUnit.SECONDS.sleep(1);

    final int searchSize = 5;
    List<List<Float>> vectorsToSearch = vectors.subList(0, searchSize);

    List<DateRange> queryRanges = new ArrayList<>();
    Date today = new Date();
    Calendar c = Calendar.getInstance();
    c.setTime(today);
    c.add(Calendar.DAY_OF_MONTH, -1);
    Date yesterday = c.getTime();
    c.setTime(today);
    c.add(Calendar.DAY_OF_MONTH, 1);
    Date tomorrow = c.getTime();
    queryRanges.add(new DateRange(yesterday, tomorrow));
    final long topK = 10;
    SearchParam searchParam =
        new SearchParam.Builder(randomCollectionName, vectorsToSearch)
            .withTopK(topK)
            .withNProbe(20)
            .withDateRanges(queryRanges)
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

  //    @org.junit.jupiter.api.Test
  //    void searchInFiles() {
  //    }

  @org.junit.jupiter.api.Test
  void describeCollection() {
    DescribeCollectionResponse describeCollectionResponse = client.describeCollection(randomCollectionName);
    assertTrue(describeCollectionResponse.ok());
    assertTrue(describeCollectionResponse.getCollectionMapping().isPresent());

    String nonExistingCollectionName = generator.generate(10);
    describeCollectionResponse = client.describeCollection(nonExistingCollectionName);
    assertFalse(describeCollectionResponse.ok());
    assertFalse(describeCollectionResponse.getCollectionMapping().isPresent());
  }

  @org.junit.jupiter.api.Test
  void showCollections() {
    ShowCollectionsResponse showCollectionsResponse = client.showCollections();
    assertTrue(showCollectionsResponse.ok());
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
  void getCollectionRowCount() throws InterruptedException {
    insert();
    TimeUnit.SECONDS.sleep(1);

    GetCollectionRowCountResponse getCollectionRowCountResponse = client.getCollectionRowCount(randomCollectionName);
    assertTrue(getCollectionRowCountResponse.ok());
    assertEquals(size, getCollectionRowCountResponse.getCollectionRowCount());
  }

  @org.junit.jupiter.api.Test
  void preloadCollection() {
    Response preloadCollectionResponse = client.preloadCollection(randomCollectionName);
    assertTrue(preloadCollectionResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void describeIndex() {
    DescribeIndexResponse describeIndexResponse = client.describeIndex(randomCollectionName);
    assertTrue(describeIndexResponse.ok());
    assertTrue(describeIndexResponse.getIndex().isPresent());
  }

  @org.junit.jupiter.api.Test
  void dropIndex() {
    Response dropIndexResponse = client.dropIndex(randomCollectionName);
    assertTrue(dropIndexResponse.ok());
  }
}
