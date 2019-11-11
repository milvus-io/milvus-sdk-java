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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import static org.junit.jupiter.api.Assertions.*;

class MilvusClientTest {

  private MilvusClient client;

  private RandomStringGenerator generator;

  private String randomTableName;
  private long size;
  private long dimension;

  // Helper function that generates random vectors
  static List<List<Float>> generateVectors(long vectorCount, long dimension) {
    SplittableRandom splittableRandom = new SplittableRandom();
    List<List<Float>> vectors = new ArrayList<>();
    for (int i = 0; i < vectorCount; ++i) {
      splittableRandom = splittableRandom.split();
      DoubleStream doubleStream = splittableRandom.doubles(dimension);
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
    randomTableName = generator.generate(10);
    size = 100000;
    dimension = 128;
    TableSchema tableSchema =
        new TableSchema.Builder(randomTableName, dimension)
            .withIndexFileSize(1024)
            .withMetricType(MetricType.IP)
            .build();

    assertTrue(client.createTable(tableSchema).ok());
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() throws InterruptedException {
    assertTrue(client.dropTable(randomTableName).ok());
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
    assertTrue(client.showTables().ok());
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
  void createInvalidTable() {
    String invalidTableName = "╯°□°）╯";
    TableSchema invalidTableSchema = new TableSchema.Builder(invalidTableName, dimension).build();
    Response createTableResponse = client.createTable(invalidTableSchema);
    assertFalse(createTableResponse.ok());
    assertEquals(Response.Status.ILLEGAL_TABLE_NAME, createTableResponse.getStatus());
  }

  @org.junit.jupiter.api.Test
  void hasTable() {
    HasTableResponse hasTableResponse = client.hasTable(randomTableName);
    assertTrue(hasTableResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void dropTable() {
    String nonExistingTableName = generator.generate(10);
    Response dropTableResponse = client.dropTable(nonExistingTableName);
    assertFalse(dropTableResponse.ok());
    assertEquals(Response.Status.TABLE_NOT_EXISTS, dropTableResponse.getStatus());
  }

  @org.junit.jupiter.api.Test
  void createIndex() {
    Index index = new Index.Builder().withIndexType(IndexType.IVF_SQ8).withNList(16384).build();
    CreateIndexParam createIndexParam =
        new CreateIndexParam.Builder(randomTableName).withIndex(index).build();
    Response createIndexResponse = client.createIndex(createIndexParam);
    assertTrue(createIndexResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void insert() {
    List<List<Float>> vectors = generateVectors(size, dimension);
    InsertParam insertParam = new InsertParam.Builder(randomTableName, vectors).build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.ok());
    assertEquals(size, insertResponse.getVectorIds().size());
  }

  @org.junit.jupiter.api.Test
  void search() throws InterruptedException, IOException {
    List<List<Float>> vectors = generateVectors(size, dimension);
    vectors = vectors.stream().map(MilvusClientTest::normalizeVector).collect(Collectors.toList());
    InsertParam insertParam = new InsertParam.Builder(randomTableName, vectors).build();
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
        new SearchParam.Builder(randomTableName, vectorsToSearch)
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
  void describeTable() {
    DescribeTableResponse describeTableResponse = client.describeTable(randomTableName);
    assertTrue(describeTableResponse.ok());
    assertTrue(describeTableResponse.getTableSchema().isPresent());

    String nonExistingTableName = generator.generate(10);
    describeTableResponse = client.describeTable(nonExistingTableName);
    assertFalse(describeTableResponse.ok());
    assertFalse(describeTableResponse.getTableSchema().isPresent());
  }

  @org.junit.jupiter.api.Test
  void showTables() {
    ShowTablesResponse showTablesResponse = client.showTables();
    assertTrue(showTablesResponse.ok());
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
  void getTableRowCount() throws InterruptedException {
    insert();
    TimeUnit.SECONDS.sleep(1);

    GetTableRowCountResponse getTableRowCountResponse = client.getTableRowCount(randomTableName);
    assertTrue(getTableRowCountResponse.ok());
    assertEquals(size, getTableRowCountResponse.getTableRowCount());
  }

  @org.junit.jupiter.api.Test
  void preloadTable() {
    Response preloadTableResponse = client.preloadTable(randomTableName);
    assertTrue(preloadTableResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void describeIndex() {
    DescribeIndexResponse describeIndexResponse = client.describeIndex(randomTableName);
    assertTrue(describeIndexResponse.ok());
    assertTrue(describeIndexResponse.getIndex().isPresent());
  }

  @org.junit.jupiter.api.Test
  void dropIndex() {
    Response dropIndexResponse = client.dropIndex(randomTableName);
    assertTrue(dropIndexResponse.ok());
  }
}
