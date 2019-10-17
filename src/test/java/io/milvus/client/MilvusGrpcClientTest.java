/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.milvus.client;

import org.apache.commons.text.RandomStringGenerator;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import static org.junit.jupiter.api.Assertions.*;

class MilvusGrpcClientTest {

  private MilvusGrpcClient client;

  private RandomStringGenerator generator;

  private String randomTableName;
  private long size;
  private long dimension;
  private TableParam tableParam;

  // Helper function that generates random vectors
  static List<List<Float>> generateVectors(long vectorCount, long dimension) {
    SplittableRandom splittableRandom = new SplittableRandom();
    List<List<Float>> vectors = new ArrayList<>();
    for (int i = 0; i < vectorCount; ++i) {
      DoubleStream doubleStream = splittableRandom.doubles(dimension);
      List<Float> vector =
          doubleStream.boxed().map(Double::floatValue).collect(Collectors.toList());
      vectors.add(vector);
    }
    return vectors;
  }

  // Helper function that normalizes a vector if you are using IP (Inner product) as your metric
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
    tableParam = new TableParam.Builder(randomTableName).build();
    TableSchema tableSchema = new TableSchema.Builder(randomTableName, dimension)
            .withIndexFileSize(1024)
            .withMetricType(MetricType.IP)
            .build();
    TableSchemaParam tableSchemaParam = new TableSchemaParam.Builder(tableSchema).build();

    assertTrue(client.createTable(tableSchemaParam).ok());
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() throws InterruptedException {
    assertTrue(client.dropTable(tableParam).ok());
    client.disconnect();
  }

  @org.junit.jupiter.api.Test
  void isConnected() {
    assertTrue(client.isConnected());
  }

  @org.junit.jupiter.api.Test
  void createTable() {
    String invalidTableName = "╯°□°）╯";
    TableSchema invalidTableSchema = new TableSchema.Builder(invalidTableName, dimension).build();
    TableSchemaParam invalidTableSchemaParam =
        new TableSchemaParam.Builder(invalidTableSchema).withTimeout(20).build();
    Response createTableResponse = client.createTable(invalidTableSchemaParam);
    assertFalse(createTableResponse.ok());
    assertEquals(Response.Status.ILLEGAL_TABLE_NAME, createTableResponse.getStatus());
  }

  @org.junit.jupiter.api.Test
  void hasTable() {
    HasTableResponse hasTableResponse = client.hasTable(tableParam);
    assertTrue(hasTableResponse.getResponse().ok());
  }

  @org.junit.jupiter.api.Test
  void dropTable() {
    String nonExistingTableName = generator.generate(10);
    TableParam tableParam = new TableParam.Builder(nonExistingTableName).build();
    Response dropTableResponse = client.dropTable(tableParam);
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
    assertTrue(insertResponse.getResponse().ok());
    assertEquals(size, insertResponse.getVectorIds().size());
  }

  @org.junit.jupiter.api.Test
  void search() throws InterruptedException {
    List<List<Float>> vectors = generateVectors(size, dimension);
    vectors.forEach(MilvusGrpcClientTest::normalizeVector);
    InsertParam insertParam = new InsertParam.Builder(randomTableName, vectors).build();
    InsertResponse insertResponse = client.insert(insertParam);
    assertTrue(insertResponse.getResponse().ok());
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
    System.out.println(queryRanges);
    final long topK = 1000;
    SearchParam searchParam =
        new SearchParam.Builder(randomTableName, vectorsToSearch)
            .withTopK(topK)
            .withNProbe(20)
            .withDateRanges(queryRanges)
            .build();
    SearchResponse searchResponse = client.search(searchParam);
    assertTrue(searchResponse.getResponse().ok());
    System.out.println(searchResponse);
    List<List<SearchResponse.QueryResult>> queryResultsList = searchResponse.getQueryResultsList();
    assertEquals(searchSize, queryResultsList.size());
    final double epsilon = 0.001;
    for (int i = 0; i < searchSize; i++) {
      SearchResponse.QueryResult firstQueryResult = queryResultsList.get(i).get(0);
      assertEquals(vectorIds.get(i), firstQueryResult.getVectorId());
      assertTrue(firstQueryResult.getDistance() > (1 - epsilon));
    }
  }

  //    @org.junit.jupiter.api.Test
  //    void searchInFiles() {
  //    }

  @org.junit.jupiter.api.Test
  void describeTable() {
    DescribeTableResponse describeTableResponse = client.describeTable(tableParam);
    assertTrue(describeTableResponse.getResponse().ok());
    assertTrue(describeTableResponse.getTableSchema().isPresent());

    String nonExistingTableName = generator.generate(10);
    TableParam tableParam = new TableParam.Builder(nonExistingTableName).build();
    describeTableResponse = client.describeTable(tableParam);
    assertFalse(describeTableResponse.getResponse().ok());
    assertFalse(describeTableResponse.getTableSchema().isPresent());
  }

  @org.junit.jupiter.api.Test
  void showTables() {
    ShowTablesResponse showTablesResponse = client.showTables();
    assertTrue(showTablesResponse.getResponse().ok());
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

    GetTableRowCountResponse getTableRowCountResponse = client.getTableRowCount(tableParam);
    assertTrue(getTableRowCountResponse.getResponse().ok());
    assertEquals(size, getTableRowCountResponse.getTableRowCount());
  }

  @org.junit.jupiter.api.Test
  void deleteByRange() {
    Date today = new Date();
    Calendar c = Calendar.getInstance();
    c.setTime(today);
    c.add(Calendar.DAY_OF_MONTH, -1);
    Date yesterday = c.getTime();
    c.setTime(today);
    c.add(Calendar.DAY_OF_MONTH, 1);
    Date tomorrow = c.getTime();

    DeleteByRangeParam deleteByRangeParam =
        new DeleteByRangeParam.Builder(new DateRange(yesterday, tomorrow), randomTableName).build();
    Response deleteByRangeResponse = client.deleteByRange(deleteByRangeParam);
    assertTrue(deleteByRangeResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void preloadTable() {
    Response preloadTableResponse = client.preloadTable(tableParam);
    assertTrue(preloadTableResponse.ok());
  }

  @org.junit.jupiter.api.Test
  void describeIndex() {
    DescribeIndexResponse describeIndexResponse = client.describeIndex(tableParam);
    assertTrue(describeIndexResponse.getResponse().ok());
    assertTrue(describeIndexResponse.getIndex().isPresent());
  }

  @org.junit.jupiter.api.Test
  void dropIndex() {
    Response dropIndexResponse = client.dropIndex(tableParam);
    assertTrue(dropIndexResponse.ok());
  }
}
