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

import io.milvus.client.*;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

public class MilvusClientExample {

  // Helper function that generates random vectors
  static List<List<Float>> generateRandomVectors(long vectorCount, long dimension) {
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
  static List<Float> normalize(List<Float> vector) {
    float squareSum = vector.stream().map(x -> x * x).reduce((float) 0, Float::sum);
    final float norm = (float) Math.sqrt(squareSum);
    vector = vector.stream().map(x -> x / norm).collect(Collectors.toList());
    return vector;
  }

  public static void main(String[] args) throws InterruptedException {

    final String host = "localhost";
    final String port = "19530";

    // Create Milvus client
    MilvusClient client = new MilvusGrpcClient();

    // Connect to Milvus server
    ConnectParam connectParam = new ConnectParam.Builder().withHost(host).withPort(port).build();
    Response connectResponse = client.connect(connectParam);
    System.out.println(connectResponse);

    // Check whether we are connected
    boolean connected = client.connected();
    System.out.println("Connected = " + connected);

    // Create a table with the following table schema
    final String tableName = "example";
    final long dimension = 128;
    final long indexFileSize = 1024;
    final MetricType metricType = MetricType.IP;
    TableSchema tableSchema =
        new TableSchema.Builder(tableName, dimension)
            .withIndexFileSize(indexFileSize)
            .withMetricType(metricType)
            .build();
    TableSchemaParam tableSchemaParam =
        new TableSchemaParam.Builder(tableSchema).withTimeout(10).build();
    Response createTableResponse = client.createTable(tableSchemaParam);
    System.out.println(createTableResponse);

    // Check whether the table exists
    TableParam hasTableParam = new TableParam.Builder(tableName).withTimeout(1).build();
    HasTableResponse hasTableResponse = client.hasTable(hasTableParam);
    System.out.println(hasTableResponse);

    // Describe the table
    TableParam describeTableParam = new TableParam.Builder(tableName).withTimeout(1).build();
    DescribeTableResponse describeTableResponse = client.describeTable(describeTableParam);
    System.out.println(describeTableResponse);

    // Insert randomly generated vectors to table
    final int vectorCount = 100000;
    List<List<Float>> vectors = generateRandomVectors(vectorCount, dimension);
    vectors.forEach(MilvusClientExample::normalize);
    InsertParam insertParam = new InsertParam.Builder(tableName, vectors).withTimeout(10).build();
    InsertResponse insertResponse = client.insert(insertParam);
    System.out.println(insertResponse);
    // Insert returns a list of vector ids that you will be using (if you did not supply them
    // yourself) to reference the vectors you just inserted
    List<Long> vectorIds = insertResponse.getVectorIds();

    // Sleep for 1 second
    TimeUnit.SECONDS.sleep(1);

    // Get current row count of table
    TableParam getTableRowCountParam = new TableParam.Builder(tableName).withTimeout(1).build();
    GetTableRowCountResponse getTableRowCountResponse =
        client.getTableRowCount(getTableRowCountParam);
    System.out.println(getTableRowCountResponse);

    // Create index for the table
    final IndexType indexType = IndexType.IVF_SQ8;
    Index index = new Index.Builder().withIndexType(IndexType.IVF_SQ8).build();
    CreateIndexParam createIndexParam =
        new CreateIndexParam.Builder(tableName).withIndex(index).withTimeout(10).build();
    Response createIndexResponse = client.createIndex(createIndexParam);
    System.out.println(createIndexResponse);

    // Describe the index for your table
    TableParam describeIndexParam = new TableParam.Builder(tableName).withTimeout(1).build();
    DescribeIndexResponse describeIndexResponse = client.describeIndex(describeIndexParam);
    System.out.println(describeIndexResponse);

    // Search vectors
    final int searchSize = 5;
    // Searching the first 5 vectors of the vectors we just inserted
    List<List<Float>> vectorsToSearch = vectors.subList(0, searchSize);
    final long topK = 10;
    SearchParam searchParam =
        new SearchParam.Builder(tableName, vectorsToSearch).withTopK(topK).withTimeout(10).build();
    SearchResponse searchResponse = client.search(searchParam);
    System.out.println(searchResponse);
    List<List<SearchResponse.QueryResult>> queryResultsList = searchResponse.getQueryResultsList();
    final double epsilon = 0.001;
    for (int i = 0; i < searchSize; i++) {
      // Since we are searching for vector that is already present in the table,
      // the first result vector should be itself and the distance (inner product) should be
      // very close to 1 (some precision is lost during the process)
      SearchResponse.QueryResult firstQueryResult = queryResultsList.get(i).get(0);
      if (firstQueryResult.getVectorId() != vectorIds.get(i)
          || firstQueryResult.getDistance() <= (1 - epsilon)) {
        throw new AssertionError();
      }
    }

    // Drop index for the table
    TableParam dropIndexParam = new TableParam.Builder(tableName).withTimeout(1).build();
    Response dropIndexResponse = client.dropIndex(dropIndexParam);
    System.out.println(dropIndexResponse);

    // Drop table
    TableParam dropTableParam = new TableParam.Builder(tableName).withTimeout(1).build();
    Response dropTableResponse = client.dropTable(dropTableParam);
    System.out.println(dropTableResponse);

    // Disconnect from Milvus server
    Response disconnectResponse = client.disconnect();
    System.out.println(disconnectResponse);
  }
}
