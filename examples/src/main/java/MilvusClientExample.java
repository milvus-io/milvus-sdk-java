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

import io.milvus.client.*;

import java.util.ArrayList;
import java.util.List;
import java.util.SplitcollectionRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

public class MilvusClientExample {

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

  public static void main(String[] args) throws InterruptedException, ConnectFailedException {

    // You may need to change the following to the host and port of your Milvus server
    final String host = "localhost";
    final String port = "19530";

    // Create Milvus client
    MilvusClient client = new MilvusGrpcClient();

    // Connect to Milvus server
    ConnectParam connectParam = new ConnectParam.Builder().withHost(host).withPort(port).build();
    try {
      Response connectResponse = client.connect(connectParam);
    } catch (ConnectFailedException e) {
      System.out.println(e.toString());
      throw e;
    }

    // Check whether we are connected
    boolean connected = client.isConnected();
    System.out.println("Connected = " + connected);

    // Create a collection with the following collection mapping
    final String collectionName = "example"; // collection name
    final long dimension = 128; // dimension of each vector
    final long indexFileSize = 1024; // maximum size (in MB) of each index file
    final MetricType metricType = MetricType.IP; // we choose IP (Inner Product) as our metric type
    CollectionMapping collectionMapping =
        new CollectionMapping.Builder(collectionName, dimension)
            .withIndexFileSize(indexFileSize)
            .withMetricType(metricType)
            .build();
    Response createCollectionResponse = client.createCollection(collectionMapping);
    System.out.println(createCollectionResponse);

    // Check whether the collection exists
    HasCollectionResponse hasCollectionResponse = client.hasCollection(collectionName);
    System.out.println(hasCollectionResponse);

    // Describe the collection
    DescribeCollectionResponse describeCollectionResponse = client.describeCollection(collectionName);
    System.out.println(describeCollectionResponse);

    // Insert randomly generated vectors to collection
    final int vectorCount = 100000;
    List<List<Float>> vectors = generateVectors(vectorCount, dimension);
    vectors =
        vectors.stream().map(MilvusClientExample::normalizeVector).collect(Collectors.toList());
    InsertParam insertParam = new InsertParam.Builder(collectionName, vectors).build();
    InsertResponse insertResponse = client.insert(insertParam);
    System.out.println(insertResponse);
    // Insert returns a list of vector ids that you will be using (if you did not supply them
    // yourself) to reference the vectors you just inserted
    List<Long> vectorIds = insertResponse.getVectorIds();

    // The data we just inserted won't be serialized and written to meta until the next second
    // wait 1 second here
    TimeUnit.SECONDS.sleep(1);

    // Get current row count of collection
    GetCollectionRowCountResponse getCollectionRowCountResponse = client.getCollectionRowCount(collectionName);
    System.out.println(getCollectionRowCountResponse);

    // Create index for the collection
    // We choose IVF_SQ8 as our index type here. Refer to IndexType javadoc for a
    // complete explanation of different index types
    final IndexType indexType = IndexType.IVF_SQ8;
    Index index = new Index.Builder().withIndexType(indexType).build();
    CreateIndexParam createIndexParam =
        new CreateIndexParam.Builder(collectionName).withIndex(index).build();
    Response createIndexResponse = client.createIndex(createIndexParam);
    System.out.println(createIndexResponse);

    // Describe the index for your collection
    DescribeIndexResponse describeIndexResponse = client.describeIndex(collectionName);
    System.out.println(describeIndexResponse);

    // Search vectors
    // Searching the first 5 vectors of the vectors we just inserted
    final int searchBatchSize = 5;
    List<List<Float>> vectorsToSearch = vectors.subList(0, searchBatchSize);
    final long topK = 10;
    SearchParam searchParam =
        new SearchParam.Builder(collectionName, vectorsToSearch).withTopK(topK).build();
    SearchResponse searchResponse = client.search(searchParam);
    System.out.println(searchResponse);
    if (searchResponse.ok()) {
      List<List<SearchResponse.QueryResult>> queryResultsList =
          searchResponse.getQueryResultsList();
      final double epsilon = 0.001;
      for (int i = 0; i < searchBatchSize; i++) {
        // Since we are searching for vector that is already present in the collection,
        // the first result vector should be itself and the distance (inner product) should be
        // very close to 1 (some precision is lost during the process)
        SearchResponse.QueryResult firstQueryResult = queryResultsList.get(i).get(0);
        if (firstQueryResult.getVectorId() != vectorIds.get(i)
            || Math.abs(1 - firstQueryResult.getDistance()) > epsilon) {
          throw new AssertionError("Wrong results!");
        }
      }
    }

    // Drop index for the collection
    Response dropIndexResponse = client.dropIndex(collectionName);
    System.out.println(dropIndexResponse);

    // Drop collection
    Response dropCollectionResponse = client.dropCollection(collectionName);
    System.out.println(dropCollectionResponse);

    // Disconnect from Milvus server
    try {
      Response disconnectResponse = client.disconnect();
    } catch (InterruptedException e) {
      System.out.println("Failed to disconnect: " + e.toString());
      throw e;
    }
  }
}
