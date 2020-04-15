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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonObject;
import io.milvus.client.*;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

// This is a simple example demonstrating how to use the Milvus Java SDK.
// For detailed API document, please refer to
// https://milvus-io.github.io/milvus-sdk-java/javadoc/io/milvus/client/package-summary.html
// You can also find more information in https://milvus.io/
public class MilvusClientExample {

  // Helper function that generates random vectors
  static List<List<Float>> generateVectors(long vectorCount, long dimension) {
    SplittableRandom splitcollectionRandom = new SplittableRandom();
    List<List<Float>> vectors = new ArrayList<>();
    for (long i = 0; i < vectorCount; ++i) {
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
    final int port = 19530;

    // Create Milvus client
    // You can specify the log level. Currently we have three levels of logs: INFO, WARNING and
    // SEVERE
    MilvusClient client = new MilvusGrpcClient(Level.ALL);

    // Connect to Milvus server
    ConnectParam connectParam = new ConnectParam.Builder().withHost(host).withPort(port).build();
    try {
      Response connectResponse = client.connect(connectParam);
    } catch (ConnectFailedException e) {
      System.out.println("Failed to connect to Milvus server: " + e.toString());
      throw e;
    }

    // Check whether we are connected
    boolean connected = client.isConnected();

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

    // Check whether the collection exists
    HasCollectionResponse hasCollectionResponse = client.hasCollection(collectionName);

    // Describe the collection
    DescribeCollectionResponse describeCollectionResponse =
        client.describeCollection(collectionName);

    // Insert randomly generated vectors to collection
    final int vectorCount = 100000;
    List<List<Float>> vectors = generateVectors(vectorCount, dimension);
    vectors =
        vectors.stream().map(MilvusClientExample::normalizeVector).collect(Collectors.toList());
    InsertParam insertParam =
        new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
    InsertResponse insertResponse = client.insert(insertParam);
    // Insert returns a list of vector ids that you will be using (if you did not supply them
    // yourself) to reference the vectors you just inserted
    List<Long> vectorIds = insertResponse.getVectorIds();

    // Flush data in collection
    Response flushResponse = client.flush(collectionName);

    // Get current row count of collection
    GetCollectionRowCountResponse getCollectionRowCountResponse =
        client.getCollectionRowCount(collectionName);

    // Create index for the collection
    // We choose IVF_SQ8 as our index type here. Refer to IndexType javadoc for a
    // complete explanation of different index types
    final IndexType indexType = IndexType.IVF_SQ8;
    // Each index type has its optional parameters you can set. Refer to the Milvus documentation
    // for how to set the optimal parameters based on your needs.
    JsonObject indexParamsJson = new JsonObject();
    indexParamsJson.addProperty("nlist", 16384);
    Index index =
        new Index.Builder(collectionName, indexType)
            .withParamsInJson(indexParamsJson.toString())
            .build();
    Response createIndexResponse = client.createIndex(index);

    // Describe the index for your collection
    DescribeIndexResponse describeIndexResponse = client.describeIndex(collectionName);

    // Search vectors
    // Searching the first 5 vectors of the vectors we just inserted
    final int searchBatchSize = 5;
    List<List<Float>> vectorsToSearch = vectors.subList(0, searchBatchSize);
    final long topK = 10;
    // Based on the index you created, the available search parameters will be different. Refer to
    // the Milvus documentation for how to set the optimal parameters based on your needs.
    JsonObject searchParamsJson = new JsonObject();
    searchParamsJson.addProperty("nprobe", 20);
    SearchParam searchParam =
        new SearchParam.Builder(collectionName)
            .withFloatVectors(vectorsToSearch)
            .withTopK(topK)
            .withParamsInJson(searchParamsJson.toString())
            .build();
    SearchResponse searchResponse = client.search(searchParam);
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
    // You can also get result ids and distances separately
    List<List<Long>> resultIds = searchResponse.getResultIdsList();
    List<List<Float>> resultDistances = searchResponse.getResultDistancesList();

    // You can send search request asynchronously, which returns a ListenableFuture object
    ListenableFuture<SearchResponse> searchResponseFuture = client.searchAsync(searchParam);
    try {
      // Get search response immediately. Obviously you will want to do more complicated stuff with
      // ListenableFuture
      searchResponseFuture.get();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }

    // Delete the first 5 of vectors you just searched
    Response deleteByIdsResponse =
        client.deleteByIds(collectionName, vectorIds.subList(0, searchBatchSize));

    // Flush again, so deletions to data become visible
    flushResponse = client.flush(collectionName);

    // Try to get the corresponding vector of the first id you just deleted.
    GetVectorByIdResponse getVectorByIdResponse =
        client.getVectorById(collectionName, vectorIds.get(0));
    // Obviously you won't get anything
    if (getVectorByIdResponse.exists()) {
      throw new AssertionError("This can never happen!");
    }

    // Compact the collection, erasing deleted data from disk and rebuild index in background (if
    // the data size after compaction is still larger than indexFileSize). Data was only
    // soft-deleted until you call compact.
    Response compactResponse = client.compact(collectionName);

    // Drop index for the collection
    Response dropIndexResponse = client.dropIndex(collectionName);

    // Drop collection
    Response dropCollectionResponse = client.dropCollection(collectionName);

    // Disconnect from Milvus server
    try {
      Response disconnectResponse = client.disconnect();
    } catch (InterruptedException e) {
      System.out.println("Failed to disconnect: " + e.toString());
      throw e;
    }
  }
}
