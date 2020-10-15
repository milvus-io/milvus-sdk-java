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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.milvus.client.*;
import org.testcontainers.containers.GenericContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

// This is a simple example demonstrating how to use Milvus Java SDK v0.9.0.
// For detailed API documentation, please refer to
// https://milvus-io.github.io/milvus-sdk-java/javadoc/io/milvus/client/package-summary.html
// You can also find more information on https://milvus.io/docs/overview.md
public class MilvusClientExample {

  // Helper function that generates random vectors
  static List<List<Float>> generateVectors(int vectorCount, int dimension) {
    SplittableRandom splitCollectionRandom = new SplittableRandom();
    List<List<Float>> vectors = new ArrayList<>(vectorCount);
    for (int i = 0; i < vectorCount; ++i) {
      splitCollectionRandom = splitCollectionRandom.split();
      DoubleStream doubleStream = splitCollectionRandom.doubles(dimension);
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

  public static void main(String[] args) throws InterruptedException {
    String dockerImage = System.getProperty("docker_image_name", "milvusdb/milvus:0.11.0-cpu");
    try (GenericContainer milvusContainer = new GenericContainer(dockerImage).withExposedPorts(19530)) {
      milvusContainer.start();
      ConnectParam connectParam = new ConnectParam.Builder()
          .withHost("localhost")
          .withPort(milvusContainer.getFirstMappedPort())
          .build();
      run(connectParam);
    }
  }

  public static void run(ConnectParam connectParam) {
    // Create Milvus client
    MilvusClient client = new MilvusGrpcClient(connectParam).withLogging();

    // Create a collection with the following collection mapping
    final String collectionName = "example"; // collection name
    final int dimension = 128; // dimension of each vector
    // we choose IP (Inner Product) as our metric type
    CollectionMapping collectionMapping = CollectionMapping
        .create(collectionName)
        .addField("int64", DataType.INT64)
        .addField("float", DataType.FLOAT)
        .addVectorField("float_vec", DataType.VECTOR_FLOAT, dimension)
        .setParamsInJson("{\"segment_row_limit\": 50000, \"auto_id\": true}");

    client.createCollection(collectionMapping);

    if (!client.hasCollection(collectionName)) {
      throw new AssertionError("Collection not found");
    }

    System.out.println(collectionMapping.toString());

    // Get collection info
    CollectionMapping collectionInfo = client.getCollectionInfo(collectionName);

    // Insert randomly generated field values to collection
    final int vectorCount = 100000;

    List<Long> longValues = LongStream.range(0, vectorCount).boxed().collect(Collectors.toList());
    List<Float> floatValues = LongStream.range(0, vectorCount).boxed().map(Long::floatValue).collect(Collectors.toList());
    List<List<Float>> vectors = generateVectors(vectorCount, dimension).stream()
        .map(MilvusClientExample::normalizeVector)
        .collect(Collectors.toList());

    InsertParam insertParam = InsertParam
        .create(collectionName)
        .addField("int64", DataType.INT64, longValues)
        .addField("float", DataType.FLOAT, floatValues)
        .addVectorField("float_vec", DataType.VECTOR_FLOAT, vectors);

    // Insert returns a list of entity ids that you will be using (if you did not supply them
    // yourself) to reference the entities you just inserted
    List<Long> vectorIds = client.insert(insertParam);

    // Flush data in collection
    client.flush(collectionName);

    // Get current entity count of collection
    long entityCount = client.countEntities(collectionName);

    // Create index for the collection
    // We choose IVF_SQ8 as our index type here. Refer to Milvus documentation for a
    // complete explanation of different index types and their relative parameters.
    Index index = Index
        .create(collectionName, "float_vec")
        .setIndexType(IndexType.IVF_SQ8)
        .setMetricType(MetricType.L2)
        .setParamsInJson(new JsonBuilder().param("nlist", 2048).build());

    client.createIndex(index);

    // Get collection info
    String collectionStats = client.getCollectionStats(collectionName);
    System.out.format("Collection Stats: %s\n", collectionStats);

    // Check whether a partition exists in collection
    // Obviously we do not have partition "tag" now
    if (client.hasPartition(collectionName, "tag")) {
      throw new AssertionError("Unexpected partition found!");
    }

    // Search entities using DSL statement.
    // Searching the first 5 entities we just inserted by including them in DSL.
    final int searchBatchSize = 5;
    List<List<Float>> vectorsToSearch = vectors.subList(0, searchBatchSize);
    final long topK = 10;
    // Based on the index you created, the available search parameters will be different. Refer to
    // the Milvus documentation for how to set the optimal parameters based on your needs.
    String dsl = String.format(
        "{\"bool\": {"
            + "\"must\": [{"
            + "    \"range\": {"
            + "        \"float\": {\"GT\": -10, \"LT\": 100}"
            + "    }},{"
            + "    \"vector\": {"
            + "        \"float_vec\": {"
            + "            \"topk\": %d, \"metric_type\": \"IP\", \"type\": \"float\", \"query\": "
            + "%s, \"params\": {\"nprobe\": 50}"
            + "    }}}]}}",
        topK, vectorsToSearch.toString());
    SearchParam searchParam = SearchParam
        .create(collectionName)
        .setDsl(dsl)
        .setParamsInJson("{\"fields\": [\"int64\", \"float\"]}");
    SearchResult searchResult = client.search(searchParam);
    List<List<SearchResult.QueryResult>> queryResultsList = searchResult.getQueryResultsList();
    final double epsilon = 0.01;
    for (int i = 0; i < searchBatchSize; i++) {
      // Since we are searching for vector that is already present in the collection,
      // the first result vector should be itself and the distance (inner product) should be
      // very close to 1 (some precision is lost during the process)
      SearchResult.QueryResult firstQueryResult = queryResultsList.get(i).get(0);
      if (firstQueryResult.getEntityId() != vectorIds.get(i)
          || Math.abs(1 - firstQueryResult.getDistance()) > epsilon) {
        throw new AssertionError("Wrong results!");
      }
    }

    // You can also get result ids and distances separately
    List<List<Long>> resultIds = searchResult.getResultIdsList();
    List<List<Float>> resultDistances = searchResult.getResultDistancesList();

    // You can send search request asynchronously, which returns a ListenableFuture object
    ListenableFuture<SearchResult> searchResponseFuture = client.searchAsync(searchParam);
    // Get search response immediately. Obviously you will want to do more complicated stuff with
    // ListenableFuture
    Futures.getUnchecked(searchResponseFuture);

    // Delete the first 5 entities you just searched
    client.deleteEntityByID(collectionName, vectorIds.subList(0, searchBatchSize));
    client.flush(collectionName);

    // After deleting them, we call getEntityByID and obviously all 5 entities should not be returned.
    Map<Long, Map<String, Object>> entities = client.getEntityByID(collectionName, vectorIds.subList(0, searchBatchSize));
    if (!entities.isEmpty()) {
      throw new AssertionError("Unexpected entity count!");
    }

    // Compact the collection, erase deleted data from disk and rebuild index in background (if
    // the data size after compaction is still larger than indexFileSize). Data was only
    // soft-deleted until you call compact.
    client.compact(CompactParam.create(collectionName).setThreshold(0.2));

    // Drop index for the collection
    client.dropIndex(collectionName, "float_vec");

    // Drop collection
    client.dropCollection(collectionName);
  }
}
