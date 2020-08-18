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
import io.milvus.client.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import org.json.JSONObject;

// This is a simple example demonstrating how to use Milvus Java SDK v0.9.0.
// For detailed API documentation, please refer to
// https://milvus-io.github.io/milvus-sdk-java/javadoc/io/milvus/client/package-summary.html
// You can also find more information on https://milvus.io/docs/overview.md
public class MilvusClientExample {

  // Helper function that generates random vectors
  static List<List<Float>> generateVectors(int vectorCount, int dimension) {
    SplittableRandom splitcollectionRandom = new SplittableRandom();
    List<List<Float>> vectors = new ArrayList<>(vectorCount);
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

  // Helper function that generates default fields list for a collection
  // In this example, we have 3 fields with names "int64", "float" and "float_vec".
  // Their DataType must also be defined.
  static List<Map<String, Object>> generateDefaultFields(int dimension) {
    List<Map<String, Object>> fieldList = new ArrayList<>();
    Map<String, Object> intField = new HashMap<>();
    intField.put("field", "int64");
    intField.put("type", DataType.INT64);

    Map<String, Object> floatField = new HashMap<>();
    floatField.put("field", "float");
    floatField.put("type", DataType.FLOAT);

    Map<String, Object> vecField = new HashMap<>();
    vecField.put("field", "float_vec");
    vecField.put("type", DataType.VECTOR_FLOAT);
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("dim", dimension);
    vecField.put("params", jsonObject.toString());

    fieldList.add(intField);
    fieldList.add(floatField);
    fieldList.add(vecField);
    return fieldList;
  }

  // Helper function that generates entity field values for inserting into a collection
  // This corresponds to the above function that initializes fields.
  static List<Map<String, Object>> generateDefaultFieldValues(int vectorCount, List<List<Float>> vectors) {
    List<Map<String, Object>> fieldList = new ArrayList<>();
    Map<String, Object> intField = new HashMap<>();
    intField.put("field", "int64");
    intField.put("type", DataType.INT64);

    Map<String, Object> floatField = new HashMap<>();
    floatField.put("field", "float");
    floatField.put("type", DataType.FLOAT);

    Map<String, Object> vecField = new HashMap<>();
    vecField.put("field", "float_vec");
    vecField.put("type", DataType.VECTOR_FLOAT);

    List<Long> intValues = new ArrayList<>(vectorCount);
    List<Float> floatValues = new ArrayList<>(vectorCount);
    for (int i = 0; i < vectorCount; i++) {
      intValues.add((long) i);
      floatValues.add((float) i);
    }
    intField.put("values", intValues);
    floatField.put("values", floatValues);
    vecField.put("values", vectors);

    fieldList.add(intField);
    fieldList.add(floatField);
    fieldList.add(vecField);
    return fieldList;
  }

  public static void main(String[] args) throws InterruptedException, ConnectFailedException {

    // You may need to change the following to the host and port of your Milvus server
    String host = "localhost";
    int port = 19530;
    if (args.length >= 2) {
      host = args[0];
      port = Integer.parseInt(args[1]);
    }

    // Create Milvus client
    MilvusClient client = new MilvusGrpcClient();

    // Connect to Milvus server
    ConnectParam connectParam = new ConnectParam.Builder().withHost(host).withPort(port).build();
    try {
      Response connectResponse = client.connect(connectParam);
    } catch (ConnectFailedException e) {
      System.out.println("Failed to connect to Milvus server: " + e.toString());
      throw e;
    }

    // Create a collection with the following collection mapping
    final String collectionName = "example1"; // collection name
    final int dimension = 128; // dimension of each vector
    // we choose IP (Inner Product) as our metric type
    CollectionMapping collectionMapping =
        new CollectionMapping.Builder(collectionName)
            .withFields(generateDefaultFields(dimension))
            .withParamsInJson("{\"segment_row_count\": 50000, \"auto_id\": true}")
            .build();
    Response createCollectionResponse = client.createCollection(collectionMapping);

    // Check whether the collection exists
    HasCollectionResponse hasCollectionResponse = client.hasCollection(collectionName);

    // Get collection info
    GetCollectionInfoResponse getCollectionInfoResponse = client.getCollectionInfo(collectionName);

    // Insert randomly generated field values to collection
    final int vectorCount = 100000;
    List<List<Float>> vectors = generateVectors(vectorCount, dimension);
    vectors =
        vectors.stream().map(MilvusClientExample::normalizeVector).collect(Collectors.toList());
    List<Map<String, Object>> defaultFieldValues = generateDefaultFieldValues(vectorCount, vectors);
    InsertParam insertParam =
        new InsertParam.Builder(collectionName)
            .withFields(defaultFieldValues)
            .build();
    InsertResponse insertResponse = client.insert(insertParam);
    // Insert returns a list of entity ids that you will be using (if you did not supply them
    // yourself) to reference the entities you just inserted
    List<Long> vectorIds = insertResponse.getEntityIds();

    // Flush data in collection
    Response flushResponse = client.flush(collectionName);

    // Get current entity count of collection
    CountEntitiesResponse countEntitiesResponse = client.countEntities(collectionName);

    // Create index for the collection
    // We choose IVF_SQ8 as our index type here. Refer to Milvus documentation for a
    // complete explanation of different index types and their relative parameters.
    Index index =
        new Index.Builder(collectionName, "float_vec")
            .withParamsInJson("{\"index_type\": \"IVF_SQ8\", \"metric_type\": \"L2\", "
                + "\"params\": {\"nlist\": 2048}}")
            .build();
    Response createIndexResponse = client.createIndex(index);

    // Get collection info
    Response getCollectionStatsResponse = client.getCollectionStats(collectionName);
    if (getCollectionStatsResponse.ok()) {
      // Collection info is sent back with JSON type string
      String jsonString = getCollectionStatsResponse.getMessage();
      System.out.format("Collection Stats: %s\n", jsonString);
    }

    // Check whether a partition exists in collection
    // Obviously we do not have partition "tag" now
    HasPartitionResponse testHasPartition = client.hasPartition(collectionName, "tag");
    if (testHasPartition.ok() && testHasPartition.hasPartition()) {
      throw new AssertionError("Wrong results!");
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
    SearchParam searchParam =
        new SearchParam.Builder(collectionName)
            .withDSL(dsl)
            .withParamsInJson("{\"fields\": [\"int64\", \"float\"]}")
            .build();
    SearchResponse searchResponse = client.search(searchParam);
    if (searchResponse.ok()) {
      List<List<SearchResponse.QueryResult>> queryResultsList =
          searchResponse.getQueryResultsList();
      final double epsilon = 0.01;
      for (int i = 0; i < searchBatchSize; i++) {
        // Since we are searching for vector that is already present in the collection,
        // the first result vector should be itself and the distance (inner product) should be
        // very close to 1 (some precision is lost during the process)
        SearchResponse.QueryResult firstQueryResult = queryResultsList.get(i).get(0);
        if (firstQueryResult.getEntityId() != vectorIds.get(i)
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

    // Delete the first 5 entities you just searched
    Response deleteByIdsResponse =
        client.deleteEntityByID(collectionName, vectorIds.subList(0, searchBatchSize));
    flushResponse = client.flush(collectionName);

    // After deleting them, we call getEntityByID and obviously all 5 entities should not be returned.
    GetEntityByIDResponse getEntityByIDResponse =
        client.getEntityByID(collectionName, vectorIds.subList(0, searchBatchSize));
    if (getEntityByIDResponse.getValidIds().size() > 0) {
      throw new AssertionError("This can never happen!");
    }

    // Compact the collection, erase deleted data from disk and rebuild index in background (if
    // the data size after compaction is still larger than indexFileSize). Data was only
    // soft-deleted until you call compact.
    Response compactResponse = client.compact(
        new CompactParam.Builder(collectionName).withThreshold(0.2).build());

    // Drop index for the collection
    Response dropIndexResponse = client.dropIndex(index);

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
