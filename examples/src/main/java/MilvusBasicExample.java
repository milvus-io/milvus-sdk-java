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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SplittableRandom;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import org.json.JSONObject;

/** This is a simple example demonstrating how to use Milvus Java SDK v0.9.0.
 * For detailed API documentation, please refer to
 * https://milvus-io.github.io/milvus-sdk-java/javadoc/io/milvus/client/package-summary.html
 * You can also find more information on https://milvus.io/docs/overview.md
 */
public class MilvusBasicExample {

  // Helper function that generates random float vectors
  private static List<List<Float>> randomFloatVectors(int vectorCount, int dimension) {
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

  public static void main(String[] args) {
    try {
      run();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void run() {
    /*
     * Setup:
     *   First of all, you need a running Milvus server (v0.11.0). By default, Milvus runs on
     *   localhost in port 19530. There are various configurations that you can set for
     *   ConnectParam. Refer to JavaDoc for more information.
     *
     *   You can use `withLogging()` for `client` to enable logging framework.
     */
    ConnectParam connectParam = new ConnectParam.Builder().withHost("127.0.0.1").withPort(19530).build();
    MilvusClient client = new MilvusGrpcClient(connectParam);

    /*
     * Basic create collection:
     *   You now have a Milvus instance running and client connected to the server.
     *   The first thing we will do is to create a collection `demo_films`. If we already had a
     *   collection with the same name, we need to drop it before creating again.
     */
    final String collectionName = "demo_films";
    if (client.listCollections().contains(collectionName)) {
      client.dropCollection(collectionName);
    }

    /*
     * Basic create collection:
     *   We will create a collection with three fields: film duration, release_year and an
     *   embedding which is essentially a float vector.
     *
     *   CollectionMapping will be used to create a collection. When adding vector fields, the
     *   dimension must be specified. `auto_id` is set to false so we can provide custom ids.
     */
    final int dimension = 8;
    CollectionMapping collectionMapping = CollectionMapping
        .create(collectionName)
        .addField("duration", DataType.INT32)
        .addField("release_year", DataType.INT64)
        .addVectorField("embedding", DataType.VECTOR_FLOAT, dimension)
        .setParamsInJson("{\"segment_row_limit\": 4096, \"auto_id\": false}");

    client.createCollection(collectionMapping);
    // Check the existence of collection
    if (!client.hasCollection(collectionName)) {
      throw new AssertionError("Collection not found!");
    }

    /*
     * Basic create partition:
     *   We can create partitions in a collection. Here we create a partition called "American"
     *   since the films we insert will be American.
     */
    final String partitionTag = "American";
    client.createPartition(collectionName, partitionTag);
    // Check the existence of partition
    if (!client.hasPartition(collectionName, partitionTag)) {
      throw new AssertionError("Partition not found!");
    }

    // You can now get information about the collection and partition created.
    System.out.println("\n--------Get Collection Info--------");
    CollectionMapping collectionInfo = client.getCollectionInfo(collectionName);
    System.out.println(collectionInfo.toString());
    System.out.println("\n--------Get Partition List--------");
    List<String> partitions = client.listPartitions(collectionName);
    System.out.println(partitions);

    /*
     * Basic insert:
     *   We will insert three films of The_Lord_of_the_Rings series with their id, duration,
     *   release year and fake embeddings. When inserting entities into Milvus, values from
     *   the same field should be grouped together to create InsertParam. We also wish to
     *   insert them into the partition "American".
     *
     *   The titles and relative film properties are listed below for your reference.
     */
    List<Long> ids = new ArrayList<>(Arrays.asList(1L, 2L, 3L));
    List<String> titles = Arrays.asList("The_Fellowship_of_the_Ring", "The_Two_Towers", "The_Return_of_the_King");
    List<Integer> durations = new ArrayList<>(Arrays.asList(208, 226, 252));
    List<Long> releaseYears = new ArrayList<>(Arrays.asList(2001L, 2002L, 2003L));
    List<List<Float>> embeddings = randomFloatVectors(3, dimension);

    InsertParam insertParam = InsertParam
        .create(collectionName)
        .addField("duration", DataType.INT32, durations)
        .addField("release_year", DataType.INT64, releaseYears)
        .addVectorField("embedding", DataType.VECTOR_FLOAT, embeddings)
        .setEntityIds(ids)
        .setPartitionTag(partitionTag);

    System.out.println("\n--------Insert Entities--------");
    List<Long> entityIds = client.insert(insertParam);
    System.out.println(entityIds);

    /*
     * Basic insert:
     *   After inserting entities into the collection, we need to perform flush to make sure the
     *   data is on disk. Then we are able to retrieve it.
     */
    long beforeEntityCount = client.countEntities(collectionName);
    client.flush(collectionName);
    long afterEntityCount = client.countEntities(collectionName);
    System.out.println("\n--------Flush Collection--------");
    System.out.printf("There are %d films in the collection before flush.\n", beforeEntityCount);
    System.out.printf("There are %d films in the collection after flush.\n", afterEntityCount);

    // We can get the detail of collection statistics.
    System.out.println("\n--------Collection Stats--------");
    JSONObject json = new JSONObject(client.getCollectionStats(collectionName));
    System.out.println(json.toString(4));

    /*
     * Basic search entities:
     *   Now that we have 3 films inserted into our collection, it's time to obtain them.
     *   We can get films by ids, and invalid/non-existent ids will be ignored.
     *   In the case below, we will only films with ids 1 and 2.
     */
    List<Long> queryIds = new ArrayList<>(Arrays.asList(1L, 2L, 10L, 2333L));
    Map<Long, Map<String, Object>> entities = client.getEntityByID(collectionName, queryIds);
    System.out.println("\n--------Get Entity By ID--------");
    for (Entry<Long, Map<String, Object>> entry : entities.entrySet()) {
      Long id = entry.getKey();
      Map<String, Object> val = entry.getValue();
      System.out.printf(
          " > id: %d,\n > duration: %smin,\n > release_year: %s,\n > embedding: %s\n\n",
          id, val.get("duration"), val.get("release_year"), val.get("embedding").toString());
    }

    /*
     * Basic hybrid search:
     *   Getting films by id is not enough, we are going to get films based on vector similarities.
     *   Let's say we have a film with its `embedding` and we want to find `top1` film that is
     *   most similar to it by L2 metric_type (Euclidean Distance).
     *
     *   In addition to vector similarities, we also want to filter films such that:
     *     - `released year` is 2002 or 2003,
     *     - `duration` larger than 250 minutes.
     *
     *   There will be only one film that satisfies our filters, namely "The_Return_of_the_King".
     *
     *   Milvus provides Query DSL(Domain Specific Language) to support structured data filtering
     *   in queries. This includes `term`, `range` and `vector` queries. For more information about
     *   DSL statements, please refer to Milvus documentation for more details.
     */
    List<List<Float>> queryEmbedding = randomFloatVectors(1, dimension);
    final long topK = 1;
    String dsl = String.format(
        "{\"bool\": {"
            + "\"must\": [{"
            + "    \"range\": {"
            + "        \"duration\": {\"GT\": 250}" // "GT" for greater than
            + "    }},{"
            + "    \"term\": {"
            + "        \"release_year\": %s" // "term" is a list
            + "    }},{"
            + "    \"vector\": {"
            + "        \"embedding\": {"
            + "            \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"float\", \"query\": %s"
            + "    }}}]}}",
        releaseYears.subList(1, 3).toString(), topK, queryEmbedding.toString());

    // Only specified fields in `setParamsInJson` will be returned from search request.
    // If not set, all fields will be returned.
    SearchParam searchParam = SearchParam
        .create(collectionName)
        .setDsl(dsl)
        .setParamsInJson("{\"fields\": [\"duration\", \"release_year\", \"embedding\"]}");
    System.out.println("\n--------Search Result--------");
    SearchResult searchResult = client.search(searchParam);
    System.out.println("- ids: " + searchResult.getResultIdsList().toString());
    System.out.println("- distances: " + searchResult.getResultDistancesList().toString());
    for (List<Map<String, Object>> singleQueryResult : searchResult.getFieldsMap()) {
      // We only have 1 film returned
      for (Map<String, Object> res : singleQueryResult) {
        System.out.println("- release_year: " + res.get("release_year"));
        System.out.println("- duration: " + res.get("duration"));
        System.out.println("- embedding: " + res.get("embedding"));
      }
    }

    /*
     * Basic hybrid search:
     *   You can send search request asynchronously, which returns a ListenableFuture object.
     */
    ListenableFuture<SearchResult> searchResponseFuture = client.searchAsync(searchParam);
    Futures.getUnchecked(searchResponseFuture);

    /*
     * Basic delete:
     *   Now let's see how to delete entities in Milvus.
     *   You can simply delete entities by their ids. Here we delete the first two films.
     *   After deleting, it is obvious that `getEntityByID` should return an empty map.
     */
    client.deleteEntityByID(collectionName, ids.subList(0, 2));
    client.flush(collectionName);
    entities = client.getEntityByID(collectionName, queryIds);
    if (!entities.isEmpty()) {
      throw new AssertionError("Unexpected entity count!");
    }
    System.out.println("\n--------Delete Entities--------");
    long entityCount = client.countEntities(collectionName);
    System.out.println(entityCount + " entity remains after delete.");

    /*
     * Other operations:
     *   There are some other operations in Milvus, such as `compact` and `listIDInSegment`.
     *
     *   Compacting the collection will erase deleted data from disk and rebuild index in background.
     *   Data were only soft-deleted until you call compact.
     *
     *   `listIDInSegment` will simply list all ids in a segment given the `segmentId`.
     */
    client.compact(CompactParam.create(collectionName).setThreshold(0.2));

    /*
     * Basic delete:
     *   You can drop the partitions and finally the whole collection after use.
     */
    client.dropPartition(collectionName, partitionTag);
    if (client.listCollections().contains(collectionName)) {
      client.dropCollection(collectionName);
    }

    // Close connection
    client.close();
  }
}
