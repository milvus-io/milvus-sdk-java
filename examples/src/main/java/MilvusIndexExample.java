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
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import org.json.JSONObject;

/** This is an example of Milvus Java SDK v0.9.1. In particular, we demonstrate how we can build
 * and search by index in Milvus.
 *
 * We will be using `films.csv` as our dataset. There are 4 columns in the file, namely
 * `id`, `title`, `release_year` and `embedding`. The dataset comes from MovieLens `ml-latest-small`,
 * with id and embedding being fake values.
 *
 * We assume that you have walked through `MilvusBasicExample.java` and understand basic operations
 * in Milvus. For detailed API documentation, please refer to
 * https://milvus-io.github.io/milvus-sdk-java/javadoc/io/milvus/client/package-summary.html
 */
public class MilvusIndexExample {

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

  public static void run() throws IOException {

    // Connect to Milvus
    ConnectParam connectParam = new ConnectParam.Builder().build();
    MilvusClient client = new MilvusGrpcClient(connectParam);

    final String collectionName = "demo_index";
    if (client.listCollections().contains(collectionName)) {
      client.dropCollection(collectionName);
    }

    // Create collection
    final int dimension = 8;
    CollectionMapping collectionMapping = CollectionMapping
        .create(collectionName)
        .addField("release_year", DataType.INT64)
        .addVectorField("embedding", DataType.VECTOR_FLOAT, dimension)
        .setParamsInJson("{\"segment_row_limit\": 4096, \"auto_id\": false}");

    client.createCollection(collectionMapping);

    /*
     * Basic insert and create index:
     *   Now that we have a collection in Milvus, we can create an index for `embedding`.
     *
     *   We can create index BEFORE or AFTER we insert any entities. However, Milvus will not
     *   actually start to build the index until the number of rows you insert reaches `segment_row_limit`.
     *
     *   We will read data from `films.csv` and again, group data with the same field together.
     */
    String path = System.getProperty("user.dir") + "/src/main/java/films.csv";
    BufferedReader csvReader = new BufferedReader(new FileReader(path));
    List<Long> ids = new ArrayList<>();
    List<String> titles = new ArrayList<>();
    List<Long> releaseYears = new ArrayList<>();
    List<List<Float>> embeddings = new ArrayList<>();
    String row;
    while ((row = csvReader.readLine()) != null) {
      String[] data = row.split(",");
      // process four columns in order
      ids.add(Long.parseLong(data[0]));
      titles.add(data[1]);
      releaseYears.add(Long.parseLong(data[2]));
      List<Float> embedding = new ArrayList<>(dimension);
      for (int i = 3; i < 11; i++) {
        // 8 float values in a vector
        if (i == 3) {
          embedding.add(Float.parseFloat(data[i].substring(2)));
        } else if (i == 10) {
          embedding.add(Float.parseFloat(data[i].substring(1, data[i].length() - 2)));
        } else {
          embedding.add(Float.parseFloat(data[i].substring(1)));
        }
      }
      embeddings.add(embedding);
    }
    csvReader.close();

    // Now we can insert entities, the total row count should be 8657.
    InsertParam insertParam = InsertParam
        .create(collectionName)
        .addField("release_year", DataType.INT64, releaseYears)
        .addVectorField("embedding", DataType.VECTOR_FLOAT, embeddings)
        .setEntityIds(ids);

    client.insert(insertParam);
    client.flush(collectionName);
    System.out.printf("There are %d films in the collection.\n", client.countEntities(collectionName));

    /*
     * Basic create index:
     *   When building index, we need to indicate which field to build index for, the `index_type`,
     *   `metric_type` and params for the specific index type. Here we create an `IVF_FLAT` index
     *   with param `nlist`. Refer to Milvus documentation for more information on choosing
     *   parameters when creating index.
     *
     *   Note that if there is already an index and create index is called again, the previous index
     *   will be replaced.
     */
    Index index = Index
        .create(collectionName, "embedding")
        .setIndexType(IndexType.IVF_FLAT)
        .setMetricType(MetricType.L2)
        .setParamsInJson(new JsonBuilder().param("nlist", 100).build());

    client.createIndex(index);

    // Get collection stats with index
    System.out.println("\n--------Collection Stats--------");
    JSONObject json = new JSONObject(client.getCollectionStats(collectionName));
    System.out.println(json.toString(4));

    /*
     * Basic search entities:
     *   In order to search with index, specific search parameters need to be provided. For `IVF_FLAT`,
     *   thr param is `nprobe`.
     *
     *   Based on the index you created, the available search parameters will be different. Refer to
     *   Milvus documentation for how to set the optimal parameters based on your needs.
     */
    List<List<Float>> queryEmbedding = randomFloatVectors(1, dimension);
    final long topK = 3;
    String dsl = String.format(
        "{\"bool\": {"
            + "\"must\": [{"
            + "    \"term\": {"
            + "        \"release_year\": [2002, 1995]"
            + "    }},{"
            + "    \"vector\": {"
            + "        \"embedding\": {"
            + "            \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"float\", \"query\": "
            + "%s, \"params\": {\"nprobe\": 8}"
            + "    }}}]}}",
        topK, queryEmbedding.toString());

    SearchParam searchParam = SearchParam
        .create(collectionName)
        .setDsl(dsl)
        .setParamsInJson("{\"fields\": [\"release_year\", \"embedding\"]}");
    System.out.println("\n--------Search Result--------");
    SearchResult searchResult = client.search(searchParam);
    System.out.println("- ids: " + searchResult.getResultIdsList().toString());
    System.out.println("- distances: " + searchResult.getResultDistancesList().toString());
    for (List<Map<String, Object>> singleQueryResult : searchResult.getFieldsMap()) {
      for (int i = 0; i < singleQueryResult.size(); i++) {
        Map<String, Object> res = singleQueryResult.get(i);
        System.out.println("==");
        System.out.println("- title: " + titles.get(
            Math.toIntExact(searchResult.getResultIdsList().get(0).get(i))));
        System.out.println("- release_year: " + res.get("release_year"));
        System.out.println("- embedding: " + res.get("embedding"));
      }
    }

    /*
     * Basic delete index:
     *   Index can be dropped for a vector field.
     */
    client.dropIndex(collectionName, "embedding");

    if (client.listCollections().contains(collectionName)) {
      client.dropCollection(collectionName);
    }

    // Close connection
    client.close();
  }
}