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

package io.milvus.tutorial.dql;

import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.FunctionScore;
import io.milvus.v2.service.vector.request.HybridSearchReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchIteratorReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.data.SparseFloatVec;
import io.milvus.v2.service.vector.request.ranker.WeightedRanker;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.service.vector.response.QueryResp.QueryResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Tutorial 6: Data query language (DQL).
 *
 * <p>Demonstrates reading data with {@code MilvusClientV2}: scalar filtering with {@code query},
 * dense nearest-neighbor search with {@code search}, combined dense + sparse retrieval with
 * {@code hybridSearch}, and paged reads with {@code queryIterator} and {@code searchIterator}.
 *
 * <p>Connection defaults to {@code MILVUS_URI=http://localhost:19530} and
 * {@code MILVUS_TOKEN=root:Milvus}. Override them with environment variables when needed.
 */
public class App {
    private static final int DIMENSION = 4;

    public static void main(String[] args) {
        String uri = System.getenv().getOrDefault("MILVUS_URI", "http://localhost:19530");
        String token = System.getenv().getOrDefault("MILVUS_TOKEN", "root:Milvus");

        ConnectConfig config = ConnectConfig.builder()
                .uri(uri)
                .token(token)
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        try {
            String collectionName = "tutorial_dql";

            // Pre-drop any collection left behind by an interrupted run; dropping a non-existent
            // collection succeeds, so this keeps the tutorial rerunnable.
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());

            createLoadAndInsert(client, collectionName);
            runQuery(client, collectionName);
            runSearch(client, collectionName);
            runHybridSearch(client, collectionName);
            runQueryIterator(client, collectionName);
            runSearchIterator(client, collectionName);

            // Clean up.
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
            System.out.printf("Collection '%s' dropped%n", collectionName);
        } finally {
            client.close();
        }
    }

    private static void createLoadAndInsert(MilvusClientV2 client, String collectionName) {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("title")
                .dataType(DataType.VarChar)
                .maxLength(256)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("category")
                .dataType(DataType.Int32)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("dense")
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("sparse")
                .dataType(DataType.SparseFloatVector)
                .build());

        // createCollection creates the query target and both indexes. Each IndexParam selects its
        // vector field, index implementation, and similarity metric.
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .indexParams(Arrays.asList(
                        IndexParam.builder()
                                .fieldName("dense")
                                .indexType(IndexParam.IndexType.AUTOINDEX)
                                .metricType(IndexParam.MetricType.COSINE)
                                .build(),
                        IndexParam.builder()
                                .fieldName("sparse")
                                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                                .metricType(IndexParam.MetricType.IP)
                                .build()))
                .build());
        System.out.printf("Collection '%s' created with dense and sparse indexes%n", collectionName);

        // loadCollection prepares the collection for DQL.
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());
        System.out.println("Collection loaded");

        // insert seeds the tutorial data with JSON entities matching the declared schema.
        Gson gson = new Gson();
        List<JsonObject> rows = new ArrayList<>();
        for (int id = 0; id < 12; id++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", id);
            row.addProperty("title", "document_" + id);
            row.addProperty("category", id % 3);
            row.add("dense", gson.toJsonTree(denseVector(id)));
            row.add("sparse", gson.toJsonTree(sparseVector(id)));
            rows.add(row);
        }
        int inserted = (int) client.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(rows)
                .build()).getInsertCnt();
        System.out.printf("Inserted %d tutorial rows%n", inserted);
    }

    private static void runQuery(MilvusClientV2 client, String collectionName) {
        System.out.println("\nquery: category == 1");
        // query performs scalar filtering. Strong consistency includes the recent insert.
        QueryResp resp = client.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter("category == 1")
                .outputFields(Arrays.asList("id", "title", "category"))
                .limit(5)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.println("Query results:");
        for (QueryResult result : resp.getQueryResults()) {
            Map<String, Object> entity = result.getEntity();
            System.out.printf("  id=%s title=%s category=%s%n",
                    entity.get("id"), entity.get("title"), entity.get("category"));
        }
    }

    private static void runSearch(MilvusClientV2 client, String collectionName) {
        System.out.println("\nsearch: nearest neighbors in the dense field");
        // search performs nearest-neighbor search on the dense vector field.
        SearchResp resp = client.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField("dense")
                .data(Arrays.asList(new FloatVec(denseVector(2))))
                .filter("category >= 0")
                .outputFields(Arrays.asList("title", "category"))
                .limit(3)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.println("Search results:");
        for (List<SearchResp.SearchResult> results : resp.getSearchResults()) {
            for (SearchResp.SearchResult result : results) {
                System.out.printf("  id=%s score=%f title=%s%n",
                        result.getId(), result.getScore(),
                        result.getEntity().get("title"));
            }
        }
    }

    private static void runHybridSearch(MilvusClientV2 client, String collectionName) {
        System.out.println("\nhybrid_search: combine dense and sparse similarities");
        // hybridSearch combines the dense and sparse sub-searches. weights controls their
        // relative contribution and limit caps the reranked matches.
        AnnSearchReq dense = AnnSearchReq.builder()
                .vectorFieldName("dense")
                .vectors(Arrays.asList(new FloatVec(denseVector(2))))
                .limit(6)
                .build();
        AnnSearchReq sparse = AnnSearchReq.builder()
                .vectorFieldName("sparse")
                .vectors(Arrays.asList(new SparseFloatVec(sparseVector(2))))
                .limit(6)
                .build();
        SearchResp resp = client.hybridSearch(HybridSearchReq.builder()
                .collectionName(collectionName)
                .searchRequests(Arrays.asList(dense, sparse))
                .functionScore(FunctionScore.builder()
                        .addFunction(WeightedRanker.builder().weights(Arrays.asList(0.7f, 0.3f)).build())
                        .build())
                .outFields(Arrays.asList("title", "category"))
                .limit(4)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.println("Hybrid search results:");
        for (List<SearchResp.SearchResult> results : resp.getSearchResults()) {
            for (SearchResp.SearchResult result : results) {
                System.out.printf("  id=%s score=%f title=%s%n",
                        result.getId(), result.getScore(),
                        result.getEntity().get("title"));
            }
        }
    }

    private static void runQueryIterator(MilvusClientV2 client, String collectionName) {
        System.out.println("\nquery_iterator: batch_size=3, limit=7");
        // queryIterator paginates a query over the primary key. batch_size controls rows per page
        // and limit controls the total rows returned across all pages.
        QueryIterator iterator = client.queryIterator(QueryIteratorReq.builder()
                .collectionName(collectionName)
                .expr("id >= 0")
                .outputFields(Arrays.asList("id", "title", "category"))
                .batchSize(3)
                .limit(7)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        int count = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> page = iterator.next();
            if (page.isEmpty()) {
                iterator.close();
                break;
            }
            for (QueryResultsWrapper.RowRecord record : page) {
                Map<String, Object> values = record.getFieldValues();
                System.out.printf("  id=%s title=%s category=%s%n",
                        values.get("id"), values.get("title"), values.get("category"));
                count++;
            }
        }
        System.out.printf("%d query rows returned%n", count);
    }

    private static void runSearchIterator(MilvusClientV2 client, String collectionName) {
        System.out.println("\nsearch_iterator: batch_size=3, limit=7");
        // searchIterator paginates a search while preserving one search session.
        SearchIterator iterator = client.searchIterator(SearchIteratorReq.builder()
                .collectionName(collectionName)
                .vectorFieldName("dense")
                .metricType(IndexParam.MetricType.COSINE)
                .vectors(Arrays.asList(new FloatVec(denseVector(2))))
                .outputFields(Arrays.asList("title", "category"))
                .batchSize(3)
                .limit(7)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        int count = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> page = iterator.next();
            if (page.isEmpty()) {
                iterator.close();
                break;
            }
            for (QueryResultsWrapper.RowRecord record : page) {
                Map<String, Object> values = record.getFieldValues();
                System.out.printf("  id=%s title=%s category=%s%n",
                        values.get("id"), values.get("title"), values.get("category"));
                count++;
            }
        }
        System.out.printf("%d search rows returned%n", count);
    }

    private static float[] denseVector(int id) {
        float base = id / 10.0f;
        return new float[]{base, base + 0.1f, base + 0.2f, base + 0.3f};
    }

    private static SortedMap<Long, Float> sparseVector(int id) {
        SortedMap<Long, Float> vector = new TreeMap<>();
        vector.put((long) (id % 8), 1.0f);
        vector.put((long) ((id + 3) % 8), 0.5f);
        return vector;
    }
}
