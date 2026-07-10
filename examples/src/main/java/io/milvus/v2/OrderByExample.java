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

package io.milvus.v2;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.aggregation.AggDirection;
import io.milvus.v2.service.vector.request.aggregation.OrderByField;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class OrderByExample {
    private static final String COLLECTION_NAME = "java_sdk_example_order_by_v2";
    private static final String ID_FIELD = "id";
    private static final String PRICE_FIELD = "price";
    private static final String RATING_FIELD = "rating";
    private static final String CATEGORY_FIELD = "category";
    private static final String METADATA_FIELD = "metadata";
    private static final String VECTOR_FIELD = "embeddings";
    private static final String DYNAMIC_VIEWS_FIELD = "dynamic_views";
    private static final int DIM = 8;
    private static final int NUM_ENTITIES = 200;
    private static final Random RNG = new Random(19530L);

    private static List<OrderByField> orderByFields(OrderByField... fields) {
        return Arrays.asList(fields);
    }

    private static void printSearchResults(SearchResp searchResp, String title) {
        System.out.printf("%n============================================================%n%s%n============================================================%n", title);
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (int i = 0; i < searchResults.size(); i++) {
            System.out.printf("Search result %d:%n", i);
            for (SearchResp.SearchResult result : searchResults.get(i)) {
                System.out.println(result);
            }
        }
    }

    private static void printQueryResults(QueryResp queryResp, String title) {
        System.out.printf("%n============================================================%n%s%n============================================================%n", title);
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            System.out.println(result.getEntity());
        }
    }

    private static float[] randomVector() {
        float[] values = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            values[i] = RNG.nextFloat();
        }
        return values;
    }

    private static void prepareCollection(MilvusClientV2 client) {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(true)
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(PRICE_FIELD)
                .dataType(DataType.Double)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(RATING_FIELD)
                .dataType(DataType.Double)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CATEGORY_FIELD)
                .dataType(DataType.VarChar)
                .maxLength(64)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(METADATA_FIELD)
                .dataType(DataType.JSON)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(Collections.singletonMap("nlist", 128))
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());
        System.out.println("Collection created");
    }

    private static void insertRows(MilvusClientV2 client) {
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        List<String> categories = Arrays.asList("cate1", "cate2", "cate3", "cate4", "cate5");
        for (int i = 0; i < NUM_ENTITIES; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            row.addProperty(PRICE_FIELD, 10.0 + (i % 12));
            row.addProperty(RATING_FIELD, Math.round(RNG.nextDouble() * 50.0) / 10.0);
            row.addProperty(CATEGORY_FIELD, categories.get(i % categories.size()));
            row.add(VECTOR_FIELD, gson.toJsonTree(randomVector()));

            JsonObject metadata = new JsonObject();
            metadata.addProperty("age", 18 + (i % 40));
            metadata.addProperty("score", i % 101);
            metadata.addProperty("popularity", Math.round(RNG.nextDouble() * 100.0) / 10.0);
            JsonArray tags = new JsonArray();
            tags.add(categories.get(i % categories.size()));
            tags.add("tag_" + (i % 10));
            metadata.add("tags", tags);
            row.add(METADATA_FIELD, metadata);

            row.addProperty(DYNAMIC_VIEWS_FIELD, i * 10 + RNG.nextInt(100));
            rows.add(row);
        }

        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());
        System.out.printf("%d rows inserted%n", rows.size());
    }

    private static void waitForVisible(MilvusClientV2 client) {
        QueryResp countResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows persisted%n", (long) countResp.getQueryResults().get(0).getEntity().get("count(*)"));
    }

    private static void searchExamples(MilvusClientV2 client) {
        List<FloatVec> vectors = Collections.singletonList(new FloatVec(randomVector()));

        SearchResp searchByPrice = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .annsField(VECTOR_FIELD)
                .data(new ArrayList<>(vectors))
                .limit(10)
                .outputFields(Arrays.asList(ID_FIELD, PRICE_FIELD, RATING_FIELD, CATEGORY_FIELD))
                .orderByFields(orderByFields(
                        OrderByField.builder().fieldName(PRICE_FIELD).direction(AggDirection.ASC).build()))
                .build());
        printSearchResults(searchByPrice, "Search with order_by_fields price ASC");

        SearchResp searchByPriceAndRating = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .annsField(VECTOR_FIELD)
                .data(new ArrayList<>(vectors))
                .limit(10)
                .outputFields(Arrays.asList(ID_FIELD, PRICE_FIELD, RATING_FIELD, CATEGORY_FIELD, METADATA_FIELD))
                .orderByFields(orderByFields(
                        OrderByField.builder().fieldName(PRICE_FIELD).direction(AggDirection.ASC).build(),
                        OrderByField.builder().fieldName(RATING_FIELD).direction(AggDirection.DESC).build()))
                .build());
        printSearchResults(searchByPriceAndRating, "Search with order_by_fields price ASC, rating DESC");

        SearchResp searchByJsonAndDynamic = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .annsField(VECTOR_FIELD)
                .data(new ArrayList<>(vectors))
                .limit(10)
                .outputFields(Arrays.asList(ID_FIELD, PRICE_FIELD, RATING_FIELD, CATEGORY_FIELD, METADATA_FIELD, DYNAMIC_VIEWS_FIELD))
                .orderByFields(orderByFields(
                        OrderByField.builder().fieldName("metadata[\"age\"]").direction(AggDirection.ASC).build(),
                        OrderByField.builder().fieldName(DYNAMIC_VIEWS_FIELD).direction(AggDirection.DESC).build()))
                .build());
        printSearchResults(searchByJsonAndDynamic, "Search with order_by_fields metadata[\"age\"] ASC, dynamic_views DESC");
    }

    private static void queryExamples(MilvusClientV2 client) {
        QueryResp queryByPrice = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(ID_FIELD + " >= 0")
                .limit(30)
                .outputFields(Arrays.asList(ID_FIELD, PRICE_FIELD, RATING_FIELD, CATEGORY_FIELD))
                .orderByFields(orderByFields(
                        OrderByField.builder().fieldName(PRICE_FIELD).direction(AggDirection.DESC).build()))
                .build());
        printQueryResults(queryByPrice, "Query with order_by_fields price DESC");

        QueryResp queryByPriceAndRating = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(ID_FIELD + " >= 0")
                .limit(30)
                .outputFields(Arrays.asList(ID_FIELD, PRICE_FIELD, RATING_FIELD, CATEGORY_FIELD, METADATA_FIELD))
                .orderByFields(orderByFields(
                        OrderByField.builder().fieldName(PRICE_FIELD).direction(AggDirection.ASC).build(),
                        OrderByField.builder().fieldName(RATING_FIELD).direction(AggDirection.DESC).build()))
                .build());
        printQueryResults(queryByPriceAndRating, "Query with order_by_fields price ASC, rating DESC");

        QueryResp queryByCategoryAndPrice = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(ID_FIELD + " >= 0")
                .limit(30)
                .outputFields(Arrays.asList(ID_FIELD, PRICE_FIELD, RATING_FIELD, CATEGORY_FIELD, METADATA_FIELD, DYNAMIC_VIEWS_FIELD))
                .orderByFields(orderByFields(
                        OrderByField.builder().fieldName(CATEGORY_FIELD).direction(AggDirection.ASC).build(),
                        OrderByField.builder().fieldName(PRICE_FIELD).direction(AggDirection.DESC).build()))
                .build());
        printQueryResults(queryByCategoryAndPrice, "Query with order_by_fields category ASC, price DESC");
    }

    public static void main(String[] args) {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        prepareCollection(client);
        insertRows(client);
        waitForVisible(client);
        searchExamples(client);
        queryExamples(client);

        client.close();
        System.out.println("All order_by examples completed!");
    }
}
