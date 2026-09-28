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

import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.partition.request.ListPartitionsReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Demonstrates a collection whose VARCHAR field is marked as the partition key:
 * entities are auto-hashed into {@code numPartitions} partitions by the partition key value,
 * and query/search filters that reference the partition key are routed to the matching
 * partitions only.
 *
 * <p>Prerequisites: a running Milvus at {@code http://localhost:19530} (the default in
 * {@link ConnectConfig} below).</p>
 */
public class PartitionKeyExample {
    private static final String COLLECTION_NAME = "java_sdk_example_partition_key_v2";
    private static final String ID_FIELD = "id";
    private static final String NAME_FIELD = "name";
    private static final String VECTOR_FIELD = "vector";
    private static final int VECTOR_DIM = 128;
    private static final int NUM_PARTITIONS = 8;

    public static void main(String[] args) throws InterruptedException {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        System.out.println("Server version: " + client.getServerVersion());

        try {
            createCollection(client);
            createIndex(client);
            listPartitions(client);
            insertData(client);
            queryWithPartitionKeyFilter(client);
            searchWithPartitionKeyFilter(client);
        } finally {
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .build());
            client.close(5);
        }
    }

    private static void createCollection(MilvusClientV2 client) {
        System.out.println("========== Create collection with partition key ==========");
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.TRUE)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(NAME_FIELD)
                .dataType(DataType.VarChar)
                .maxLength(100)
                .isPartitionKey(Boolean.TRUE)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .numPartitions(NUM_PARTITIONS)
                .build());
        System.out.printf("Collection '%s' created with %d partitions%n", COLLECTION_NAME, NUM_PARTITIONS);
    }

    private static void createIndex(MilvusClientV2 client) {
        System.out.println("========== createIndex() ==========");
        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("M", 64);
        extraParams.put("efConstruction", 100);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.IP)
                .extraParams(extraParams)
                .build();
        client.createIndex(CreateIndexReq.builder()
                .collectionName(COLLECTION_NAME)
                .indexParams(Collections.singletonList(indexParam))
                .timeout(100000L)
                .build());
        System.out.println("Index created on field '" + VECTOR_FIELD + "'");
    }

    private static void listPartitions(MilvusClientV2 client) {
        System.out.println("========== listPartitions() ==========");
        List<String> partitions = client.listPartitions(ListPartitionsReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println("Partitions of '" + COLLECTION_NAME + "': " + partitions);
    }

    private static void insertData(MilvusClientV2 client) {
        System.out.println("========== insert() ==========");
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        Gson gson = new Gson();
        // 10 batches x 1000 rows; the "name" value is hashed into one of the partitions.
        long totalInserted = 0;
        for (int i = 0; i < 10; i++) {
            List<JsonObject> rows = new ArrayList<>();
            for (int j = 0; j < 1000; j++) {
                JsonObject row = new JsonObject();
                row.addProperty(NAME_FIELD, "name_" + i + "_" + j);
                row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));
                rows.add(row);
            }
            InsertResp resp = client.insert(InsertReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(rows)
                    .build());
            totalInserted += resp.getInsertCnt();
        }
        System.out.println("Inserted " + totalInserted + " rows");
    }

    private static void queryWithPartitionKeyFilter(MilvusClientV2 client) {
        System.out.println("========== query() with partition key filter ==========");
        QueryResp resp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(NAME_FIELD + " == \"name_3_500\"")
                .outputFields(Collections.singletonList(NAME_FIELD))
                .build());
        System.out.println("Query results size: " + resp.getQueryResults().size());
        for (QueryResp.QueryResult result : resp.getQueryResults()) {
            System.out.println("  " + result.getEntity());
        }
    }

    private static void searchWithPartitionKeyFilter(MilvusClientV2 client) {
        System.out.println("========== search() with partition key filter ==========");
        List<BaseVector> queryVectors = new ArrayList<>();
        queryVectors.add(new FloatVec(CommonUtils.generateFloatVector(VECTOR_DIM)));
        SearchResp resp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(queryVectors)
                .filter(NAME_FIELD + " == \"name_3_500\"")
                .outputFields(Collections.singletonList(NAME_FIELD))
                .topK(5)
                .metricType(IndexParam.MetricType.IP)
                .annsField(VECTOR_FIELD)
                .build());
        System.out.println("Search results size: " + resp.getSearchResults().size());
        for (List<SearchResp.SearchResult> results : resp.getSearchResults()) {
            for (SearchResp.SearchResult result : results) {
                System.out.println("  " + result);
            }
        }
    }
}
