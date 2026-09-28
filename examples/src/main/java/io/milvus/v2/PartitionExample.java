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
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.DropPartitionReq;
import io.milvus.v2.service.partition.request.GetPartitionStatsReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import io.milvus.v2.service.partition.request.LoadPartitionsReq;
import io.milvus.v2.service.partition.request.ReleasePartitionsReq;
import io.milvus.v2.service.partition.response.GetPartitionStatsResp;
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.response.InsertResp;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Demonstrates the partition management APIs on {@link MilvusClientV2}: createPartition,
 * hasPartition, loadPartitions, getPartitionStats, releasePartitions and dropPartition.
 *
 * <p>Prerequisites: a running Milvus at {@code http://localhost:19530} (the default in
 * {@link ConnectConfig} below).</p>
 */
public class PartitionExample {
    private static final String COLLECTION_NAME = "java_sdk_example_partition_v2";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";
    private static final int VECTOR_DIM = 4;
    private static final String PARTITION_NAME = "p1";

    public static void main(String[] args) throws InterruptedException {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        System.out.println("Server version: " + client.getServerVersion());

        try {
            createCollection(client);
            createPartition(client);
            hasPartition(client);
            loadPartitions(client);
            insertIntoPartition(client);
            getPartitionStats(client);
            releasePartitions(client);
            dropPartition(client);
        } finally {
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .build());
            client.close(5);
        }
    }

    private static void createCollection(MilvusClientV2 client) {
        System.out.println("========== Create collection ==========");
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
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .build());

        // A collection must have an index before its partitions can be loaded.
        IndexParam indexParam = IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build();
        client.createIndex(CreateIndexReq.builder()
                .collectionName(COLLECTION_NAME)
                .indexParams(Collections.singletonList(indexParam))
                .timeout(100000L)
                .build());
        System.out.println("Collection '" + COLLECTION_NAME + "' created with index");
    }

    private static void createPartition(MilvusClientV2 client) {
        System.out.println("========== createPartition() ==========");
        client.createPartition(CreatePartitionReq.builder()
                .collectionName(COLLECTION_NAME)
                .partitionName(PARTITION_NAME)
                .build());
        System.out.println("Partition '" + PARTITION_NAME + "' created");
    }

    private static void hasPartition(MilvusClientV2 client) {
        System.out.println("========== hasPartition() ==========");
        Boolean exists = client.hasPartition(HasPartitionReq.builder()
                .collectionName(COLLECTION_NAME)
                .partitionName(PARTITION_NAME)
                .build());
        System.out.println("Partition '" + PARTITION_NAME + "' exists: " + exists);
    }

    private static void loadPartitions(MilvusClientV2 client) {
        System.out.println("========== loadPartitions() ==========");
        client.loadPartitions(LoadPartitionsReq.builder()
                .collectionName(COLLECTION_NAME)
                .partitionNames(Collections.singletonList(PARTITION_NAME))
                .build());
        System.out.println("Partition '" + PARTITION_NAME + "' loaded");
    }

    private static void insertIntoPartition(MilvusClientV2 client) {
        System.out.println("========== insert() into partition ==========");
        Gson gson = new Gson();
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            JsonObject row = new JsonObject();
            row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));
            rows.add(row);
        }
        InsertResp resp = client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .partitionName(PARTITION_NAME)
                .data(rows)
                .build());
        System.out.println("Inserted " + resp.getInsertCnt() + " rows into partition '" + PARTITION_NAME + "'");

        client.flush(FlushReq.builder()
                .collectionNames(Collections.singletonList(COLLECTION_NAME))
                .build());
    }

    private static void getPartitionStats(MilvusClientV2 client) {
        System.out.println("========== getPartitionStats() ==========");
        GetPartitionStatsResp resp = client.getPartitionStats(GetPartitionStatsReq.builder()
                .collectionName(COLLECTION_NAME)
                .partitionName(PARTITION_NAME)
                .build());
        Map<String, String> stats = resp.getStats();
        System.out.println("Partition '" + PARTITION_NAME + "' stats: " + stats);
        if (resp.getNumOfEntities() != null) {
            System.out.println("  numOfEntities: " + resp.getNumOfEntities());
        }
    }

    private static void releasePartitions(MilvusClientV2 client) {
        System.out.println("========== releasePartitions() ==========");
        client.releasePartitions(ReleasePartitionsReq.builder()
                .collectionName(COLLECTION_NAME)
                .partitionNames(Collections.singletonList(PARTITION_NAME))
                .build());
        System.out.println("Partition '" + PARTITION_NAME + "' released");
    }

    private static void dropPartition(MilvusClientV2 client) {
        System.out.println("========== dropPartition() ==========");
        client.dropPartition(DropPartitionReq.builder()
                .collectionName(COLLECTION_NAME)
                .partitionName(PARTITION_NAME)
                .build());
        System.out.println("Partition '" + PARTITION_NAME + "' dropped");
    }
}
