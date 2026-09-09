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

package io.milvus.tutorial.index;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tutorial 4: Index management.
 *
 * <p>Creates a collection without indexes, then demonstrates the index lifecycle: build one vector
 * index and two scalar indexes with {@code createIndex}, list them, inspect one with
 * {@code describeIndex}, and remove one with {@code dropIndex}.
 *
 * <p>Connection defaults to {@code MILVUS_URI=http://localhost:19530} and
 * {@code MILVUS_TOKEN=root:Milvus}. Override them with environment variables when needed.
 */
public class App {
    private static final String CATEGORY_INDEX = "category_inverted_idx";
    private static final String PRICE_INDEX = "price_sort_idx";
    private static final String VECTOR_INDEX = "embedding_hnsw_idx";

    public static void main(String[] args) {
        String uri = System.getenv().getOrDefault("MILVUS_URI", "http://localhost:19530");
        String token = System.getenv().getOrDefault("MILVUS_TOKEN", "root:Milvus");

        ConnectConfig config = ConnectConfig.builder()
                .uri(uri)
                .token(token)
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        try {
            String collectionName = "tutorial_index";

            // Pre-drop any collection left behind by an interrupted run; dropping a non-existent
            // collection succeeds, so this keeps the tutorial rerunnable.
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());

            // createCollection creates the fields that the indexes will target. No index is
            // supplied here because this tutorial demonstrates createIndex separately.
            CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
            schema.addField(AddFieldReq.builder()
                    .fieldName("id")
                    .dataType(DataType.Int64)
                    .isPrimaryKey(true)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName("category")
                    .dataType(DataType.VarChar)
                    .maxLength(128)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName("price")
                    .dataType(DataType.Float)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName("embedding")
                    .dataType(DataType.FloatVector)
                    .dimension(8)
                    .build());
            client.createCollection(CreateCollectionReq.builder()
                    .collectionName(collectionName)
                    .collectionSchema(schema)
                    .build());
            System.out.printf("Collection '%s' created without indexes%n", collectionName);

            // createIndex builds all supplied index definitions. Each IndexParam identifies a
            // field, index name/type, and any metric or build parameters.
            Map<String, Object> hnswParams = new HashMap<>();
            hnswParams.put("M", "16");
            hnswParams.put("efConstruction", "100");
            client.createIndex(CreateIndexReq.builder()
                    .collectionName(collectionName)
                    .indexParams(Arrays.asList(
                            IndexParam.builder()
                                    .fieldName("embedding")
                                    .indexName(VECTOR_INDEX)
                                    .indexType(IndexParam.IndexType.HNSW)
                                    .metricType(IndexParam.MetricType.COSINE)
                                    .extraParams(hnswParams)
                                    .build(),
                            IndexParam.builder()
                                    .fieldName("category")
                                    .indexName(CATEGORY_INDEX)
                                    .indexType(IndexParam.IndexType.INVERTED)
                                    .build(),
                            IndexParam.builder()
                                    .fieldName("price")
                                    .indexName(PRICE_INDEX)
                                    .indexType(IndexParam.IndexType.STL_SORT)
                                    .build()))
                    .build());
            System.out.println("createIndex completed");

            printIndexes(client, collectionName, "Created indexes");

            // describeIndex returns detailed metadata for the selected field and index name.
            DescribeIndexResp descResp = client.describeIndex(DescribeIndexReq.builder()
                    .collectionName(collectionName)
                    .fieldName("embedding")
                    .indexName(VECTOR_INDEX)
                    .build());
            for (DescribeIndexResp.IndexDesc index : descResp.getIndexDescriptions()) {
                System.out.printf("Vector index detail: name=%s, type=%s, metric=%s, state=%s%n",
                        index.getIndexName(), index.getIndexType(), index.getMetricType(),
                        index.getIndexState());
            }

            // dropIndex removes only the named index; it does not delete its field or collection.
            client.dropIndex(DropIndexReq.builder()
                    .collectionName(collectionName)
                    .indexName(PRICE_INDEX)
                    .build());
            System.out.println("Dropped index " + PRICE_INDEX);

            printIndexes(client, collectionName, "Indexes after dropping price index");

            // Clean up.
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
            System.out.printf("Collection '%s' dropped%n", collectionName);
        } finally {
            client.close();
        }
    }

    private static void printIndexes(MilvusClientV2 client, String collectionName, String heading) {
        // listIndexes returns every index name defined on the selected collection.
        List<String> indexNames = client.listIndexes(ListIndexesReq.builder()
                .collectionName(collectionName)
                .build());
        System.out.println(heading + ": " + indexNames);
    }
}
