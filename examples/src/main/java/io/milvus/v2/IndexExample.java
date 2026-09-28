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

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.index.request.AlterIndexPropertiesReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.DropIndexPropertiesReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Demonstrates every index-related API on {@link MilvusClientV2}:
 * createIndex, listIndexes, describeIndex, alterIndex, alterIndexProperties,
 * dropIndexProperties and dropIndex.
 *
 * <p>Prerequisites: a running Milvus at {@code http://localhost:19530} (the default in
 * {@link ConnectConfig} below). The example creates a small collection with one dense vector
 * field and one scalar field, creates a vector index plus a scalar index, inspects them, then
 * alters and finally drops them before cleaning up the collection.</p>
 */
public class IndexExample {
    private static final String COLLECTION_NAME = "java_sdk_example_index_v2";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";
    private static final String SCALAR_FIELD = "name";
    private static final int VECTOR_DIM = 128;

    private static final String VECTOR_INDEX_NAME = "idx_vector";
    private static final String SCALAR_INDEX_NAME = "idx_scalar";

    public static void main(String[] args) throws InterruptedException {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        System.out.println("Server version: " + client.getServerVersion());

        try {
            createCollection(client);
            createIndexes(client);
            listIndexes(client);
            describeIndexes(client);
            alterIndexProperties(client);
            dropIndexProperties(client);
            dropIndexes(client);
        } finally {
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .build());
            client.close(5);
        }
    }

    /**
     * Drops any leftover collection from a previous run and creates a fresh one with one dense
     * vector field ({@link DataType#FloatVector}) and one scalar field ({@link DataType#VarChar}).
     */
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
        schema.addField(AddFieldReq.builder()
                .fieldName(SCALAR_FIELD)
                .dataType(DataType.VarChar)
                .maxLength(256)
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .build());
        System.out.printf("Collection '%s' created%n", COLLECTION_NAME);
    }

    /**
     * Creates an IVF_FLAT index on the vector field and an INVERTED index on the scalar field in
     * a single call, then waits for the build to finish (sync mode).
     */
    private static void createIndexes(MilvusClientV2 client) {
        System.out.println("========== createIndex() ==========");
        Map<String, Object> vectorExtraParams = new HashMap<>();
        vectorExtraParams.put("nlist", 128);

        IndexParam vectorIndex = IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexName(VECTOR_INDEX_NAME)
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(vectorExtraParams)
                .build();
        IndexParam scalarIndex = IndexParam.builder()
                .fieldName(SCALAR_FIELD)
                .indexName(SCALAR_INDEX_NAME)
                .indexType(IndexParam.IndexType.INVERTED)
                .build();

        client.createIndex(CreateIndexReq.builder()
                .collectionName(COLLECTION_NAME)
                .indexParams(Arrays.asList(vectorIndex, scalarIndex))
                .timeout(100000L)
                .build());
        System.out.println("Indexes created: " + VECTOR_INDEX_NAME + " (vector), " + SCALAR_INDEX_NAME + " (scalar)");
    }

    /**
     * Lists all index names on the collection.
     */
    private static void listIndexes(MilvusClientV2 client) {
        System.out.println("========== listIndexes() ==========");
        List<String> indexNames = client.listIndexes(ListIndexesReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println("Index names: " + indexNames);
    }

    /**
     * Describes every index on the collection by index name and by field name, printing the
     * index type, metric type, build state and the number of indexed/total rows.
     */
    private static void describeIndexes(MilvusClientV2 client) {
        System.out.println("========== describeIndex() ==========");

        DescribeIndexResp vectorDesc = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(COLLECTION_NAME)
                .indexName(VECTOR_INDEX_NAME)
                .build());
        printIndexDesc(vectorDesc);

        DescribeIndexResp scalarDesc = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(COLLECTION_NAME)
                .fieldName(SCALAR_FIELD)
                .build());
        printIndexDesc(scalarDesc);
    }

    /**
     * Alters index properties, e.g. enabling mmap for the vector index. Only server-declared
     * configurable index properties are accepted (such as {@code mmap.enabled}).
     */
    private static void alterIndexProperties(MilvusClientV2 client) {
        System.out.println("========== alterIndexProperties() ==========");
        Map<String, String> properties = new HashMap<>();
        properties.put("mmap.enabled", "true");
        client.alterIndexProperties(AlterIndexPropertiesReq.builder()
                .collectionName(COLLECTION_NAME)
                .indexName(VECTOR_INDEX_NAME)
                .properties(properties)
                .build());
        System.out.println("Altered " + VECTOR_INDEX_NAME + " properties via alterIndexProperties: " + properties);
    }

    /**
     * Drops the property set by {@link #alterIndexProperties(MilvusClientV2)}.
     */
    private static void dropIndexProperties(MilvusClientV2 client) {
        System.out.println("========== dropIndexProperties() ==========");
        client.dropIndexProperties(DropIndexPropertiesReq.builder()
                .collectionName(COLLECTION_NAME)
                .indexName(VECTOR_INDEX_NAME)
                .propertyKeys(Collections.singletonList("mmap.enabled"))
                .build());
        System.out.println("Dropped 'mmap.enabled' property from " + VECTOR_INDEX_NAME);
    }

    /**
     * Drops both the vector index and the scalar index.
     */
    private static void dropIndexes(MilvusClientV2 client) {
        System.out.println("========== dropIndex() ==========");
        client.dropIndex(DropIndexReq.builder()
                .collectionName(COLLECTION_NAME)
                .indexName(VECTOR_INDEX_NAME)
                .build());
        System.out.println("Dropped vector index: " + VECTOR_INDEX_NAME);

        client.dropIndex(DropIndexReq.builder()
                .collectionName(COLLECTION_NAME)
                .indexName(SCALAR_INDEX_NAME)
                .build());
        System.out.println("Dropped scalar index: " + SCALAR_INDEX_NAME);
    }

    private static void printIndexDesc(DescribeIndexResp resp) {
        for (DescribeIndexResp.IndexDesc desc : resp.getIndexDescriptions()) {
            System.out.printf("  Index '%s' on field '%s': type=%s, metric=%s, state=%s, rows=%d/%d%n",
                    desc.getIndexName(),
                    desc.getFieldName(),
                    desc.getIndexType(),
                    desc.getMetricType(),
                    desc.getIndexState(),
                    desc.getIndexedRows(),
                    desc.getTotalRows());
        }
    }
}
