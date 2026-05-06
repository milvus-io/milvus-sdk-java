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

package io.milvus.v2.service.vector;

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.milvus.common.utils.Float16Utils;
import io.milvus.common.utils.JsonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.*;
import io.milvus.v2.service.vector.response.*;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.milvus.MilvusContainer;

import java.nio.ByteBuffer;
import java.util.*;

@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NullableVectorTest {

    @Container
    private static final MilvusContainer milvus = new MilvusContainer(io.milvus.TestUtils.MilvusDockerImageID)
            .withEnv("DEPLOY_MODE", "STANDALONE");

    private static MilvusClientV2 client;
    private static final int DIMENSION = 8;
    private static final Random RANDOM = new Random();
    private static final String COLLECTION_PREFIX = "test_nullable_vec_";

    // Vector type configurations
    private static final List<VectorTypeConfig> VECTOR_TYPES = Arrays.asList(
            new VectorTypeConfig("float_vector", DataType.FloatVector, DIMENSION, "L2", "FLAT"),
            new VectorTypeConfig("binary_vector", DataType.BinaryVector, DIMENSION * 8, "HAMMING", "BIN_FLAT"),
            new VectorTypeConfig("float16_vector", DataType.Float16Vector, DIMENSION, "L2", "FLAT"),
            new VectorTypeConfig("bfloat16_vector", DataType.BFloat16Vector, DIMENSION, "L2", "FLAT"),
            new VectorTypeConfig("sparse_float_vector", DataType.SparseFloatVector, 0, "IP", "SPARSE_INVERTED_INDEX"),
            new VectorTypeConfig("int8_vector", DataType.Int8Vector, DIMENSION, "L2", "HNSW")
    );

    static class VectorTypeConfig {
        String name;
        DataType dataType;
        int dimension;
        String metricType;
        String indexType;

        VectorTypeConfig(String name, DataType dataType, int dimension, String metricType, String indexType) {
            this.name = name;
            this.dataType = dataType;
            this.dimension = dimension;
            this.metricType = metricType;
            this.indexType = indexType;
        }
    }

    @BeforeAll
    public static void setUp() {
        ConnectConfig config = ConnectConfig.builder()
                .uri(milvus.getEndpoint())
                .build();
        client = new MilvusClientV2(config);
    }

    @AfterAll
    public static void tearDown() {
        if (client != null) {
            // Cleanup collections
            for (VectorTypeConfig vtc : VECTOR_TYPES) {
                String collectionName = COLLECTION_PREFIX + vtc.name;
                try {
                    client.dropCollection(DropCollectionReq.builder()
                            .collectionName(collectionName)
                            .build());
                } catch (Exception ignored) {
                }
            }
            try {
                client.close(5L);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private Object generateVector(VectorTypeConfig config) {
        switch (config.dataType) {
            case FloatVector: {
                List<Float> vector = new ArrayList<>();
                for (int i = 0; i < config.dimension; i++) {
                    vector.add(RANDOM.nextFloat());
                }
                return vector;
            }
            case BinaryVector: {
                int byteCount = config.dimension / 8;
                byte[] bytes = new byte[byteCount];
                RANDOM.nextBytes(bytes);
                ByteBuffer buf = ByteBuffer.wrap(bytes);
                return buf;
            }
            case Float16Vector: {
                // Generate float values first, then convert to fp16
                List<Float> floatVec = new ArrayList<>();
                for (int i = 0; i < config.dimension; i++) {
                    floatVec.add(RANDOM.nextFloat());
                }
                ByteBuffer buf = Float16Utils.f32VectorToFp16Buffer(floatVec);
                buf.rewind();
                return buf;
            }
            case BFloat16Vector: {
                // Generate float values first, then convert to bf16
                List<Float> floatVec = new ArrayList<>();
                for (int i = 0; i < config.dimension; i++) {
                    floatVec.add(RANDOM.nextFloat());
                }
                ByteBuffer buf = Float16Utils.f32VectorToBf16Buffer(floatVec);
                buf.rewind();
                return buf;
            }
            case Int8Vector: {
                ByteBuffer buf = ByteBuffer.allocate(config.dimension);
                for (int i = 0; i < config.dimension; i++) {
                    buf.put((byte) (RANDOM.nextInt(256) - 128));
                }
                buf.rewind();
                return buf;
            }
            case SparseFloatVector: {
                SortedMap<Long, Float> sparse = new TreeMap<>();
                int nnz = RANDOM.nextInt(10) + 2;
                for (int i = 0; i < nnz; i++) {
                    sparse.put((long) i, RANDOM.nextFloat());
                }
                return sparse;
            }
            default:
                throw new IllegalArgumentException("Unknown vector type: " + config.dataType);
        }
    }

    @Test
    @Order(1)
    void testNullableFloatVector() throws Exception {
        testNullableVector(VECTOR_TYPES.get(0));
    }

    @Test
    @Order(2)
    void testNullableBinaryVector() throws Exception {
        testNullableVector(VECTOR_TYPES.get(1));
    }

    @Test
    @Order(3)
    void testNullableFloat16Vector() throws Exception {
        testNullableVector(VECTOR_TYPES.get(2));
    }

    @Test
    @Order(4)
    void testNullableBFloat16Vector() throws Exception {
        testNullableVector(VECTOR_TYPES.get(3));
    }

    @Test
    @Order(5)
    void testNullableSparseFloatVector() throws Exception {
        testNullableVector(VECTOR_TYPES.get(4));
    }

    @Test
    @Order(6)
    void testNullableInt8Vector() throws Exception {
        testNullableVector(VECTOR_TYPES.get(5));
    }

    /**
     * Test that adding a non-nullable vector field to existing collection should fail.
     */
    @Test
    @Order(7)
    void testAddVectorFieldMustBeNullable() throws Exception {
        String collectionName = COLLECTION_PREFIX + "add_field_test";
        System.out.println("\n[Test] add_vector_field_must_be_nullable");

        // Drop if exists
        try {
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
        } catch (Exception ignored) {
        }

        // Create collection with one vector field
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("embedding")
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        // Try to add a non-nullable vector field - should fail
        Exception exception = Assertions.assertThrows(Exception.class, () -> {
            client.addCollectionField(AddCollectionFieldReq.builder()
                    .collectionName(collectionName)
                    .fieldName("embedding_v2")
                    .dataType(DataType.FloatVector)
                    .dimension(DIMENSION)
                    .isNullable(false)  // Non-nullable should fail
                    .build());
        });
        System.out.println("  Expected error: " + exception.getMessage());
        Assertions.assertTrue(exception.getMessage().toLowerCase().contains("nullable"),
                "Error should mention nullable requirement");

        // Cleanup
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        System.out.println("  PASSED");
    }

    private void testNullableVector(VectorTypeConfig config) throws Exception {
        String collectionName = COLLECTION_PREFIX + config.name;
        System.out.println("\n[Test] " + config.name);

        // Drop if exists
        try {
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
        } catch (Exception ignored) {
        }

        // Create schema with nullable vector field
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("name")
                .dataType(DataType.VarChar)
                .maxLength(100)
                .build());

        // Add nullable vector field
        AddFieldReq.AddFieldReqBuilder fieldBuilder = AddFieldReq.builder()
                .fieldName("embedding")
                .dataType(config.dataType)
                .isNullable(true);
        if (config.dimension > 0) {
            fieldBuilder.dimension(config.dimension);
        }
        schema.addField(fieldBuilder.build());

        // Create collection
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        // Create index
        IndexParam indexParam = IndexParam.builder()
                .fieldName("embedding")
                .metricType(IndexParam.MetricType.valueOf(config.metricType))
                .indexType(IndexParam.IndexType.valueOf(config.indexType))
                .build();
        client.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());

        // Load collection
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        // Prepare test data: 100 rows, ~50% null vectors
        int totalRows = 100;
        int nullPercent = 50;
        List<JsonObject> data = new ArrayList<>();
        int expectedNullCount = 0;
        int expectedValidCount = 0;

        for (int i = 1; i <= totalRows; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", (long) i);
            row.addProperty("name", "row_" + i);

            boolean isNull = RANDOM.nextInt(100) < nullPercent;
            if (isNull) {
                row.add("embedding", JsonNull.INSTANCE);
                expectedNullCount++;
            } else {
                Object vector = generateVector(config);
                expectedValidCount++;
                if (config.dataType == DataType.FloatVector) {
                    row.add("embedding", JsonUtils.toJsonTree(vector));
                } else if (config.dataType == DataType.SparseFloatVector) {
                    row.add("embedding", JsonUtils.toJsonTree(vector));
                } else {
                    // For binary/fp16/bf16/int8, encode as base64 or byte array
                    ByteBuffer buf = (ByteBuffer) vector;
                    byte[] bytes = new byte[buf.remaining()];
                    buf.get(bytes);
                    buf.rewind();
                    row.add("embedding", JsonUtils.toJsonTree(bytes));
                }
            }
            data.add(row);
        }

        // Insert data
        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(data)
                .build());
        Assertions.assertEquals(totalRows, insertResp.getInsertCnt());

        // Wait for data to be available
        Thread.sleep(1000);

        // Query all data
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter("id >= 0")
                .outputFields(Arrays.asList("id", "name", "embedding"))
                .limit(totalRows + 10)
                .build());

        List<QueryResp.QueryResult> results = queryResp.getQueryResults();
        Assertions.assertEquals(totalRows, results.size());

        int nullCount = 0;
        int validCount = 0;
        for (QueryResp.QueryResult result : results) {
            Map<String, Object> entity = result.getEntity();
            Object embedding = entity.get("embedding");
            if (embedding == null) {
                nullCount++;
            } else {
                validCount++;
            }
        }
        Assertions.assertEquals(expectedNullCount, nullCount);
        Assertions.assertEquals(expectedValidCount, validCount);

        // Search - should only return non-null vectors
        Object searchVector = generateVector(config);
        BaseVector searchVec;
        if (config.dataType == DataType.FloatVector) {
            searchVec = new FloatVec((List<Float>) searchVector);
        } else if (config.dataType == DataType.BinaryVector) {
            searchVec = new BinaryVec((ByteBuffer) searchVector);
        } else if (config.dataType == DataType.Float16Vector) {
            searchVec = new Float16Vec((ByteBuffer) searchVector);
        } else if (config.dataType == DataType.BFloat16Vector) {
            searchVec = new BFloat16Vec((ByteBuffer) searchVector);
        } else if (config.dataType == DataType.SparseFloatVector) {
            searchVec = new SparseFloatVec((SortedMap<Long, Float>) searchVector);
        } else {
            // Int8Vector
            searchVec = new Int8Vec((ByteBuffer) searchVector);
        }

        int searchLimit = Math.min(50, expectedValidCount);
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(searchVec))
                .annsField("embedding")
                .topK(searchLimit)
                .outputFields(Arrays.asList("id", "name", "embedding"))
                .build());

        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        Assertions.assertFalse(searchResults.isEmpty());
        List<SearchResp.SearchResult> hits = searchResults.get(0);

        // Search should only return non-null vectors
        Assertions.assertTrue(hits.size() <= expectedValidCount, "Search should return at most expectedValidCount results");
        for (SearchResp.SearchResult hit : hits) {
            Map<String, Object> entity = hit.getEntity();
            Object embedding = entity.get("embedding");
            Assertions.assertNotNull(embedding, "Search should not return null vectors");
        }

        System.out.println("  PASSED");
    }
}
