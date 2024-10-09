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

package io.milvus.v2.client;

import com.google.common.collect.Lists;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.pool.MilvusClientV2Pool;
import io.milvus.pool.PoolConfig;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.*;
import io.milvus.v2.service.database.request.*;
import io.milvus.v2.service.database.response.*;
import io.milvus.v2.service.index.request.*;
import io.milvus.v2.service.index.response.*;
import io.milvus.v2.service.partition.request.*;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.*;
import io.milvus.v2.service.vector.response.*;
import org.apache.commons.text.RandomStringGenerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.milvus.MilvusContainer;

import java.nio.ByteBuffer;
import java.util.*;

@Testcontainers(disabledWithoutDocker = true)
class MilvusClientV2DockerTest {
    private static MilvusClientV2 client;
    private static RandomStringGenerator generator;
    private static final int dimension = 256;

    private static final Gson GSON_INSTANCE = new Gson();

    private static final Random RANDOM = new Random();

    @Container
    private static final MilvusContainer milvus = new MilvusContainer("milvusdb/milvus:v2.3.20");

    @BeforeAll
    public static void setUp() {
        ConnectConfig config = ConnectConfig.builder()
                .uri(milvus.getEndpoint())
                .build();
        client = new MilvusClientV2(config);
        generator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();
    }

    @AfterAll
    public static void tearDown() throws InterruptedException {
        if (client != null) {
            client.close(5L);
        }
    }

    private List<Float> generateFolatVector(int dim) {
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < dim; ++i) {
            vector.add(RANDOM.nextFloat());
        }
        return vector;
    }

    private List<Float> generateFolatVector() {
        return generateFolatVector(dimension);
    }

    private List<List<Float>> generateFloatVectors(int count) {
        List<List<Float>> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            vectors.add(generateFolatVector());
        }

        return vectors;
    }

    private ByteBuffer generateBinaryVector() {
        int byteCount = dimension / 8;
        ByteBuffer vector = ByteBuffer.allocate(byteCount);
        for (int i = 0; i < byteCount; ++i) {
            vector.put((byte) RANDOM.nextInt(Byte.MAX_VALUE));
        }
        return vector;
    }

    private List<ByteBuffer> generateBinaryVectors(int count) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            vectors.add(generateBinaryVector());
        }
        return vectors;

    }


    private CreateCollectionReq.CollectionSchema baseSchema() {
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("bool_field")
                .dataType(DataType.Bool)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("int8_field")
                .dataType(DataType.Int8)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("int16_field")
                .dataType(DataType.Int16)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("int32_field")
                .dataType(DataType.Int32)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("int64_field")
                .dataType(DataType.Int64)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("float_field")
                .dataType(DataType.Float)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("double_field")
                .dataType(DataType.Double)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("varchar_field")
                .dataType(DataType.VarChar)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("json_field")
                .dataType(DataType.JSON)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("arr_int_field")
                .dataType(DataType.Array)
                .maxCapacity(50)
                .elementType(DataType.Int32)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("arr_float_field")
                .dataType(DataType.Array)
                .maxCapacity(20)
                .elementType(DataType.Float)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("arr_varchar_field")
                .dataType(DataType.Array)
                .maxCapacity(10)
                .elementType(DataType.VarChar)
                .build());
        return collectionSchema;
    }

    private JsonArray generateRandomArray(CreateCollectionReq.FieldSchema field) {
        DataType dataType = field.getDataType();
        if (dataType != DataType.Array) {
            Assertions.fail();
        }

        DataType eleType = field.getElementType();
        int eleCnt = RANDOM.nextInt(field.getMaxCapacity());
        switch (eleType) {
            case Bool: {
                List<Boolean> values = new ArrayList<>();
                for (int i = 0; i < eleCnt; i++) {
                    values.add(i%10 == 0);
                }
                return GSON_INSTANCE.toJsonTree(values).getAsJsonArray();
            }
            case Int8: {
                List<Integer> values = new ArrayList<>();
                for (int i = 0; i < eleCnt; i++) {
                    values.add(RANDOM.nextInt(256));
                }
                return GSON_INSTANCE.toJsonTree(values).getAsJsonArray();
            }
            case Int16:
            case Int32: {
                List<Integer> values = new ArrayList<>();
                for (int i = 0; i < eleCnt; i++) {
                    values.add(RANDOM.nextInt());
                }
                return GSON_INSTANCE.toJsonTree(values).getAsJsonArray();
            }
            case Int64: {
                List<Long> values = new ArrayList<>();
                for (int i = 0; i < eleCnt; i++) {
                    values.add(RANDOM.nextLong());
                }
                return GSON_INSTANCE.toJsonTree(values).getAsJsonArray();
            }
            case Float: {
                List<Float> values = new ArrayList<>();
                for (int i = 0; i < eleCnt; i++) {
                    values.add(RANDOM.nextFloat());
                }
                return GSON_INSTANCE.toJsonTree(values).getAsJsonArray();
            }
            case Double: {
                List<Double> values = new ArrayList<>();
                for (int i = 0; i < eleCnt; i++) {
                    values.add(RANDOM.nextDouble());
                }
                return GSON_INSTANCE.toJsonTree(values).getAsJsonArray();
            }
            case VarChar: {
                List<String> values = new ArrayList<>();
                for (int i = 0; i < eleCnt; i++) {
                    values.add(String.format("varchar_arr_%d", i));
                }
                return GSON_INSTANCE.toJsonTree(values).getAsJsonArray();
            }
            default:
                Assertions.fail();
        }
        return null;
    }

    private List<JsonObject> generateRandomData(CreateCollectionReq.CollectionSchema schema, long count) {
        List<CreateCollectionReq.FieldSchema> fields = schema.getFieldSchemaList();
        List<JsonObject> rows = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            for (CreateCollectionReq.FieldSchema field : fields) {
                DataType dataType = field.getDataType();
                switch (dataType) {
                    case Bool:
                        row.addProperty(field.getName(), i%3==0);
                        break;
                    case Int8:
                        row.addProperty(field.getName(), i%128);
                        break;
                    case Int16:
                        row.addProperty(field.getName(), i%32768);
                        break;
                    case Int32:
                        row.addProperty(field.getName(), i%65536);
                        break;
                    case Int64:
                        row.addProperty(field.getName(), i);
                        break;
                    case Float:
                        row.addProperty(field.getName(), i/8);
                        break;
                    case Double:
                        row.addProperty(field.getName(), i/3);
                        break;
                    case VarChar:
                        row.addProperty(field.getName(), String.format("varchar_%d", i));
                        break;
                    case JSON: {
                        JsonObject jsonObj = new JsonObject();
                        jsonObj.addProperty(String.format("JSON_%d", i), i);
                        jsonObj.add("flags", GSON_INSTANCE.toJsonTree(new long[]{i, i+1, i + 2}));
                        row.add(field.getName(), jsonObj);
                        break;
                    }
                    case Array: {
                        JsonArray array = generateRandomArray(field);
                        row.add(field.getName(), array);
                        break;
                    }
                    case FloatVector: {
                        List<Float> vector = generateFolatVector();
                        row.add(field.getName(), GSON_INSTANCE.toJsonTree(vector));
                        break;
                    }
                    case BinaryVector: {
                        ByteBuffer vector = generateBinaryVector();
                        row.add(field.getName(), GSON_INSTANCE.toJsonTree(vector.array()));
                        break;
                    }
                    default:
                        Assertions.fail();
                }
            }
            rows.add(row);
        }

        return rows;
    }

    private void verifyOutput(JsonObject row, Map<String, Object> entity) {
        Boolean b = (Boolean) entity.get("bool_field");
        Assertions.assertEquals(row.get("bool_field").getAsBoolean(), b);
        Integer i8 = (Integer) entity.get("int8_field");
        Assertions.assertEquals(row.get("int8_field").getAsInt(), i8);
        Integer i16 = (Integer) entity.get("int16_field");
        Assertions.assertEquals(row.get("int16_field").getAsInt(), i16);
        Integer i32 = (Integer) entity.get("int32_field");
        Assertions.assertEquals(row.get("int32_field").getAsInt(), i32);
        Long i64 = (Long) entity.get("int64_field");
        Assertions.assertEquals(row.get("int64_field").getAsLong(), i64);
        Float f32 = (Float) entity.get("float_field");
        Assertions.assertEquals(row.get("float_field").getAsFloat(), f32);
        Double f64 = (Double) entity.get("double_field");
        Assertions.assertEquals(row.get("double_field").getAsDouble(), f64);
        String str = (String) entity.get("varchar_field");
        Assertions.assertEquals(row.get("varchar_field").getAsString(), str);
        JsonObject jsn = (JsonObject) entity.get("json_field");
        Assertions.assertEquals(row.get("json_field").toString(), jsn.toString());

        List<Integer> arrInt = (List<Integer>) entity.get("arr_int_field");
        List<Integer> arrIntOri = GSON_INSTANCE.fromJson(row.get("arr_int_field"), new TypeToken<List<Integer>>() {}.getType());
        Assertions.assertEquals(arrIntOri, arrInt);
        List<Float> arrFloat = (List<Float>) entity.get("arr_float_field");
        List<Float> arrFloatOri = GSON_INSTANCE.fromJson(row.get("arr_float_field"), new TypeToken<List<Float>>() {}.getType());
        Assertions.assertEquals(arrFloatOri, arrFloat);
        List<String> arrStr = (List<String>) entity.get("arr_varchar_field");
        List<String> arrStrOri = GSON_INSTANCE.fromJson(row.get("arr_varchar_field"), new TypeToken<List<String>>() {}.getType());
        Assertions.assertEquals(arrStrOri, arrStr);
    }

    private long getRowCount(String collectionName) {
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter("")
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        Assertions.assertEquals(1, queryResults.size());
        return (long)queryResults.get(0).getEntity().get("count(*)");
    }


    @Test
    void testFloatVectors() {
        String randomCollectionName = generator.generate(10);

        String vectorFieldName = "float_vector";
        CreateCollectionReq.CollectionSchema collectionSchema = baseSchema();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(vectorFieldName)
                .dataType(DataType.FloatVector)
                .dimension(dimension)
                .build());

        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("M",16);
        extraParams.put("efConstruction",64);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(vectorFieldName)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.COSINE)
                .extraParams(extraParams)
                .build();

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .description("dummy")
                .collectionSchema(collectionSchema)
                .indexParams(Collections.singletonList(indexParam))
                .build();
        client.createCollection(requestCreate);

        // insert rows
        long count = 10000;
        List<JsonObject> data = generateRandomData(collectionSchema, count);
        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(data)
                .build());
        Assertions.assertEquals(count, insertResp.getInsertCnt());

        // create partition, upsert one row to the partition
        String partitionName = "PPP";
        client.createPartition(CreatePartitionReq.builder()
                .collectionName(randomCollectionName)
                .partitionName(partitionName)
                .build());

        List<JsonObject> upsertData = new ArrayList<>();
        upsertData.add(data.get((int) (count - 1)));
        UpsertResp upsertResp = client.upsert(UpsertReq.builder()
                .collectionName(randomCollectionName)
                .partitionName(partitionName)
                .data(upsertData)
                .build());
        Assertions.assertEquals(1, upsertResp.getUpsertCnt());

        // get row count
        long rowCount = getRowCount(randomCollectionName);
        Assertions.assertEquals(count + 1, rowCount);

        // describe collection
        DescribeCollectionResp descResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(randomCollectionName, descResp.getCollectionName());
        Assertions.assertEquals("dummy", descResp.getDescription());
        Assertions.assertEquals(2, descResp.getNumOfPartitions());
        Assertions.assertEquals(1, descResp.getVectorFieldNames().size());
        Assertions.assertEquals("id", descResp.getPrimaryFieldName());
        Assertions.assertTrue(descResp.getEnableDynamicField());
        Assertions.assertFalse(descResp.getAutoID());

        List<String> fieldNames = descResp.getFieldNames();
        Assertions.assertEquals(collectionSchema.getFieldSchemaList().size(), fieldNames.size());
        CreateCollectionReq.CollectionSchema schema = descResp.getCollectionSchema();
        for (String name : fieldNames) {
            CreateCollectionReq.FieldSchema f1 = collectionSchema.getField(name);
            CreateCollectionReq.FieldSchema f2 = schema.getField(name);
            Assertions.assertNotNull(f1);
            Assertions.assertNotNull(f2);
            Assertions.assertEquals(f1.getName(), f2.getName());
            Assertions.assertEquals(f1.getDescription(), f2.getDescription());
            Assertions.assertEquals(f1.getDataType(), f2.getDataType());
            Assertions.assertEquals(f1.getDimension(), f2.getDimension());
            Assertions.assertEquals(f1.getMaxLength(), f2.getMaxLength());
            Assertions.assertEquals(f1.getIsPrimaryKey(), f2.getIsPrimaryKey());
            Assertions.assertEquals(f1.getIsPartitionKey(), f2.getIsPartitionKey());
            if (f1.getDataType() == io.milvus.v2.common.DataType.Array) {
                Assertions.assertEquals(f1.getElementType(), f2.getElementType());
                Assertions.assertEquals(f1.getMaxCapacity(), f2.getMaxCapacity());
            }
        }

        // search in partition
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .partitionNames(Collections.singletonList(partitionName))
                .annsField(vectorFieldName)
                .data(Collections.singletonList(new FloatVec(generateFolatVector())))
                .topK(10)
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        Assertions.assertEquals(1, searchResults.size());
        Assertions.assertEquals(1, searchResults.get(0).size());
        Assertions.assertEquals(count - 1, searchResults.get(0).get(0).getId());


        // query entities
        int nq = 5;
        List<Long> targetIDs = new ArrayList<>();
        List<BaseVector> targetVectors = new ArrayList<>();
        for (int i = 0; i < nq; i++) {
            JsonObject row = data.get(RANDOM.nextInt((int)count));
            targetIDs.add(row.get("id").getAsLong());
            List<Float> vector = GSON_INSTANCE.fromJson(row.get(vectorFieldName), new TypeToken<List<Float>>() {}.getType());
            targetVectors.add(new FloatVec(vector));
        }

        GetResp getResp = client.get(GetReq.builder()
                .collectionName(randomCollectionName)
                .ids(new ArrayList<>(targetIDs))
                .outputFields(Collections.singletonList("*"))
                .build());
        for (QueryResp.QueryResult result : getResp.getGetResults()) {
            boolean found = false;
            for (int i = 0; i < nq; i++) {
                Map<String, Object> entity = result.getEntity();
                if (Objects.equals(targetIDs.get(i), entity.get("id"))) {
                    JsonObject row = data.get(targetIDs.get(i).intValue());
                    verifyOutput(row, entity);
                    found = true;
                    break;
                }
            }
            if (!found) {
                Assertions.fail();
            }
        }

        // search in collection
        int topk = 10;
        searchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField(vectorFieldName)
                .data(targetVectors)
                .topK(10)
                .outputFields(Collections.singletonList("*"))
                .build());
        searchResults = searchResp.getSearchResults();
        Assertions.assertEquals(nq, searchResults.size());
        for (int i = 0; i < nq; i++) {
            List<SearchResp.SearchResult> results = searchResults.get(i);
            Assertions.assertEquals(topk, results.size());
            SearchResp.SearchResult result = results.get(0);
            Assertions.assertEquals(targetIDs.get(i), result.getId());

            Map<String, Object> entity = result.getEntity();
            JsonObject row = data.get(targetIDs.get(i).intValue());
            verifyOutput(row, entity);
        }

        // query
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(randomCollectionName)
                .filter("JSON_CONTAINS_ANY(json_field[\"flags\"], [4, 100])")
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        Assertions.assertEquals(6, queryResults.size());

        client.dropCollection(DropCollectionReq.builder().collectionName(randomCollectionName).build());
    }

    @Test
    void testBinaryVectors() throws InterruptedException {
        String randomCollectionName = generator.generate(10);

        String vectorFieldName = "binary_vector";
        CreateCollectionReq.CollectionSchema collectionSchema = baseSchema();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(vectorFieldName)
                .dataType(DataType.BinaryVector)
                .dimension(dimension)
                .build());

        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("nlist",64);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(vectorFieldName)
                .indexType(IndexParam.IndexType.BIN_IVF_FLAT)
                .metricType(IndexParam.MetricType.JACCARD)
                .extraParams(extraParams)
                .build();

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(Collections.singletonList(indexParam))
                .build();
        client.createCollection(requestCreate);

        // insert rows
        long count = 10000;
        List<JsonObject> data = generateRandomData(collectionSchema, count);
        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(data)
                .build());
        Assertions.assertEquals(count, insertResp.getInsertCnt());

        // get row count
        long rowCount = getRowCount(randomCollectionName);
        Assertions.assertEquals(count, rowCount);

        // search in collection
        int nq = 5;
        int topk = 10;
        List<Long> targetIDs = new ArrayList<>();
        List<BaseVector> targetVectors = new ArrayList<>();
        for (int i = 0; i < nq; i++) {
            JsonObject row = data.get(RANDOM.nextInt((int)count));
            targetIDs.add(row.get("id").getAsLong());
            byte[] vector = GSON_INSTANCE.fromJson(row.get(vectorFieldName), new TypeToken<byte[]>() {}.getType());
            targetVectors.add(new BinaryVec(vector));
        }
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField(vectorFieldName)
                .data(targetVectors)
                .topK(10)
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        Assertions.assertEquals(nq, searchResults.size());
        for (int i = 0; i < nq; i++) {
            List<SearchResp.SearchResult> results = searchResults.get(i);
            Assertions.assertEquals(topk, results.size());
            Assertions.assertEquals(targetIDs.get(i), results.get(0).getId());
        }

        client.dropCollection(DropCollectionReq.builder().collectionName(randomCollectionName).build());
    }

    @Test
    void testDeleteUpsert() {
        String randomCollectionName = generator.generate(10);

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("pk")
                .dataType(DataType.VarChar)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("float_vector")
                .dataType(DataType.FloatVector)
                .dimension(4)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName("float_vector")
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(new HashMap<String,Object>(){{put("nlist", 64);}})
                .build());
        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(indexParams)
                .build();
        client.createCollection(requestCreate);

        // insert
        List<JsonObject> data = new ArrayList<>();
        Gson gson = new Gson();
        for (int i = 0; i < 10; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("pk", String.format("pk_%d", i));
            row.add("float_vector", gson.toJsonTree(new float[]{(float)i, (float)(i + 1), (float)(i + 2), (float)(i + 3)}));
            data.add(row);
        }

        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(data)
                .build());
        Assertions.assertEquals(10, insertResp.getInsertCnt());

        // delete
        DeleteResp deleteResp = client.delete(DeleteReq.builder()
                .collectionName(randomCollectionName)
                .ids(Arrays.asList("pk_5", "pk_8"))
                .build());
        Assertions.assertEquals(2, deleteResp.getDeleteCnt());

        // get row count
        long rowCount = getRowCount(randomCollectionName);
        Assertions.assertEquals(8L, rowCount);

        // upsert
        List<JsonObject> dataUpdate = new ArrayList<>();
        JsonObject row1 = new JsonObject();
        row1.addProperty("pk", "pk_5");
        row1.add("float_vector", gson.toJsonTree(new float[]{5.0f, 5.0f, 5.0f, 5.0f}));
        dataUpdate.add(row1);
        JsonObject row2 = new JsonObject();
        row2.addProperty("pk", "pk_2");
        row2.add("float_vector", gson.toJsonTree(new float[]{2.0f, 2.0f, 2.0f, 2.0f}));
        dataUpdate.add(row2);
        UpsertResp upsertResp = client.upsert(UpsertReq.builder()
                .collectionName(randomCollectionName)
                .data(dataUpdate)
                .build());
        Assertions.assertEquals(2, upsertResp.getUpsertCnt());

        // get row count
        rowCount = getRowCount(randomCollectionName);
        Assertions.assertEquals(9L, rowCount);

        // verify
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(randomCollectionName)
                .filter("pk == \"pk_2\" or pk == \"pk_5\"")
                .outputFields(Collections.singletonList("*"))
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        Assertions.assertEquals(2, queryResults.size());

        QueryResp.QueryResult result1 = queryResults.get(0);
        Map<String, Object> entity1 = result1.getEntity();
        Assertions.assertTrue(entity1.containsKey("pk"));
        Assertions.assertEquals("pk_2", entity1.get("pk"));
        Assertions.assertTrue(entity1.containsKey("float_vector"));
        Assertions.assertTrue(entity1.get("float_vector") instanceof List);
        List<Float> vector1 = (List<Float>) entity1.get("float_vector");
        for (Float f : vector1) {
            Assertions.assertEquals(2.0f, f);
        }

        QueryResp.QueryResult result2 = queryResults.get(1);
        Map<String, Object> entity2 = result2.getEntity();
        Assertions.assertTrue(entity2.containsKey("pk"));
        Assertions.assertEquals("pk_5", entity2.get("pk"));
        Assertions.assertTrue(entity2.containsKey("float_vector"));
        Assertions.assertTrue(entity2.get("float_vector") instanceof List);
        List<Float> vector2 = (List<Float>) entity2.get("float_vector");
        for (Float f : vector2) {
            Assertions.assertEquals(5.0f, f);
        }

        client.dropCollection(DropCollectionReq.builder().collectionName(randomCollectionName).build());
    }

    @Test
    void testAlias() {
        client.createCollection(CreateCollectionReq.builder()
                .collectionName("AAA")
                .description("desc_A")
                .dimension(100)
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName("BBB")
                .description("desc_B")
                .dimension(50)
                .build());

        client.createAlias(CreateAliasReq.builder()
                .collectionName("BBB")
                .alias("CCC")
                .build());

        DescribeCollectionResp descResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName("CCC")
                .build());
        Assertions.assertEquals("desc_B", descResp.getDescription());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName("BBB")
                .build());

        Assertions.assertThrows(MilvusClientException.class, ()->client.describeCollection(DescribeCollectionReq.builder()
                .collectionName("CCC")
                .build()));

        client.alterAlias(AlterAliasReq.builder()
                .collectionName("AAA")
                .alias("CCC")
                .build());

        descResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName("CCC")
                .build());
        Assertions.assertEquals("desc_A", descResp.getDescription());

        client.dropAlias(DropAliasReq.builder()
                .alias("CCC")
                .build());

        Assertions.assertThrows(MilvusClientException.class, ()->client.describeCollection(DescribeCollectionReq.builder()
                .collectionName("CCC")
                .build()));
    }

    @Test
    void testPartition() {
        String randomCollectionName = generator.generate(10);
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .dimension(4)
                .build());

        client.createPartition(CreatePartitionReq.builder()
                .collectionName(randomCollectionName)
                .partitionName("P1")
                .build());

        client.createPartition(CreatePartitionReq.builder()
                .collectionName(randomCollectionName)
                .partitionName("P2")
                .build());

        List<String> partitions = client.listPartitions(ListPartitionsReq.builder()
                .collectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(3, partitions.size());
        Assertions.assertTrue(partitions.contains("P1"));
        Assertions.assertTrue(partitions.contains("P2"));
        Assertions.assertTrue(partitions.contains("_default"));

        Boolean has = client.hasPartition(HasPartitionReq.builder()
                .collectionName(randomCollectionName)
                .partitionName("P1")
                .build());
        Assertions.assertTrue(has);

        client.releasePartitions(ReleasePartitionsReq.builder()
                .collectionName(randomCollectionName)
                .partitionNames(Collections.singletonList("P1"))
                .build());

        client.dropPartition(DropPartitionReq.builder()
                .collectionName(randomCollectionName)
                .partitionName("P1")
                .build());

        has = client.hasPartition(HasPartitionReq.builder()
                .collectionName(randomCollectionName)
                .partitionName("P1")
                .build());
        Assertions.assertFalse(has);

        partitions = client.listPartitions(ListPartitionsReq.builder()
                .collectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(2, partitions.size());
        Assertions.assertFalse(partitions.contains("P1"));
    }

    @Test
    void testIndex() {
        String randomCollectionName = generator.generate(10);

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(dimension)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        Map<String,Object> extra = new HashMap<>();
        extra.put("M",8);
        extra.put("efConstruction",64);
        indexes.add(IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.COSINE)
                .extraParams(extra)
                .build());
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .build());
        client.releaseCollection(ReleaseCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        DescribeIndexResp descResp = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("vector")
                .build());
        DescribeIndexResp.IndexDesc desc = descResp.getIndexDescByFieldName("vector");
        Assertions.assertEquals("vector", desc.getFieldName());
        Assertions.assertFalse(desc.getIndexName().isEmpty());
        Assertions.assertEquals(IndexParam.IndexType.HNSW, desc.getIndexType());
        Map<String, String> extraParams = desc.getExtraParams();
        Assertions.assertTrue(extraParams.containsKey("M"));
        Assertions.assertEquals("8", extraParams.get("M"));
        Assertions.assertTrue(extraParams.containsKey("efConstruction"));
        Assertions.assertEquals("64", extraParams.get("efConstruction"));

        client.dropIndex(DropIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("vector")
                .build());
        IndexParam param = IndexParam.builder()
                .fieldName("vector")
                .indexName("XXX")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.COSINE)
                .extraParams(extra)
                .build();
        client.createIndex(CreateIndexReq.builder()
                .collectionName(randomCollectionName)
                .indexParams(Collections.singletonList(param))
                .build());
        descResp = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("vector")
                .build());

        desc = descResp.getIndexDescByFieldName("vector");
        Assertions.assertEquals("vector", desc.getFieldName());
        Assertions.assertEquals("XXX", desc.getIndexName());
        Assertions.assertEquals(IndexParam.IndexType.HNSW, desc.getIndexType());
        Assertions.assertEquals(IndexParam.MetricType.COSINE, desc.getMetricType());
        extraParams = desc.getExtraParams();
        Assertions.assertTrue(extraParams.containsKey("M"));
        Assertions.assertEquals("8", extraParams.get("M"));
        Assertions.assertTrue(extraParams.containsKey("efConstruction"));
        Assertions.assertEquals("64", extraParams.get("efConstruction"));
    }

    @Test
    void testCacheCollectionSchema() {
        String randomCollectionName = generator.generate(10);

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .autoID(true)
                .dimension(dimension)
                .build());

        // insert
        JsonObject row = new JsonObject();
        row.add("vector", GSON_INSTANCE.toJsonTree(generateFloatVectors(1).get(0)));
        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(Collections.singletonList(row))
                .build());
        Assertions.assertEquals(1L, insertResp.getInsertCnt());

        // drop collection
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        // create a new collection with the same name, different schema
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .autoID(true)
                .dimension(100)
                .build());

        // insert wrong data
        Assertions.assertThrows(MilvusClientException.class, ()->client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(Collections.singletonList(row))
                .build()));

        // insert correct data
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            vector.add(RANDOM.nextFloat());
        }
        row.add("vector", GSON_INSTANCE.toJsonTree(vector));
        insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(Collections.singletonList(row))
                .build());
        Assertions.assertEquals(1L, insertResp.getInsertCnt());
    }

    @Test
    public void testIterator() {
        String randomCollectionName = generator.generate(10);
        CreateCollectionReq.CollectionSchema collectionSchema = baseSchema();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("float_vector")
                .dataType(DataType.FloatVector)
                .dimension(dimension)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName("float_vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(indexParams)
                .build();
        client.createCollection(requestCreate);

        // insert rows
        long count = 10000;
        List<JsonObject> data = generateRandomData(collectionSchema, count);
        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(data)
                .build());
        Assertions.assertEquals(count, insertResp.getInsertCnt());

        // get row count
        long rowCount = getRowCount(randomCollectionName);
        Assertions.assertEquals(count, rowCount);

        // search iterator
        SearchIterator searchIterator = client.searchIterator(SearchIteratorReq.builder()
                .collectionName(randomCollectionName)
                .outputFields(Lists.newArrayList("*"))
                .batchSize(20L)
                .vectorFieldName("float_vector")
                .vectors(Collections.singletonList(new FloatVec(generateFolatVector())))
                .expr("int64_field > 500 && int64_field < 1000")
                .params("{\"range_filter\": 5.0, \"radius\": 50.0}")
                .topK(1000)
                .metricType(IndexParam.MetricType.L2)
                .consistencyLevel(ConsistencyLevel.EVENTUALLY)
                .build());

        int counter = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = searchIterator.next();
            if (res.isEmpty()) {
                System.out.println("search iteration finished, close");
                searchIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                Assertions.assertInstanceOf(Float.class, record.get("score"));
                Assertions.assertTrue((float)record.get("score") >= 5.0);
                Assertions.assertTrue((float)record.get("score") <= 50.0);

                Assertions.assertInstanceOf(Boolean.class, record.get("bool_field"));
                Assertions.assertInstanceOf(Integer.class, record.get("int8_field"));
                Assertions.assertInstanceOf(Integer.class, record.get("int16_field"));
                Assertions.assertInstanceOf(Integer.class, record.get("int32_field"));
                Assertions.assertInstanceOf(Long.class, record.get("int64_field"));
                Assertions.assertInstanceOf(Float.class, record.get("float_field"));
                Assertions.assertInstanceOf(Double.class, record.get("double_field"));
                Assertions.assertInstanceOf(String.class, record.get("varchar_field"));
                Assertions.assertInstanceOf(JsonObject.class, record.get("json_field"));
                Assertions.assertInstanceOf(List.class, record.get("arr_int_field"));
                Assertions.assertInstanceOf(List.class, record.get("float_vector"));

                long int64Val = (long)record.get("int64_field");
                Assertions.assertTrue(int64Val > 500L && int64Val < 1000L);

                String varcharVal = (String)record.get("varchar_field");
                Assertions.assertTrue(varcharVal.startsWith("varchar_"));

                JsonObject jsonObj = (JsonObject)record.get("json_field");
                Assertions.assertTrue(jsonObj.has(String.format("JSON_%d", int64Val)));

                List<Integer> intArr = (List<Integer>)record.get("arr_int_field");
                Assertions.assertTrue(intArr.size() <= 50); // max capacity 50 is defined in the baseSchema()

                List<Float> floatVector = (List<Float>)record.get("float_vector");
                Assertions.assertEquals(dimension, floatVector.size());

                counter++;
            }
        }
        System.out.println(String.format("There are %d items match distance between [5.0, 50.0]", counter));

        // query iterator
        QueryIterator queryIterator = client.queryIterator(QueryIteratorReq.builder()
                .collectionName(randomCollectionName)
                .expr("int64_field < 300")
                .outputFields(Lists.newArrayList("*"))
                .batchSize(50L)
                .offset(5)
                .limit(400)
                .consistencyLevel(ConsistencyLevel.EVENTUALLY)
                .build());

        counter = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = queryIterator.next();
            if (res.isEmpty()) {
                System.out.println("query iteration finished, close");
                queryIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                Assertions.assertInstanceOf(Boolean.class, record.get("bool_field"));
                Assertions.assertInstanceOf(Integer.class, record.get("int8_field"));
                Assertions.assertInstanceOf(Integer.class, record.get("int16_field"));
                Assertions.assertInstanceOf(Integer.class, record.get("int32_field"));
                Assertions.assertInstanceOf(Long.class, record.get("int64_field"));
                Assertions.assertInstanceOf(Float.class, record.get("float_field"));
                Assertions.assertInstanceOf(Double.class, record.get("double_field"));
                Assertions.assertInstanceOf(String.class, record.get("varchar_field"));
                Assertions.assertInstanceOf(JsonObject.class, record.get("json_field"));
                Assertions.assertInstanceOf(List.class, record.get("arr_int_field"));
                Assertions.assertInstanceOf(List.class, record.get("float_vector"));

                long int64Val = (long)record.get("int64_field");
                Assertions.assertTrue(int64Val < 300L);

                String varcharVal = (String)record.get("varchar_field");
                Assertions.assertTrue(varcharVal.startsWith("varchar_"));

                JsonObject jsonObj = (JsonObject)record.get("json_field");
                Assertions.assertTrue(jsonObj.has(String.format("JSON_%d", int64Val)));

                List<Integer> intArr = (List<Integer>)record.get("arr_int_field");
                Assertions.assertTrue(intArr.size() <= 50); // max capacity 50 is defined in the baseSchema()

                List<Float> floatVector = (List<Float>)record.get("float_vector");
                Assertions.assertEquals(dimension, floatVector.size());

                counter++;
            }
        }
        Assertions.assertEquals(295, counter);

        client.dropCollection(DropCollectionReq.builder().collectionName(randomCollectionName).build());
    }

    @Test
    void testDatabase() {
        // get current database
        ListDatabasesResp listDatabasesResp = client.listDatabases();
        List<String> dbNames = listDatabasesResp.getDatabaseNames();
        Assertions.assertEquals(1, dbNames.size());
        String currentDbName = dbNames.get(0);

        // create a new database
        String tempDatabaseName = "db_temp";
        CreateDatabaseReq createDatabaseReq = CreateDatabaseReq.builder()
                .databaseName(tempDatabaseName)
                .build();
        client.createDatabase(createDatabaseReq);

        listDatabasesResp = client.listDatabases();
        dbNames = listDatabasesResp.getDatabaseNames();
        Assertions.assertTrue(dbNames.contains(tempDatabaseName));

        // switch to the new database
        Assertions.assertDoesNotThrow(()->client.useDatabase(tempDatabaseName));

        // create a collection in the new database
        String randomCollectionName = generator.generate(10);
        String vectorFieldName = "float_vector";
        CreateCollectionReq.CollectionSchema collectionSchema = baseSchema();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(vectorFieldName)
                .dataType(DataType.FloatVector)
                .dimension(dimension)
                .build());

        IndexParam indexParam = IndexParam.builder()
                .fieldName(vectorFieldName)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build();

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(Collections.singletonList(indexParam))
                .build();
        client.createCollection(requestCreate);

        ListCollectionsResp listCollectionsResp = client.listCollections();
        List<String> collectionNames = listCollectionsResp.getCollectionNames();
        Assertions.assertEquals(1, collectionNames.size());
        Assertions.assertTrue(collectionNames.contains(randomCollectionName));

        // drop the collection so that we can drop the database later
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        // switch to the old database
        Assertions.assertDoesNotThrow(()->client.useDatabase(currentDbName));

        // drop the new database
        client.dropDatabase(DropDatabaseReq.builder()
                .databaseName(tempDatabaseName)
                .build());

        // check the new database is deleted
        listDatabasesResp = client.listDatabases();
        dbNames = listDatabasesResp.getDatabaseNames();
        Assertions.assertFalse(dbNames.contains(tempDatabaseName));
    }

    @Test
    void testClientPool() {
        try {
            ConnectConfig connectConfig = ConnectConfig.builder()
                    .uri(milvus.getEndpoint())
                    .rpcDeadlineMs(100L)
                    .build();
            PoolConfig poolConfig = PoolConfig.builder()
                    .build();
            MilvusClientV2Pool pool = new MilvusClientV2Pool(poolConfig, connectConfig);

            List<Thread> threadList = new ArrayList<>();
            int threadCount = 10;
            int requestPerThread = 10;
            String key = "192.168.1.1";
            for (int k = 0; k < threadCount; k++) {
                Thread t = new Thread(() -> {
                    for (int i = 0; i < requestPerThread; i++) {
                        MilvusClientV2 client = pool.getClient(key);
                        String version = client.getServerVersion();
//                            System.out.printf("%d, %s%n", i, version);
                        System.out.printf("idle %d, active %d%n", pool.getIdleClientNumber(key), pool.getActiveClientNumber(key));
                        pool.returnClient(key, client);
                    }
                    System.out.println(String.format("Thread %s finished", Thread.currentThread().getName()));
                });
                t.start();
                threadList.add(t);
            }

            for (Thread t : threadList) {
                t.join();
            }

            System.out.println(String.format("idle %d, active %d", pool.getIdleClientNumber(key), pool.getActiveClientNumber(key)));
            System.out.println(String.format("total idle %d, total active %d", pool.getTotalIdleClientNumber(), pool.getTotalActiveClientNumber()));
            pool.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    void testMultiThreadsInsert() {
        String randomCollectionName = generator.generate(10);
        int dim = 64;

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.VarChar)
                .isPrimaryKey(Boolean.TRUE)
                .maxLength(65535)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(dim)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("dataTime")
                .dataType(DataType.Int64)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build());
        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(indexParams)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");

        try {
            Gson gson = new Gson();
            Random rand = new Random();
            List<Thread> threadList = new ArrayList<>();
            for (int k = 0; k < 10; k++) {
                Thread t = new Thread(() -> {
                    for (int i = 0; i < 20; i++) {
                        List<JsonObject> rows = new ArrayList<>();
                        int cnt = rand.nextInt(100) + 100;
                        for (int j = 0; j < cnt; j++) {
                            JsonObject obj = new JsonObject();
                            obj.addProperty("id", String.format("%d", i*cnt + j));
                            List<Float> vector = generateFolatVector(dim);
                            obj.add("vector", gson.toJsonTree(vector));
                            obj.addProperty("dataTime", System.currentTimeMillis());
                            rows.add(obj);
                        }

                        client.insert(InsertReq.builder()
                                .collectionName(randomCollectionName)
                                .data(rows)
                                .build());
                    }
                });
                t.start();
                threadList.add(t);
            }

            for (Thread t : threadList) {
                t.join();
            }
            System.out.println("Multi-thread insert done");

            QueryResp queryResp = client.query(QueryReq.builder()
                    .filter("")
                    .collectionName(randomCollectionName)
                    .outputFields(Collections.singletonList("count(*)"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            System.out.println(queryResp.getQueryResults().get(0).getEntity().get("count(*)"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail(e.getMessage());
        }

        try {
            Gson gson = new Gson();
            Random rand = new Random();
            List<Thread> threadList = new ArrayList<>();
            for (int k = 0; k < 10; k++) {
                Thread t = new Thread(() -> {
                    for (int i = 0; i < 20; i++) {
                        List<JsonObject> rows = new ArrayList<>();
                        int cnt = rand.nextInt(100) + 100;
                        for (int j = 0; j < cnt; j++) {
                            JsonObject obj = new JsonObject();
                            obj.addProperty("id", String.format("%d", i*cnt + j));
                            List<Float> vector = generateFolatVector(dim);
                            obj.add("vector", gson.toJsonTree(vector));
                            obj.addProperty("dataTime", System.currentTimeMillis());
                            rows.add(obj);
                        }

                        UpsertReq upsertReq = UpsertReq.builder()
                                .collectionName(randomCollectionName)
                                .data(rows)
                                .build();
                        client.upsert(upsertReq);
                    }
                });
                t.start();
                threadList.add(t);
            }

            for (Thread t : threadList) {
                t.join();
            }
            System.out.println("Multi-thread upsert done");

            QueryResp queryResp = client.query(QueryReq.builder()
                    .filter("")
                    .collectionName(randomCollectionName)
                    .outputFields(Collections.singletonList("count(*)"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            System.out.println(queryResp.getQueryResults().get(0).getEntity().get("count(*)"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail(e.getMessage());
        }
    }
}
