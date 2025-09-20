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
import io.milvus.TestUtils;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.common.resourcegroup.*;
import io.milvus.common.utils.Float16Utils;
import io.milvus.common.utils.GTsDict;
import io.milvus.common.utils.JsonUtils;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.orm.iterator.SearchIteratorV2;
import io.milvus.param.Constant;
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
import io.milvus.v2.service.rbac.PrivilegeGroup;
import io.milvus.v2.service.rbac.request.*;
import io.milvus.v2.service.rbac.response.*;
import io.milvus.v2.service.resourcegroup.request.*;
import io.milvus.v2.service.resourcegroup.response.*;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.*;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.QueryReq; // 显式导入
import io.milvus.v2.service.vector.request.data.*;
import io.milvus.v2.service.vector.request.ranker.*;
import io.milvus.v2.service.vector.response.*;
import org.apache.commons.lang3.StringUtils;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Testcontainers(disabledWithoutDocker = true)
class MilvusClientV2DockerTest {
    private static MilvusClientV2 client;
    private static RandomStringGenerator generator;
    private static final int DIMENSION = 256;
    private static final Random RANDOM = new Random();
    private static final TestUtils utils = new TestUtils(DIMENSION);

    @Container
    private static final MilvusContainer milvus = new MilvusContainer(TestUtils.MilvusDockerImageID)
            .withEnv("DEPLOY_MODE", "STANDALONE");

    @BeforeAll
    public static void setUp() {
        try {
            Thread.sleep(3000); // Sleep for few seconds since the master branch milvus healthz check is bug
        } catch (InterruptedException ignored) {
        }

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
                        jsonObj.add("flags", JsonUtils.toJsonTree(new long[]{i, i+1, i + 2}));
                        row.add(field.getName(), jsonObj);
                        break;
                    }
                    case Array: {
                        List<?> values = utils.generateRandomArray(io.milvus.grpc.DataType.valueOf(field.getElementType().name()), field.getMaxCapacity());
                        JsonArray array = JsonUtils.toJsonTree(values).getAsJsonArray();
                        row.add(field.getName(), array);
                        break;
                    }
                    case FloatVector: {
                        List<Float> vector = utils.generateFloatVector();
                        row.add(field.getName(), JsonUtils.toJsonTree(vector));
                        break;
                    }
                    case BinaryVector: {
                        ByteBuffer vector = utils.generateBinaryVector();
                        row.add(field.getName(), JsonUtils.toJsonTree(vector.array()));
                        break;
                    }
                    case Float16Vector: {
                        ByteBuffer vector = utils.generateFloat16Vector();
                        row.add(field.getName(), JsonUtils.toJsonTree(vector.array()));
                        break;
                    }
                    case BFloat16Vector: {
                        ByteBuffer vector = utils.generateBFloat16Vector();
                        row.add(field.getName(), JsonUtils.toJsonTree(vector.array()));
                        break;
                    }
                    case SparseFloatVector: {
                        SortedMap<Long, Float> vector = utils.generateSparseVector();
                        row.add(field.getName(), JsonUtils.toJsonTree(vector));
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
        List<Integer> arrIntOri = JsonUtils.fromJson(row.get("arr_int_field"), new TypeToken<List<Integer>>() {}.getType());
        Assertions.assertEquals(arrIntOri, arrInt);
        List<Float> arrFloat = (List<Float>) entity.get("arr_float_field");
        List<Float> arrFloatOri = JsonUtils.fromJson(row.get("arr_float_field"), new TypeToken<List<Float>>() {}.getType());
        Assertions.assertEquals(arrFloatOri, arrFloat);
        List<String> arrStr = (List<String>) entity.get("arr_varchar_field");
        List<String> arrStrOri = JsonUtils.fromJson(row.get("arr_varchar_field"), new TypeToken<List<String>>() {}.getType());
        Assertions.assertEquals(arrStrOri, arrStr);
    }

    private long getRowCount(String collectionName) {
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(collectionName)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        Assertions.assertEquals(1, queryResults.size());
        return (long)queryResults.get(0).getEntity().get("count(*)");
    }


    @Test
    void testFloatVectors() {
        CheckHealthResp healthy = client.checkHealth();
        Assertions.assertTrue(healthy.getIsHealthy());

        String randomCollectionName = generator.generate(10);

        String vectorFieldName = "float_vector";
        CreateCollectionReq.CollectionSchema collectionSchema = baseSchema();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(vectorFieldName)
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .description("dummy")
                .collectionSchema(collectionSchema)
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

        // flush
        client.flush(FlushReq.builder()
                .collectionNames(Collections.singletonList(randomCollectionName))
                .build());

        // master branch, getPersistentSegmentInfo cannot ensure the segment is returned after flush()
        while(true) {
            // get persistent segment info
            GetPersistentSegmentInfoResp pSegInfo = client.getPersistentSegmentInfo(GetPersistentSegmentInfoReq.builder()
                    .collectionName(randomCollectionName)
                    .build());
            if (pSegInfo.getSegmentInfos().size() == 0) {
                continue;
            }
            Assertions.assertEquals(1, pSegInfo.getSegmentInfos().size());
            GetPersistentSegmentInfoResp.PersistentSegmentInfo pInfo = pSegInfo.getSegmentInfos().get(0);
            Assertions.assertTrue(pInfo.getSegmentID() > 0L);
            Assertions.assertTrue(pInfo.getCollectionID() > 0L);
            Assertions.assertTrue(pInfo.getPartitionID() > 0L);
            Assertions.assertEquals(count, pInfo.getNumOfRows());
            Assertions.assertEquals("Flushed", pInfo.getState());
            Assertions.assertEquals("L1", pInfo.getLevel());
//            Assertions.assertFalse(pInfo.getIsSorted());
            break;
        }

        // compact
        CompactResp compactResp = client.compact(CompactReq.builder()
                .collectionName(randomCollectionName)
                .build());
        // there is a segment is flushed by the flush() interface, there could be a compaction task created
        Assertions.assertTrue(compactResp.getCompactionID() == -1L || compactResp.getCompactionID() > 0L);

        // create index
        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("M", 64);
        extraParams.put("efConstruction", 200);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(vectorFieldName)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.COSINE)
                .extraParams(extraParams)
                .build();
        client.createIndex(CreateIndexReq.builder()
                .collectionName(randomCollectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());

        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        // get query segment info
        GetQuerySegmentInfoResp qSegInfo = client.getQuerySegmentInfo(GetQuerySegmentInfoReq.builder()
                .collectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(1, qSegInfo.getSegmentInfos().size());
        GetQuerySegmentInfoResp.QuerySegmentInfo qInfo = qSegInfo.getSegmentInfos().get(0);
        Assertions.assertTrue(qInfo.getSegmentID() > 0L);
        Assertions.assertTrue(qInfo.getCollectionID() > 0L);
        Assertions.assertTrue(qInfo.getPartitionID() > 0L);
        Assertions.assertTrue(qInfo.getMemSize() >= 0L);
        Assertions.assertEquals(count, qInfo.getNumOfRows());
        Assertions.assertEquals(vectorFieldName, qInfo.getIndexName());
        Assertions.assertTrue(qInfo.getIndexID() > 0L);
        Assertions.assertEquals("Sealed", qInfo.getState());
        Assertions.assertEquals("L1", qInfo.getLevel());
        Assertions.assertEquals(1, qInfo.getNodeIDs().size());
        Assertions.assertTrue(qInfo.getNodeIDs().get(0) > 0L);
        Assertions.assertTrue(qInfo.getIsSorted());

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
        Assertions.assertFalse(descResp.getEnableDynamicField()); // from v2.4.6, we add this flag in CollectionSchema, default value is False(follow the pymilvus behavior)
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
                .data(Collections.singletonList(new FloatVec(utils.generateFloatVector())))
                .limit(10)
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
            List<Float> vector = JsonUtils.fromJson(row.get(vectorFieldName), new TypeToken<List<Float>>() {}.getType());
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
                .limit(topk)
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

        {
            // query with template
            Map<String,Object> template = new HashMap<>();
            template.put("id_arr", Arrays.asList(5, 6, 7));
            QueryResp queryResp = client.query(QueryReq.builder()
                    .collectionName(randomCollectionName)
                    .filter("id in {id_arr}")
                    .filterTemplateValues(template)
                    .build());
            List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
            Assertions.assertEquals(3, queryResults.size());
        }

        {
            // query with limit
            QueryResp queryResp = client.query(QueryReq.builder()
                    .collectionName(randomCollectionName)
                    .limit(8)
                    .build());
            List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
            Assertions.assertEquals(8, queryResults.size());
        }

        {
            // query with limit and filter
            QueryResp queryResp = client.query(QueryReq.builder()
                    .collectionName(randomCollectionName)
                    .filter("id > 1")
                    .limit(8)
                    .build());
            List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
            Assertions.assertEquals(8, queryResults.size());
        }

        {
            // query with ids
            QueryResp queryResp = client.query(QueryReq.builder()
                    .collectionName(randomCollectionName)
                    .ids(Arrays.asList(1, 5, 10))
                    .build());
            List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
            Assertions.assertEquals(3, queryResults.size());
        }

        {
            // query error with 0 limit and empty filter
            Assertions.assertThrows(MilvusClientException.class, () -> client.query(QueryReq.builder()
                    .collectionName(randomCollectionName)
                    .build()));
        }

        {
            // query error with ids and filter
            Assertions.assertThrows(MilvusClientException.class, () -> client.query(QueryReq.builder()
                    .collectionName(randomCollectionName)
                    .filter("id > 1")
                    .ids(Arrays.asList(1, 3, 5))
                    .build()));
        }

        {
            // query timeout
            QueryResp queryResp = client.query(QueryReq.builder()
                    .collectionName(randomCollectionName)
                    .filter("JSON_CONTAINS_ANY(json_field[\"flags\"], [4, 100])")
                    .build());
            List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
            Assertions.assertEquals(6, queryResults.size());

            // test the withTimeout works well
            client.withTimeout(1, TimeUnit.NANOSECONDS);
            Assertions.assertThrows(MilvusClientException.class, () -> client.query(QueryReq.builder()
                    .collectionName(randomCollectionName)
                    .filter("JSON_CONTAINS_ANY(json_field[\"flags\"], [4, 100])")
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build()));
        }

        client.withTimeout(0, TimeUnit.SECONDS);
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
                .dimension(DIMENSION)
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
        List<ByteBuffer> targetOriginVectors = new ArrayList<>();
        for (int i = 0; i < nq; i++) {
            JsonObject row = data.get(RANDOM.nextInt((int)count));
            targetIDs.add(row.get("id").getAsLong());
            byte[] vector = JsonUtils.fromJson(row.get(vectorFieldName), new TypeToken<byte[]>() {}.getType());
            targetOriginVectors.add(ByteBuffer.wrap(vector));
            targetVectors.add(new BinaryVec(vector));
        }
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField(vectorFieldName)
                .data(targetVectors)
                .limit(topk)
                .outputFields(Collections.singletonList(vectorFieldName))
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        Assertions.assertEquals(nq, searchResults.size());
        for (int i = 0; i < nq; i++) {
            List<SearchResp.SearchResult> results = searchResults.get(i);
            Assertions.assertEquals(topk, results.size());
            Assertions.assertEquals(targetIDs.get(i), results.get(0).getId());

            ByteBuffer buf = (ByteBuffer) results.get(0).getEntity().get(vectorFieldName);
            Assertions.assertArrayEquals(targetOriginVectors.get(i).array(), buf.array());
        }

        client.dropCollection(DropCollectionReq.builder().collectionName(randomCollectionName).build());
    }

    @Test
    void testFloat16Vectors() {
        String randomCollectionName = generator.generate(10);

        // build a collection with two vector fields
        String float16Field = "float16_vector";
        String bfloat16Field = "bfloat16_vector";
        CreateCollectionReq.CollectionSchema collectionSchema = baseSchema();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(float16Field)
                .dataType(DataType.Float16Vector)
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(bfloat16Field)
                .dataType(DataType.BFloat16Vector)
                .dimension(DIMENSION)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("nlist",64);
        indexes.add(IndexParam.builder()
                .fieldName(float16Field)
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .extraParams(extraParams)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName(bfloat16Field)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .build();
        client.createCollection(requestCreate);

        // partial load
        List<String> loadFields = new ArrayList<>();
        loadFields.add("id");
        loadFields.add(float16Field);
        loadFields.add(bfloat16Field);
        client.releaseCollection(ReleaseCollectionReq.builder().collectionName(randomCollectionName).build());
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(randomCollectionName)
                .loadFields(loadFields)
                .build());

        // insert 10000 rows
        long count = 10000;
        List<JsonObject> data = generateRandomData(collectionSchema, count);
        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(data)
                .build());
        Assertions.assertEquals(count, insertResp.getInsertCnt());

        // update one row
        long targetID = 99;
        JsonObject row = data.get((int)targetID);
        List<Float> originVector = new ArrayList<>();
        for (int i = 0; i < DIMENSION; ++i) {
            originVector.add((float)1/(i+1));
        }
//        System.out.println("Original float32 vector: " + originVector);
        row.add(float16Field, JsonUtils.toJsonTree(Float16Utils.f32VectorToFp16Buffer(originVector).array()));
        row.add(bfloat16Field, JsonUtils.toJsonTree(Float16Utils.f32VectorToBf16Buffer(originVector).array()));

        UpsertResp upsertResp = client.upsert(UpsertReq.builder()
                .collectionName(randomCollectionName)
                .data(Collections.singletonList(row))
                .build());
        Assertions.assertEquals(1L, upsertResp.getUpsertCnt());

        int topk = 10;
        // search the float16 vector field
        {
            SearchResp searchResp = client.search(SearchReq.builder()
                    .collectionName(randomCollectionName)
                    .annsField(float16Field)
                    .data(Collections.singletonList(new Float16Vec(originVector)))
                    .limit(topk)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .outputFields(Collections.singletonList(float16Field))
                    .build());
            List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
            Assertions.assertEquals(1, searchResults.size());
            List<SearchResp.SearchResult> results = searchResults.get(0);
            Assertions.assertEquals(topk, results.size());
            SearchResp.SearchResult firstResult = results.get(0);
            Assertions.assertEquals(targetID, (long) firstResult.getId());
            Map<String, Object> entity = firstResult.getEntity();
            Assertions.assertInstanceOf(ByteBuffer.class, entity.get(float16Field));
            ByteBuffer outputBuf = (ByteBuffer) entity.get(float16Field);
            List<Float> outputVector = Float16Utils.fp16BufferToVector(outputBuf);
            for (int i = 0; i < outputVector.size(); i++) {
                Assertions.assertEquals(originVector.get(i), outputVector.get(i), 0.001f);
            }
//            System.out.println("Output float16 vector: " + outputVector);
        }

        // search the bfloat16 vector field
        {
            SearchResp searchResp = client.search(SearchReq.builder()
                    .collectionName(randomCollectionName)
                    .annsField(bfloat16Field)
                    .data(Collections.singletonList(new BFloat16Vec(originVector)))
                    .limit(topk)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .outputFields(Collections.singletonList(bfloat16Field))
                    .build());
            List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
            Assertions.assertEquals(1, searchResults.size());
            List<SearchResp.SearchResult> results = searchResults.get(0);
            Assertions.assertEquals(topk, results.size());
            SearchResp.SearchResult firstResult = results.get(0);
            Assertions.assertEquals(targetID, (long) firstResult.getId());
            Map<String, Object> entity = firstResult.getEntity();
            Assertions.assertInstanceOf(ByteBuffer.class, entity.get(bfloat16Field));
            ByteBuffer outputBuf = (ByteBuffer) entity.get(bfloat16Field);
            List<Float> outputVector = Float16Utils.bf16BufferToVector(outputBuf);
            for (int i = 0; i < outputVector.size(); i++) {
                Assertions.assertEquals(originVector.get(i), outputVector.get(i), 0.01f);
            }
//            System.out.println("Output bfloat16 vector: " + outputVector);
        }

        // get row count
        long rowCount = getRowCount(randomCollectionName);
        Assertions.assertEquals(count, rowCount);

        client.dropCollection(DropCollectionReq.builder().collectionName(randomCollectionName).build());
    }

    @Test
    void testSparseVectors() {
        String randomCollectionName = generator.generate(10);

        String vectorFieldName = "binary_vector";
        CreateCollectionReq.CollectionSchema collectionSchema = baseSchema();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(vectorFieldName)
                .dataType(DataType.SparseFloatVector)
                .dimension(DIMENSION)
                .build());

        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("drop_ratio_build",0.2);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(vectorFieldName)
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.IP)
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
            SortedMap<Long, Float> vector = JsonUtils.fromJson(row.get(vectorFieldName), new TypeToken<SortedMap<Long, Float>>() {}.getType());
            targetVectors.add(new SparseFloatVec(vector));
        }
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField(vectorFieldName)
                .data(targetVectors)
                .limit(topk)
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        Assertions.assertEquals(nq, searchResults.size());
        for (int i = 0; i < nq; i++) {
            List<SearchResp.SearchResult> results = searchResults.get(i);
            Assertions.assertEquals(targetIDs.get(i), results.get(0).getId());
        }

        client.dropCollection(DropCollectionReq.builder().collectionName(randomCollectionName).build());
    }

    @Test
    void testInt8Vectors() {
        String randomCollectionName = generator.generate(10);
        String vectorFieldName = "int8_vector";
        int dimension = 8;
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(vectorFieldName)
                .dataType(DataType.Int8Vector)
                .dimension(dimension)
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());
        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .build();
        client.createCollection(requestCreate);

        // insert rows
        Gson gson = new Gson();
        Random RANDOM = new Random();
        long count = 10;
        List<ByteBuffer> vectors = new ArrayList<>();
        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);

            ByteBuffer vector = ByteBuffer.allocate(dimension);
            for (int k = 0; k < dimension; ++k) {
                vector.put((byte) (RANDOM.nextInt(256) - 128));
            }
            vectors.add(vector);
            row.add(vectorFieldName, gson.toJsonTree(vector.array()));
            data.add(row);
        }

        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(data)
                .build());
        Assertions.assertEquals(count, insertResp.getInsertCnt());

        // flush
        client.flush(FlushReq.builder()
                .collectionNames(Collections.singletonList(randomCollectionName))
                .build());

        // create index
        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("M", 64);
        extraParams.put("efConstruction", 200);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(vectorFieldName)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.COSINE)
                .extraParams(extraParams)
                .build();
        client.createIndex(CreateIndexReq.builder()
                .collectionName(randomCollectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());

        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        // describe collection
        DescribeCollectionResp descResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(randomCollectionName, descResp.getCollectionName());

        List<String> fieldNames = descResp.getFieldNames();
        Assertions.assertEquals(collectionSchema.getFieldSchemaList().size(), fieldNames.size());
        CreateCollectionReq.CollectionSchema schema = descResp.getCollectionSchema();
        for (String name : fieldNames) {
            CreateCollectionReq.FieldSchema f1 = collectionSchema.getField(name);
            CreateCollectionReq.FieldSchema f2 = schema.getField(name);
            Assertions.assertNotNull(f1);
            Assertions.assertNotNull(f2);
            Assertions.assertEquals(f1.getName(), f2.getName());
            Assertions.assertEquals(f1.getDataType(), f2.getDataType());
            Assertions.assertEquals(f1.getDimension(), f2.getDimension());
        }

        // search in collection
        int topK = 3;
        List<BaseVector> targetVectors = Arrays.asList(new Int8Vec(vectors.get(5)), new Int8Vec(vectors.get(0)));
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField(vectorFieldName)
                .data(targetVectors)
                .limit(topK)
                .outputFields(Collections.singletonList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        Assertions.assertEquals(targetVectors.size(), searchResults.size());

        for (List<SearchResp.SearchResult> results : searchResults) {
            Assertions.assertEquals(topK, results.size());
            for (int i = 0; i < results.size(); i++) {
                SearchResp.SearchResult result = results.get(i);
                Map<String, Object> entity = result.getEntity();
                long id = (long) entity.get("id");
                ByteBuffer originVec = vectors.get((int) id);
                ByteBuffer getVec = (ByteBuffer) entity.get(vectorFieldName);
                Assertions.assertEquals(originVec, getVec);
            }
        }

        // query
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(randomCollectionName)
                .filter("id == 5")
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        Assertions.assertEquals(1, queryResults.size());
        {
            QueryResp.QueryResult result = queryResults.get(0);
            Map<String, Object> entity = result.getEntity();
            ByteBuffer originVec = vectors.get(5);
            ByteBuffer getVec = (ByteBuffer)entity.get(vectorFieldName);
            Assertions.assertEquals(originVec, getVec);
        }
    }

    @Test
    void testHybridSearch() {
        String randomCollectionName = generator.generate(10);

        CreateCollectionReq.CollectionSchema collectionSchema = baseSchema();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("float_vector")
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("binary_vector")
                .dataType(DataType.BinaryVector)
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("sparse_vector")
                .dataType(DataType.SparseFloatVector)
                .dimension(DIMENSION)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName("float_vector")
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(new HashMap<String,Object>(){{put("nlist", 64);}})
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("binary_vector")
                .indexType(IndexParam.IndexType.BIN_FLAT)
                .metricType(IndexParam.MetricType.HAMMING)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("sparse_vector")
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.IP)
                .extraParams(new HashMap<String,Object>(){{put("drop_ratio_build", 0.1);}})
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(indexParams)
                .build();
        client.createCollection(requestCreate);

        DescribeCollectionResp descResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(16, descResp.getFieldNames().size());
        Assertions.assertEquals(3, descResp.getVectorFieldNames().size());

        // prepare sub requests
        int nq = 5;
        int topk = 10;
        Function<Map<String, Object>, HybridSearchReq> genRequestFunc =
                config -> {
                    List<BaseVector> floatVectors = new ArrayList<>();
                    List<BaseVector> binaryVectors = new ArrayList<>();
                    List<BaseVector> sparseVectors = new ArrayList<>();
                    for (int i = 0; i < nq; i++) {
                        floatVectors.add(new FloatVec(utils.generateFloatVector()));
                        binaryVectors.add(new BinaryVec(utils.generateBinaryVector()));
                    }
                    int sparseCount = (Integer)config.get("sparseCount");
                    for (int i = 0; i < sparseCount; i++) {
                        sparseVectors.add(new SparseFloatVec(utils.generateSparseVector()));
                    }

                    List<AnnSearchReq> searchRequests = new ArrayList<>();
                    searchRequests.add(AnnSearchReq.builder()
                            .vectorFieldName("float_vector")
                            .vectors(floatVectors)
                            .params("{\"nprobe\": 10}")
                            .limit(15)
                            .build());
                    searchRequests.add(AnnSearchReq.builder()
                            .vectorFieldName("binary_vector")
                            .vectors(binaryVectors)
                            .limit(5)
                            .build());
                    searchRequests.add(AnnSearchReq.builder()
                            .vectorFieldName("sparse_vector")
                            .vectors(sparseVectors)
                            .limit(7)
                            .build());

                    CreateCollectionReq.Function ranker = WeightedRanker.builder().weights(Arrays.asList(0.2f, 0.5f, 0.6f)).build();
                    boolean useFunctionScore = (Boolean)config.get("useFunctionScore");
                    if (useFunctionScore) {
                        return HybridSearchReq.builder()
                                .collectionName(randomCollectionName)
                                .searchRequests(searchRequests)
                                .functionScore(FunctionScore.builder().addFunction(ranker).build())
                                .limit(topk)
                                .consistencyLevel(ConsistencyLevel.BOUNDED)
                                .build();
                    } else {
                        return HybridSearchReq.builder()
                                .collectionName(randomCollectionName)
                                .searchRequests(searchRequests)
                                .ranker(RRFRanker.builder().k(20).build())
                                .limit(topk)
                                .consistencyLevel(ConsistencyLevel.BOUNDED)
                                .build();
                    }
        };

        Map<String, Object> config = new HashMap<>();
        config.put("sparseCount", 0);
        config.put("useFunctionScore", false);
        // search with an empty nq, return error
        Assertions.assertThrows(MilvusClientException.class, ()->client.hybridSearch(genRequestFunc.apply(config)));

        // unequal nq, return error
        config.put("sparseCount", 1);
        Assertions.assertThrows(MilvusClientException.class, ()->client.hybridSearch(genRequestFunc.apply(config)));

        // search on empty collection, no result returned
        config.put("sparseCount", nq);
        SearchResp searchResp = client.hybridSearch(genRequestFunc.apply(config));
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        Assertions.assertEquals(nq, searchResults.size());
        for (List<SearchResp.SearchResult> result : searchResults) {
            Assertions.assertTrue(result.isEmpty());
        }

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

        // search again, there are results
        config.put("useFunctionScore", true);
        searchResp = client.hybridSearch(genRequestFunc.apply(config));
        searchResults = searchResp.getSearchResults();
        Assertions.assertEquals(nq, searchResults.size());
        for (int i = 0; i < nq; i++) {
            List<SearchResp.SearchResult> results = searchResults.get(i);
            Assertions.assertEquals(topk, results.size());
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
        for (int i = 0; i < 10; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("pk", String.format("pk_%d", i));
            row.add("float_vector", JsonUtils.toJsonTree(new float[]{(float)i, (float)(i + 1), (float)(i + 2), (float)(i + 3)}));
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
        row1.add("float_vector", JsonUtils.toJsonTree(new float[]{5.0f, 5.0f, 5.0f, 5.0f}));
        dataUpdate.add(row1);
        JsonObject row2 = new JsonObject();
        row2.addProperty("pk", "pk_2");
        row2.add("float_vector", JsonUtils.toJsonTree(new float[]{2.0f, 2.0f, 2.0f, 2.0f}));
        dataUpdate.add(row2);
        UpsertResp upsertResp = client.upsert(UpsertReq.builder()
                .collectionName(randomCollectionName)
                .data(dataUpdate)
                .build());
        Assertions.assertEquals(2, upsertResp.getUpsertCnt());
        Assertions.assertEquals(2, upsertResp.getPrimaryKeys().size());

        // get row count
        rowCount = getRowCount(randomCollectionName);
        Assertions.assertEquals(9L, rowCount);

        // verify
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(randomCollectionName)
                .ids(Arrays.asList("pk_2", "pk_5"))
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

        // must drop or alter alias before dropping the collection
        client.alterAlias(AlterAliasReq.builder()
                .collectionName("AAA")
                .alias("CCC")
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName("BBB")
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
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("name")
                .dataType(DataType.VarChar)
                .maxLength(100)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        Map<String,Object> extra = new HashMap<>();
        extra.put("M",8);
        extra.put("efConstruction",64);
        indexes.add(IndexParam.builder()
                .fieldName("vector")
                .indexName("abc")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.COSINE)
                .extraParams(extra)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName("name")
                .indexType(IndexParam.IndexType.TRIE)
                .build());
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .property(Constant.TTL_SECONDS, "5")
                .property(Constant.MMAP_ENABLED, "false")
                .build());

        DescribeCollectionResp descCollResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());
        Map<String, String> collProps = descCollResp.getProperties();
        Assertions.assertTrue(collProps.containsKey(Constant.TTL_SECONDS));
        Assertions.assertTrue(collProps.containsKey(Constant.MMAP_ENABLED));
        Assertions.assertEquals("5", collProps.get(Constant.TTL_SECONDS));
        Assertions.assertEquals("false", collProps.get(Constant.MMAP_ENABLED));

        client.releaseCollection(ReleaseCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        // alter field properties
        client.alterCollectionField(AlterCollectionFieldReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("name")
                .property("max_length", "9")
                .build());

        // collection alter properties
        Map<String, String> properties = new HashMap<>();
        properties.put(Constant.TTL_SECONDS, "10");
        properties.put(Constant.MMAP_ENABLED, "true");
        client.alterCollectionProperties(AlterCollectionPropertiesReq.builder()
                .collectionName(randomCollectionName)
                .properties(properties)
                .property("prop", "val")
                .build());
        descCollResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());
        collProps = descCollResp.getProperties();
        Assertions.assertTrue(collProps.containsKey(Constant.TTL_SECONDS));
        Assertions.assertTrue(collProps.containsKey(Constant.MMAP_ENABLED));
        Assertions.assertTrue(collProps.containsKey("prop"));
        Assertions.assertEquals("10", collProps.get(Constant.TTL_SECONDS));
        Assertions.assertEquals("true", collProps.get(Constant.MMAP_ENABLED));
        Assertions.assertEquals("val", collProps.get("prop"));

        CreateCollectionReq.FieldSchema fieldScheam = descCollResp.getCollectionSchema().getField("name");
        Assertions.assertEquals(9, fieldScheam.getMaxLength());

        client.dropCollectionProperties(DropCollectionPropertiesReq.builder()
                .collectionName(randomCollectionName)
                .propertyKeys(Collections.singletonList("prop"))
                .build());
        descCollResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());
        collProps = descCollResp.getProperties();
        Assertions.assertFalse(collProps.containsKey("prop"));

        // list indexes
        List<String> names = client.listIndexes(ListIndexesReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("vector")
                .build());
        Assertions.assertEquals(1, names.size());
        Assertions.assertEquals("abc", names.get(0));

        names = client.listIndexes(ListIndexesReq.builder()
                .collectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(2, names.size());

        // describe scalar index
        DescribeIndexResp descResp = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("name")
                .build());
        DescribeIndexResp.IndexDesc desc = descResp.getIndexDescByFieldName("name");
        Assertions.assertEquals(IndexParam.IndexType.TRIE, desc.getIndexType());

        // index alter properties
        descResp = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("vector")
                .build());
        desc = descResp.getIndexDescByFieldName("vector");
        Assertions.assertEquals("vector", desc.getFieldName());
        Assertions.assertFalse(desc.getIndexName().isEmpty());
        Assertions.assertEquals(IndexParam.IndexType.HNSW, desc.getIndexType());
        Map<String, String> extraParams = desc.getExtraParams();
        Assertions.assertTrue(extraParams.containsKey("M"));
        Assertions.assertEquals("8", extraParams.get("M"));
        Assertions.assertTrue(extraParams.containsKey("efConstruction"));
        Assertions.assertEquals("64", extraParams.get("efConstruction"));

        properties.clear();
        properties.put(Constant.MMAP_ENABLED, "false");
        client.alterIndexProperties(AlterIndexPropertiesReq.builder()
                .collectionName(randomCollectionName)
                .indexName(desc.getIndexName())
                .properties(properties)
                .build());

        descResp = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("vector")
                .build());
        desc = descResp.getIndexDescByFieldName("vector");
        Map<String, String> indexProps = desc.getProperties();
        Assertions.assertTrue(indexProps.containsKey(Constant.MMAP_ENABLED));
        Assertions.assertEquals("false", indexProps.get(Constant.MMAP_ENABLED));
        extraParams = desc.getExtraParams();
        Assertions.assertTrue(extraParams.containsKey(Constant.MMAP_ENABLED));
        Assertions.assertEquals("false", extraParams.get(Constant.MMAP_ENABLED));

        client.dropIndexProperties(DropIndexPropertiesReq.builder()
                .collectionName(randomCollectionName)
                .indexName(desc.getIndexName())
                .propertyKeys(Collections.singletonList(Constant.MMAP_ENABLED))
                .build());
        descResp = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("vector")
                .build());
        desc = descResp.getIndexDescByFieldName("vector");
        indexProps = desc.getProperties();
        Assertions.assertFalse(indexProps.containsKey(Constant.MMAP_ENABLED));
        extraParams = desc.getExtraParams();
        Assertions.assertFalse(extraParams.containsKey(Constant.MMAP_ENABLED));

        // drop index
        client.dropIndex(DropIndexReq.builder()
                .collectionName(randomCollectionName)
                .indexName("abc")
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

    private static void createSimpleCollection(MilvusClientV2 client, String dbName, String collName, String pkName, boolean autoID,
                                               int dimension, ConsistencyLevel level) {
        client.dropCollection(DropCollectionReq.builder()
                .databaseName(dbName)
                .collectionName(collName)
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .databaseName(dbName)
                .collectionName(collName)
                .autoID(autoID)
                .primaryFieldName(pkName)
                .dimension(dimension)
                .consistencyLevel(level)
                .enableDynamicField(false)
                .build());
    }

    @Test
    void testCacheCollectionSchema() throws InterruptedException {
        String randomCollectionName = generator.generate(10);

        // create a new db
        String testDbName = "test_database";
        client.createDatabase(CreateDatabaseReq.builder()
                .databaseName(testDbName)
                .build());

        // create a collection in the default db
        createSimpleCollection(client, "", randomCollectionName, "pk", false, DIMENSION, ConsistencyLevel.BOUNDED);

        // a temp client connect to the new db
        ConnectConfig config = ConnectConfig.builder()
                .uri(milvus.getEndpoint())
                .dbName(testDbName)
                .build();
        // fix tempClient not close
        MilvusClientV2 tempClient = null;
        try {
            tempClient = new MilvusClientV2(config);

            // use the temp client to insert correct data into the default collection
            // there will be a schema cache for this collection in the temp client
            // there will be timestamp for this collection in the global GTsDict
            JsonObject row = new JsonObject();
            row.addProperty("pk", 8);
            row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(DIMENSION)));
            InsertResp insertResp = tempClient.insert(
                    InsertReq.builder().databaseName("default").collectionName(randomCollectionName)
                            .data(Collections.singletonList(row)).build());
            Assertions.assertEquals(1L, insertResp.getInsertCnt());

        // check the timestamp of this collection, must be positive
        String key1 = GTsDict.CombineCollectionName("default", randomCollectionName);
        Long ts11 = GTsDict.getInstance().getCollectionTs(key1);
        Assertions.assertNotNull(ts11);
        Assertions.assertTrue(ts11 > 0L);

        // insert wrong data, the schema cache will be removed
        row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(7)));
        Assertions.assertThrows(MilvusClientException.class, ()->client.insert(InsertReq.builder()
                .databaseName("default")
                .collectionName(randomCollectionName)
                .data(Collections.singletonList(row))
                .build()));

        // use the default client to do upsert correct data
        TimeUnit.MILLISECONDS.sleep(100);
        row.addProperty("pk", 999);
        row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(DIMENSION)));
        UpsertResp upsertResp = client.upsert(UpsertReq.builder()
                .collectionName(randomCollectionName)
                .data(Collections.singletonList(row))
                .build());
        Assertions.assertEquals(1L, upsertResp.getUpsertCnt());

        // check the timestamp of this collection, must be a new positive
        Long ts12 = GTsDict.getInstance().getCollectionTs(key1);
        Assertions.assertNotNull(ts12);
        Assertions.assertTrue(ts12 > ts11);

        // create a new collection with the same name, different schema, in the test db
        createSimpleCollection(tempClient, "", randomCollectionName, "aaa", false, 4, ConsistencyLevel.BOUNDED);

        // use the temp client to insert wrong data, wrong dimension
        row.addProperty("aaa", 22);
        row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(7)));
            MilvusClientV2 finalTempClient = tempClient;
            Assertions.assertThrows(MilvusClientException.class, ()-> finalTempClient.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(Collections.singletonList(row))
                .build()));

        // check the timestamp of this collection, must be null
        String key2 = GTsDict.CombineCollectionName(testDbName, randomCollectionName);
        Long ts21 = GTsDict.getInstance().getCollectionTs(key2);
        Assertions.assertNull(ts21);

        // use the temp client to do upsert correct data
        TimeUnit.MILLISECONDS.sleep(100);
        row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(4)));
        upsertResp = tempClient.upsert(UpsertReq.builder()
                .collectionName(randomCollectionName)
                .data(Collections.singletonList(row))
                .build());
        Assertions.assertEquals(1L, upsertResp.getUpsertCnt());

        // check the timestamp of this collection, must be positive
        Long ts22 = GTsDict.getInstance().getCollectionTs(key2);
        Assertions.assertNotNull(ts22);
        Assertions.assertTrue(ts22 > 0L);

        // tempClient delete data
        tempClient.delete(DeleteReq.builder()
                .collectionName(randomCollectionName)
                .ids(Collections.singletonList(22L))
                .build());

        // check the timestamp of this collection, must be greater than previous
        Long ts23 = GTsDict.getInstance().getCollectionTs(key2);
        Assertions.assertNotNull(ts23);
        Assertions.assertTrue(ts23 > ts22);

        // use the default client to drop the collection in the new db
        client.dropCollection(DropCollectionReq.builder()
                .databaseName(testDbName)
                .collectionName(randomCollectionName)
                .build());

            // check the timestamp of this collection, must be deleted
            Long ts31 = GTsDict.getInstance().getCollectionTs(key2);
            Assertions.assertNull(ts31);
        } finally {
            if (tempClient != null) {
                tempClient.close();
            }
        }
    }

    @Test
    public void testIterator() {
        String randomCollectionName = generator.generate(10);
        CreateCollectionReq.CollectionSchema collectionSchema = baseSchema();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("float_vector")
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("binary_vector")
                .dataType(DataType.BinaryVector)
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("sparse_vector")
                .dataType(DataType.SparseFloatVector)
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("bfloat16_vector")
                .dataType(DataType.BFloat16Vector)
                .dimension(DIMENSION)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName("float_vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("binary_vector")
                .indexType(IndexParam.IndexType.BIN_FLAT)
                .metricType(IndexParam.MetricType.HAMMING)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("sparse_vector")
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.IP)
                .extraParams(new HashMap<String,Object>(){{put("drop_ratio_build", 0.1);}})
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("bfloat16_vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(indexParams)
                .build();
        client.createCollection(requestCreate);

        // insert rows
        long count = 20000;
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
                .vectors(Collections.singletonList(new FloatVec(utils.generateFloatVector())))
                .expr("int64_field > 500 && int64_field < 1000")
                .params("{\"range_filter\": 5.0, \"radius\": 50.0}")
                .limit(1000)
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
                Assertions.assertInstanceOf(ByteBuffer.class, record.get("binary_vector"));
                Assertions.assertInstanceOf(ByteBuffer.class, record.get("bfloat16_vector"));
                Assertions.assertInstanceOf(SortedMap.class, record.get("sparse_vector"));

                long int64Val = (long)record.get("int64_field");
                Assertions.assertTrue(int64Val > 500L && int64Val < 1000L);

                String varcharVal = (String)record.get("varchar_field");
                Assertions.assertTrue(varcharVal.startsWith("varchar_"));

                JsonObject jsonObj = (JsonObject)record.get("json_field");
                Assertions.assertTrue(jsonObj.has(String.format("JSON_%d", int64Val)));

                List<Integer> intArr = (List<Integer>)record.get("arr_int_field");
                Assertions.assertTrue(intArr.size() <= 50); // max capacity 50 is defined in the baseSchema()

                List<Float> floatVector = (List<Float>)record.get("float_vector");
                Assertions.assertEquals(DIMENSION, floatVector.size());

                ByteBuffer binaryVector = (ByteBuffer)record.get("binary_vector");
                Assertions.assertEquals(DIMENSION, binaryVector.limit()*8);

                ByteBuffer bfloat16Vector = (ByteBuffer)record.get("bfloat16_vector");
                Assertions.assertEquals(DIMENSION*2, bfloat16Vector.limit());

                SortedMap<Long, Float> sparseVector = (SortedMap<Long, Float>)record.get("sparse_vector");
                Assertions.assertTrue(sparseVector.size() >= 10 && sparseVector.size() < 20); // defined in generateSparseVector()

                counter++;
            }
        }
        System.out.println(String.format("There are %d items match score between [5.0, 50.0]", counter));
        Assertions.assertTrue(counter > 0);

        // query iterator
        long from = 17777;
        long to = 18000;
        QueryIterator queryIterator = client.queryIterator(QueryIteratorReq.builder()
                .collectionName(randomCollectionName)
                .expr("int64_field < " + String.valueOf(to))
                .outputFields(Lists.newArrayList("*"))
                .batchSize(50L)
                .offset(from)
                .limit(4000)
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
                Assertions.assertInstanceOf(Long.class, record.get("id"));
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
                Assertions.assertInstanceOf(ByteBuffer.class, record.get("binary_vector"));
                Assertions.assertInstanceOf(ByteBuffer.class, record.get("bfloat16_vector"));
                Assertions.assertInstanceOf(SortedMap.class, record.get("sparse_vector"));

                long int64Val = (long)record.get("id");
                Assertions.assertTrue(int64Val >= from);
                Assertions.assertTrue(int64Val < to);

                String varcharVal = (String)record.get("varchar_field");
                Assertions.assertTrue(varcharVal.startsWith("varchar_"));

                JsonObject jsonObj = (JsonObject)record.get("json_field");
                Assertions.assertTrue(jsonObj.has(String.format("JSON_%d", int64Val)));

                List<Integer> intArr = (List<Integer>)record.get("arr_int_field");
                Assertions.assertTrue(intArr.size() <= 50); // max capacity 50 is defined in the baseSchema()

                List<Float> floatVector = (List<Float>)record.get("float_vector");
                Assertions.assertEquals(DIMENSION, floatVector.size());

                ByteBuffer binaryVector = (ByteBuffer)record.get("binary_vector");
                Assertions.assertEquals(DIMENSION, binaryVector.limit()*8);

                ByteBuffer bfloat16Vector = (ByteBuffer)record.get("bfloat16_vector");
                Assertions.assertEquals(DIMENSION*2, bfloat16Vector.limit());

                SortedMap<Long, Float> sparseVector = (SortedMap<Long, Float>)record.get("sparse_vector");
                Assertions.assertTrue(sparseVector.size() >= 10 && sparseVector.size() < 20); // defined in generateSparseVector()

                counter++;
            }
        }
        Assertions.assertEquals(to - from, counter);

        // search iterator V2
        SearchIteratorV2 searchIteratorV2 = client.searchIteratorV2(SearchIteratorReqV2.builder()
                .collectionName(randomCollectionName)
                .outputFields(Lists.newArrayList("*"))
                .batchSize(1000L)
                .vectorFieldName("float_vector")
                .filter("id >= 50")
                .vectors(Collections.singletonList(new FloatVec(utils.generateFloatVector())))
                .metricType(IndexParam.MetricType.L2)
                .consistencyLevel(ConsistencyLevel.EVENTUALLY)
                .build());
        counter = 0;
        while (true) {
            List<SearchResp.SearchResult> res = searchIteratorV2.next();
            if (res.isEmpty()) {
                System.out.println("search iteration finished, close");
                searchIteratorV2.close();
                break;
            }

            for (SearchResp.SearchResult record : res) {
                Map<String, Object> entity = record.getEntity();
                Assertions.assertInstanceOf(Boolean.class, entity.get("bool_field"));
                Assertions.assertInstanceOf(Integer.class, entity.get("int8_field"));
                Assertions.assertInstanceOf(Integer.class, entity.get("int16_field"));
                Assertions.assertInstanceOf(Integer.class, entity.get("int32_field"));
                Assertions.assertInstanceOf(Long.class, entity.get("int64_field"));
                Assertions.assertInstanceOf(Float.class, entity.get("float_field"));
                Assertions.assertInstanceOf(Double.class, entity.get("double_field"));
                Assertions.assertInstanceOf(String.class, entity.get("varchar_field"));
                Assertions.assertInstanceOf(JsonObject.class, entity.get("json_field"));
                Assertions.assertInstanceOf(List.class, entity.get("arr_int_field"));
                Assertions.assertInstanceOf(List.class, entity.get("float_vector"));
                Assertions.assertInstanceOf(ByteBuffer.class, entity.get("binary_vector"));
                Assertions.assertInstanceOf(ByteBuffer.class, entity.get("bfloat16_vector"));
                Assertions.assertInstanceOf(SortedMap.class, entity.get("sparse_vector"));

                String varcharVal = (String)entity.get("varchar_field");
                Assertions.assertTrue(varcharVal.startsWith("varchar_"));

                long int64Val = (long)entity.get("int64_field");
                Assertions.assertEquals(int64Val, (long)record.getId());
                JsonObject jsonObj = (JsonObject)entity.get("json_field");
                Assertions.assertTrue(jsonObj.has(String.format("JSON_%d", int64Val)));

                List<Integer> intArr = (List<Integer>)entity.get("arr_int_field");
                Assertions.assertTrue(intArr.size() <= 50); // max capacity 50 is defined in the baseSchema()

                List<Float> floatVector = (List<Float>)entity.get("float_vector");
                Assertions.assertEquals(DIMENSION, floatVector.size());

                ByteBuffer binaryVector = (ByteBuffer)entity.get("binary_vector");
                Assertions.assertEquals(DIMENSION, binaryVector.limit()*8);

                ByteBuffer bfloat16Vector = (ByteBuffer)entity.get("bfloat16_vector");
                Assertions.assertEquals(DIMENSION*2, bfloat16Vector.limit());

                SortedMap<Long, Float> sparseVector = (SortedMap<Long, Float>)entity.get("sparse_vector");
                Assertions.assertTrue(sparseVector.size() >= 10 && sparseVector.size() < 20); // defined in generateSparseVector()

                counter++;
            }
        }
        // search iterator could not ensure that all the entities can be retrieved
        // expect count is 9950, but sometimes it returns 9949 or 9948
        Assertions.assertTrue(counter > ((int)count - 55) && counter <= ((int)count - 50));

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
        Map<String, String> properties = new HashMap<>();
        properties.put(Constant.DATABASE_REPLICA_NUMBER, "5");
        CreateDatabaseReq createDatabaseReq = CreateDatabaseReq.builder()
                .databaseName(tempDatabaseName)
                .properties(properties)
                .build();
        client.createDatabase(createDatabaseReq);

        listDatabasesResp = client.listDatabases();
        dbNames = listDatabasesResp.getDatabaseNames();
        Assertions.assertTrue(dbNames.contains(tempDatabaseName));

        DescribeDatabaseResp descDBResp = client.describeDatabase(DescribeDatabaseReq.builder()
                .databaseName(tempDatabaseName)
                .build());
        Map<String,String> propertiesResp = descDBResp.getProperties();
        Assertions.assertTrue(propertiesResp.containsKey(Constant.DATABASE_REPLICA_NUMBER));
        Assertions.assertEquals("5", propertiesResp.get(Constant.DATABASE_REPLICA_NUMBER));

        // alter the database
        properties.put(Constant.DATABASE_REPLICA_NUMBER, "10");
        client.alterDatabaseProperties(AlterDatabasePropertiesReq.builder()
                .databaseName(tempDatabaseName)
                .properties(properties)
                .property("prop", "val")
                .build());
        descDBResp = client.describeDatabase(DescribeDatabaseReq.builder()
                .databaseName(tempDatabaseName)
                .build());
        propertiesResp = descDBResp.getProperties();
        Assertions.assertTrue(propertiesResp.containsKey(Constant.DATABASE_REPLICA_NUMBER));
        Assertions.assertEquals("10", propertiesResp.get(Constant.DATABASE_REPLICA_NUMBER));
        Assertions.assertTrue(propertiesResp.containsKey("prop"));
        Assertions.assertEquals("val", propertiesResp.get("prop"));

        // drop property
        client.dropDatabaseProperties(DropDatabasePropertiesReq.builder()
                .databaseName(tempDatabaseName)
                .propertyKeys(Collections.singletonList("prop"))
                .build());
        descDBResp = client.describeDatabase(DescribeDatabaseReq.builder()
                .databaseName(tempDatabaseName)
                .build());
        propertiesResp = descDBResp.getProperties();
        Assertions.assertFalse(propertiesResp.containsKey("prop"));

        // switch to the new database
        Assertions.assertDoesNotThrow(()->client.useDatabase(tempDatabaseName));

        // create a collection in the new database
        String randomCollectionName = generator.generate(10);
        String vectorFieldName = "float_vector";
        CreateCollectionReq.CollectionSchema collectionSchema = baseSchema();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(vectorFieldName)
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
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
                            List<Float> vector = utils.generateFloatVector(dim);
                            obj.add("vector", JsonUtils.toJsonTree(vector));
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
                            List<Float> vector = utils.generateFloatVector(dim);
                            obj.add("vector", JsonUtils.toJsonTree(vector));
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

    @Test
    void testNullableAndDefaultValue() {
        String randomCollectionName = generator.generate(10);
        int dim = 4;

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.FALSE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(dim)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("flag")
                .dataType(DataType.Int32)
                .isNullable(true)
                .defaultValue((int)10)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("desc")
                .dataType(DataType.VarChar)
                .isNullable(Boolean.TRUE)
                .maxLength(100)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("arr")
                .dataType(DataType.Array)
                .elementType(DataType.Int32)
                .isNullable(Boolean.TRUE)
                .maxCapacity(100)
                .build());

        Assertions.assertThrows(MilvusClientException.class, () ->
                collectionSchema.addField(AddFieldReq.builder()
                        .fieldName("illegal")
                        .dataType(DataType.Bool)
                        .isNullable(false)
                        .defaultValue(null)
                        .build())
        );

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

        // insert by row-based
        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            JsonObject row = new JsonObject();
            List<Float> vector = utils.generateFloatVector(dim);
            row.addProperty("id", i);
            row.add("vector", JsonUtils.toJsonTree(vector));
            if (i%2 == 0) {
                row.addProperty("flag", i);
                row.add("desc", JsonNull.INSTANCE);
            } else {
                if (i == 5) {
                    row.add("flag", JsonNull.INSTANCE); // both null or unset will use the default value
                }
                row.addProperty("desc", "AAA");

                List<Integer> arr = Arrays.asList(5, 6);
                row.add("arr", JsonUtils.toJsonTree(arr));
            }

            data.add(row);
        }

        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(data)
                .build());
        Assertions.assertEquals(10, insertResp.getInsertCnt());

        Function<Map<String, Object>, Void> checkFunc =
                entity -> {
                    long id = (long)entity.get("id");
                    if (id%2 == 0) {
                        Assertions.assertEquals((int)id, entity.get("flag"));
                        Assertions.assertNull(entity.get("desc"));
                        Assertions.assertNull(entity.get("arr"));
                    } else {
                        Assertions.assertEquals(10, entity.get("flag"));
                        Assertions.assertEquals("AAA", entity.get("desc"));
                        Object obj = entity.get("arr");
                        Assertions.assertInstanceOf(List.class, obj);
                        List<Integer> arr = (List<Integer>)obj;
                        Assertions.assertEquals(2, arr.size());
                        Assertions.assertEquals(5, arr.get(0));
                        Assertions.assertEquals(6, arr.get(1));
                    }
                    return null;
                };
        // query
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(randomCollectionName)
                .filter("id >= 0")
                .outputFields(Arrays.asList("desc", "flag", "arr"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        Assertions.assertEquals(10, queryResults.size());
        System.out.println("Query results:");
        for (QueryResp.QueryResult result : queryResults) {
            Map<String, Object> entity = result.getEntity();
            checkFunc.apply(entity);
            System.out.println(result);
        }

        // search
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField("vector")
                .data(Collections.singletonList(new FloatVec(utils.generateFloatVector(dim))))
                .limit(10)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        Assertions.assertEquals(1, searchResults.size());
        List<SearchResp.SearchResult> firstResults = searchResults.get(0);
        Assertions.assertEquals(10, firstResults.size());
//        System.out.println("Search results:");
        for (SearchResp.SearchResult result : firstResults) {
            long id = (long)result.getId();
            Map<String, Object> entity = result.getEntity();
            checkFunc.apply(entity);
//            System.out.println(result);
        }
    }

    @Test
    void testDocInOut() {
        String randomCollectionName = generator.generate(10);

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.FALSE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("dense")
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("sparse")
                .dataType(DataType.SparseFloatVector)
                .build());
        Map<String, Object> analyzerParams = new HashMap<>();
        analyzerParams.put("tokenizer", "standard");
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("text")
                .dataType(DataType.VarChar)
                .maxLength(100)
                .enableAnalyzer(true)
                .enableMatch(true)
                .analyzerParams(analyzerParams)
                .build());

        collectionSchema.addFunction(CreateCollectionReq.Function.builder()
                .name("bm25")
                .description("desc bm25")
                .functionType(FunctionType.BM25)
                .inputFieldNames(Collections.singletonList("text"))
                .outputFieldNames(Collections.singletonList("sparse"))
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName("dense")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("sparse")
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.BM25)
                .extraParams(new HashMap<String,Object>(){{put("drop_ratio_build", 0.1);}})
                .build());
        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(indexParams)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");

        // check the schema
        DescribeCollectionResp descResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        CreateCollectionReq.CollectionSchema collSchema = descResp.getCollectionSchema();
        CreateCollectionReq.FieldSchema fieldSchema = collSchema.getField("text");
        Assertions.assertNotNull(fieldSchema);
        Assertions.assertTrue(fieldSchema.getEnableAnalyzer());
        Assertions.assertTrue(fieldSchema.getEnableAnalyzer());
        Map<String, Object> params = fieldSchema.getAnalyzerParams();
        Assertions.assertTrue(params.containsKey("tokenizer"));
        Assertions.assertEquals("standard", params.get("tokenizer"));

        List<CreateCollectionReq.Function> functions = collSchema.getFunctionList();
        Assertions.assertEquals(1, functions.size());
        Assertions.assertEquals("bm25", functions.get(0).getName());
        Assertions.assertEquals("desc bm25", functions.get(0).getDescription());
        Assertions.assertEquals(FunctionType.BM25, functions.get(0).getFunctionType());
        Assertions.assertEquals(1, functions.get(0).getInputFieldNames().size());
        Assertions.assertEquals("text", functions.get(0).getInputFieldNames().get(0));
        Assertions.assertEquals(1, functions.get(0).getOutputFieldNames().size());
        Assertions.assertEquals("sparse", functions.get(0).getOutputFieldNames().get(0));

        // insert by row-based
        List<String> texts = Arrays.asList(
                "this is a AI world",
                "milvus is a vector database for AI application",
                "hello zilliz");
        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < texts.size(); i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            row.add("dense", JsonUtils.toJsonTree(utils.generateFloatVector(DIMENSION)));
            row.addProperty("text", texts.get(i));
            data.add(row);
        }

        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(data)
                .build());
        Assertions.assertEquals(3, insertResp.getInsertCnt());

        // get row count
        long rowCount = getRowCount(randomCollectionName);
        Assertions.assertEquals(texts.size(), rowCount);

        // search
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField("sparse")
                .data(Collections.singletonList(new EmbeddedText("milvus AI")))
                .limit(10)
                .outputFields(Lists.newArrayList("*"))
                .metricType(IndexParam.MetricType.BM25)
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        Assertions.assertEquals(1, searchResults.size());
        List<SearchResp.SearchResult> firstResults = searchResults.get(0);
        Assertions.assertEquals(2, firstResults.size());
        SearchResp.SearchResult firstRes = firstResults.get(0);
        Map<String, Object> entity = firstRes.getEntity();
        Assertions.assertEquals(1L, entity.get("id"));
        Assertions.assertEquals(texts.get(1), entity.get("text"));
        System.out.println("Search results:");
        for (SearchResp.SearchResult result : firstResults) {
            System.out.println(result);
        }
    }

    @Test
    void testDynamicField() {
        String collectionName = generator.generate(10);

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .dimension(DIMENSION)
                .build());

        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        for (int i = 0; i < 100; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            row.add("vector", gson.toJsonTree(utils.generateFloatVector()));
            row.addProperty(String.format("dynamic_%d", i), "this is dynamic value"); // this value is stored in dynamic field
            rows.add(row);
        }
        InsertResp insertR = client.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(rows)
                .build());

        // query
        QueryResp countR = client.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter("")
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assertions.assertEquals(100L, (long)countR.getQueryResults().get(0).getEntity().get("count(*)"));

        GetResp getR = client.get(GetReq.builder()
                .collectionName(collectionName)
                .ids(Collections.singletonList(50L))
                .outputFields(Collections.singletonList("*"))
                .build());
        Assertions.assertEquals(1, getR.getGetResults().size());
        QueryResp.QueryResult queryR = getR.getGetResults().get(0);
        Assertions.assertTrue(queryR.getEntity().containsKey("dynamic_50"));
        Assertions.assertEquals("this is dynamic value", queryR.getEntity().get("dynamic_50"));

        // search
        SearchResp searchR = client.search(SearchReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(new FloatVec(utils.generateFloatVector())))
                .filter("id == 10")
                .topK(10)
                .outputFields(Collections.singletonList("dynamic_10"))
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchR.getSearchResults();
        Assertions.assertEquals(1, searchResults.size());
        Assertions.assertEquals(1, searchResults.get(0).size());
        SearchResp.SearchResult r = searchResults.get(0).get(0);
        Assertions.assertTrue(r.getEntity().containsKey("dynamic_10"));
        Assertions.assertEquals("this is dynamic value", r.getEntity().get("dynamic_10"));

        // add new field
        client.addCollectionField(AddCollectionFieldReq.builder()
                .collectionName(collectionName)
                .fieldName("text")
                .dataType(DataType.VarChar)
                .maxLength(100)
                .isNullable(true) // must be nullable
                .build());
        client.addCollectionField(AddCollectionFieldReq.builder()
                .collectionName(collectionName)
                .fieldName("flag")
                .dataType(DataType.Int32)
                .defaultValue(100)
                .isNullable(true) // must be nullable
                .build());

        DescribeCollectionResp descResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionName)
                .build());
        Assertions.assertEquals(4, descResp.getFieldNames().size());
        List<String> fieldNames = descResp.getFieldNames();
        Assertions.assertTrue(fieldNames.contains("text"));
        Assertions.assertTrue(fieldNames.contains("flag"));
        CreateCollectionReq.CollectionSchema schema = descResp.getCollectionSchema();

        CreateCollectionReq.FieldSchema field = schema.getField("text");
        Assertions.assertEquals(DataType.VarChar, field.getDataType());
        Assertions.assertEquals(100, field.getMaxLength());
        Assertions.assertTrue(field.getIsNullable());
    }

    @Test
    void testRBAC() {
        client.createPrivilegeGroup(CreatePrivilegeGroupReq.builder()
                .groupName("dummy")
                .build());
        client.addPrivilegesToGroup(AddPrivilegesToGroupReq.builder()
                .groupName("dummy")
                .privileges(Collections.singletonList("CreateCollection"))
                .build());

        ListPrivilegeGroupsResp resp = client.listPrivilegeGroups(ListPrivilegeGroupsReq.builder().build());
        List<PrivilegeGroup> groups = resp.getPrivilegeGroups();
        Map<String, List<String>> groupsPlivileges = new HashMap<>();
        for (PrivilegeGroup group : groups) {
            groupsPlivileges.put(group.getGroupName(), group.getPrivileges());
        }
        Assertions.assertTrue(groupsPlivileges.containsKey("dummy"));
        Assertions.assertEquals(1, groupsPlivileges.get("dummy").size());
        Assertions.assertEquals("CreateCollection", groupsPlivileges.get("dummy").get(0));
    }

    @Test
    void testResourceGroup() {
        String groupA = "group_A";
        String groupDefault = "__default_resource_group";
        client.createResourceGroup(CreateResourceGroupReq.builder()
                .groupName(groupA)
                .config(ResourceGroupConfig.newBuilder()
                        .withRequests(new ResourceGroupLimit(3))
                        .withLimits(new ResourceGroupLimit(4))
                        .withFrom(Collections.singletonList(new ResourceGroupTransfer(groupDefault)))
                        .withTo(Collections.singletonList(new ResourceGroupTransfer(groupDefault)))
                        .build())
                .build());

        ListResourceGroupsResp listResp = client.listResourceGroups(ListResourceGroupsReq.builder().build());
        List<String> groupNames = listResp.getGroupNames();
        Assertions.assertEquals(2, groupNames.size());
        Assertions.assertTrue(groupNames.contains(groupA));
        Assertions.assertTrue(groupNames.contains(groupDefault));

        // A
        DescribeResourceGroupResp descResp = client.describeResourceGroup(DescribeResourceGroupReq.builder()
                .groupName(groupA)
                .build());
        Assertions.assertEquals(groupA, descResp.getGroupName());
        Assertions.assertEquals(3, descResp.getCapacity());
        Assertions.assertEquals(1, descResp.getNumberOfAvailableNode());

        ResourceGroupConfig config = descResp.getConfig();
        Assertions.assertEquals(3, config.getRequests().getNodeNum());
        Assertions.assertEquals(4, config.getLimits().getNodeNum());

        Assertions.assertEquals(1, config.getFrom().size());
        Assertions.assertEquals(groupDefault, config.getFrom().get(0).getResourceGroupName());
        Assertions.assertEquals(1, config.getTo().size());
        Assertions.assertEquals(groupDefault, config.getTo().get(0).getResourceGroupName());

        List<NodeInfo> nodes = descResp.getNodes();
        Assertions.assertEquals(1, nodes.size());
        Assertions.assertTrue(nodes.get(0).getNodeId() > 0L);
        Assertions.assertTrue(StringUtils.isNotEmpty(nodes.get(0).getAddress()));
        Assertions.assertTrue(StringUtils.isNotEmpty(nodes.get(0).getHostname()));

        // update
        Map<String, ResourceGroupConfig> resourceGroups = new HashMap<>();
        resourceGroups.put(groupA, ResourceGroupConfig.newBuilder()
                .withRequests(new ResourceGroupLimit(0))
                .withLimits(new ResourceGroupLimit(0))
                .build());
        client.updateResourceGroups(UpdateResourceGroupsReq.builder()
                .resourceGroups(resourceGroups)
                .build());

        descResp = client.describeResourceGroup(DescribeResourceGroupReq.builder()
                .groupName(groupA)
                .build());

        config = descResp.getConfig();
        Assertions.assertEquals(0, config.getRequests().getNodeNum());
        Assertions.assertEquals(0, config.getLimits().getNodeNum());
        Assertions.assertTrue(config.getFrom().isEmpty());
        Assertions.assertTrue(config.getTo().isEmpty());

        // drop
        client.dropResourceGroup(DropResourceGroupReq.builder()
                .groupName(groupA)
                .build());

        // transfer
        String collectionName = generator.generate(10);
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .dimension(DIMENSION)
                .build());
    }

    @Test
    void testReplica() {
        String randomCollectionName = generator.generate(10);

        String vectorFieldName = "float_vector";
        CreateCollectionReq.CollectionSchema collectionSchema = baseSchema();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(vectorFieldName)
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());

        IndexParam indexParam = IndexParam.builder()
                .fieldName(vectorFieldName)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.COSINE)
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

        DescribeCollectionResp descCollResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        DescribeReplicasResp descReplicaResp = client.describeReplicas(DescribeReplicasReq.builder()
                .collectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(1, descReplicaResp.getReplicas().size());
        io.milvus.v2.service.collection.ReplicaInfo info = descReplicaResp.getReplicas().get(0);
        Assertions.assertEquals(descCollResp.getCollectionID(), info.getCollectionID());
        Assertions.assertEquals(1, info.getNodeIDs().size());
        Assertions.assertNotEquals(0L, info.getReplicaID());
        Assertions.assertFalse(info.getResourceGroupName().isEmpty());
        Assertions.assertEquals(1, info.getShardReplicas().size());
        io.milvus.v2.service.collection.ShardReplica replica = info.getShardReplicas().get(0);
        Assertions.assertFalse(replica.getChannelName().isEmpty());
        Assertions.assertFalse(replica.getLeaderAddress().isEmpty());
        Assertions.assertNotEquals(0L, replica.getLeaderID());
    }

    @Test
    void testRunAnalyzer() {
        List<String> texts = new ArrayList<>();
        texts.add("Analyzers (tokenizers) for multi languages");
        texts.add("2.5 to take advantage of enhancements and fixes!");

        Map<String, Object> analyzerParams = new HashMap<>();
        analyzerParams.put("tokenizer", "standard");
        analyzerParams.put("filter",
                Arrays.asList("lowercase",
                        new HashMap<String, Object>() {{
                            put("type", "stop");
                            put("stop_words", Arrays.asList("to", "of", "for", "the"));
                        }}));

        RunAnalyzerResp resp = client.runAnalyzer(RunAnalyzerReq.builder()
                .texts(texts)
                .analyzerParams(analyzerParams)
                .withDetail(true)
                .withHash(true)
                .build());

        List<RunAnalyzerResp.AnalyzerResult> results = resp.getResults();
        Assertions.assertEquals(texts.size(), results.size());

        {
            List<String> tokens1 = Arrays.asList("analyzers", "tokenizers", "multi", "languages");
            List<Long> startOffset1 = Arrays.asList(0L, 11L, 27L, 33L);
            List<Long> endOffset1 = Arrays.asList(9L, 21L, 32L, 42L);
            List<Long> position1 = Arrays.asList(0L, 1L, 3L, 4L);
            List<Long> positionLen1 = Arrays.asList(1L, 1L, 1L, 1L);
            List<Long> hash1 = Arrays.asList(1356745679L, 4089107865L, 3314631429L, 2698072953L);

            List<RunAnalyzerResp.AnalyzerToken> outTokens1 = results.get(0).getTokens();
            System.out.printf("%d tokens%n", outTokens1.size());
            Assertions.assertEquals(tokens1.size(), outTokens1.size());
            for (int i = 0; i < outTokens1.size(); i++) {
                RunAnalyzerResp.AnalyzerToken token = outTokens1.get(i);
                System.out.println(token);
                Assertions.assertEquals(tokens1.get(i), token.getToken());
                Assertions.assertEquals(startOffset1.get(i), token.getStartOffset());
                Assertions.assertEquals(endOffset1.get(i), token.getEndOffset());
                Assertions.assertEquals(position1.get(i), token.getPosition());
                Assertions.assertEquals(positionLen1.get(i), token.getPositionLength());
                Assertions.assertEquals(hash1.get(i), token.getHash());
            }
        }

        {
            List<String> tokens2 = Arrays.asList("2", "5", "take", "advantage", "enhancements", "and", "fixes");
            List<Long> startOffset2 = Arrays.asList(0L, 2L, 7L, 12L, 25L, 38L, 42L);
            List<Long> endOffset2 = Arrays.asList(1L, 3L, 11L, 21L, 37L, 41L, 47L);
            List<Long> position2 = Arrays.asList(0L, 1L, 3L, 4L, 6L, 7L, 8L);
            List<Long> positionLen2 = Arrays.asList(1L, 1L, 1L, 1L, 1L, 1L, 1L);
            List<Long> hash2 = Arrays.asList(450215437L, 2226203566L, 937258619L, 697180577L, 3403941281L, 133536621L, 488262645L);

            List<RunAnalyzerResp.AnalyzerToken> outTokens2 = results.get(1).getTokens();
            System.out.printf("%d tokens%n", outTokens2.size());
            Assertions.assertEquals(tokens2.size(), outTokens2.size());
            for (int i = 0; i < outTokens2.size(); i++) {
                RunAnalyzerResp.AnalyzerToken token = outTokens2.get(i);
                System.out.println(token);
                Assertions.assertEquals(tokens2.get(i), token.getToken());
                Assertions.assertEquals(startOffset2.get(i), token.getStartOffset());
                Assertions.assertEquals(endOffset2.get(i), token.getEndOffset());
                Assertions.assertEquals(position2.get(i), token.getPosition());
                Assertions.assertEquals(positionLen2.get(i), token.getPositionLength());
                Assertions.assertEquals(hash2.get(i), token.getHash());
            }
        }
    }

    @Test
    void testConsistencyLevel() throws InterruptedException {
        String randomCollectionName = generator.generate(10);
        String pkName = "pk";
        String vectorName = "vector";
        int dim = 4;
        String defaultDbName = "default";
        String tempDbName = "db_for_level";

        // create a temp database
        client.createDatabase(CreateDatabaseReq.builder()
                .databaseName(tempDbName)
                .build());

        Function<String, Void> runTestFunc =
                dbName -> {
                    // a client use the temp database
                    ConnectConfig config = ConnectConfig.builder()
                            .uri(milvus.getEndpoint())
                            .dbName(tempDbName)
                            .build();
                    MilvusClientV2 tempClient = null;
                    try {
                        tempClient = new MilvusClientV2(config);

                        for (int i = 0; i < 20; i++) {
                            JsonObject row = new JsonObject();
                            row.addProperty(pkName, i);
                            row.add(vectorName, JsonUtils.toJsonTree(utils.generateFloatVector(dim)));
                            tempClient.insert(InsertReq.builder().databaseName(dbName).collectionName(randomCollectionName)
                                    .data(Collections.singletonList(row)).build());

                            // query/search/hybridSearch immediately after insert, data must be visible
                            String filter = String.format("%s == %d", pkName, i);
                            if (i % 3 == 0) {
                                QueryResp queryResp = client.query(
                                        QueryReq.builder().databaseName(dbName).collectionName(randomCollectionName)
                                                .filter(filter).outputFields(Collections.singletonList(pkName)).build());
                                List<QueryResp.QueryResult> oneResult = queryResp.getQueryResults();
                                Assertions.assertEquals(1, oneResult.size());
                            } else if (i % 2 == 0) {
                                SearchResp searchResp = client.search(
                                        SearchReq.builder().databaseName(dbName).collectionName(randomCollectionName)
                                                .annsField(vectorName).filter(filter)
                                                .data(Collections.singletonList(new FloatVec(utils.generateFloatVector(dim))))
                                                .limit(10).build());
                                List<List<SearchResp.SearchResult>> oneResult = searchResp.getSearchResults();
                                Assertions.assertEquals(1, oneResult.size());
                                Assertions.assertEquals(1, oneResult.get(0).size());
                            } else {
                                AnnSearchReq subReq = AnnSearchReq.builder().vectorFieldName(vectorName).filter(filter)
                                        .vectors(Collections.singletonList(new FloatVec(utils.generateFloatVector(dim))))
                                        .limit(7).build();

                                SearchResp searchResp = client.hybridSearch(
                                        HybridSearchReq.builder().databaseName(dbName).collectionName(randomCollectionName)
                                                .searchRequests(Collections.singletonList(subReq))
                                                .ranker(RRFRanker.builder().k(20).build()).limit(5).build());
                                List<List<SearchResp.SearchResult>> oneResult = searchResp.getSearchResults();
                                Assertions.assertEquals(1, oneResult.size());
                                Assertions.assertEquals(1, oneResult.get(0).size());
                            }
                        }
                    } finally {
                        if (tempClient != null) {
                            tempClient.close();
                        }
                    }
                return null;
        };

        // test SESSION level
        createSimpleCollection(client, "", randomCollectionName, pkName, false, dim, ConsistencyLevel.SESSION);
        runTestFunc.apply(defaultDbName);

        createSimpleCollection(client, tempDbName, randomCollectionName, pkName, false, dim, ConsistencyLevel.SESSION);
        runTestFunc.apply(tempDbName);

        // test STRONG level
        createSimpleCollection(client, "", randomCollectionName, pkName, false, dim, ConsistencyLevel.STRONG);
        runTestFunc.apply(defaultDbName);

        createSimpleCollection(client, tempDbName, randomCollectionName, pkName, false, dim, ConsistencyLevel.STRONG);
        runTestFunc.apply(tempDbName);
    }
}
