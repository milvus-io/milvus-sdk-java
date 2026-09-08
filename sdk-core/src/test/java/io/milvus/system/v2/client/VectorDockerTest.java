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

package io.milvus.system.v2.client;

import io.milvus.support.v2.MilvusV2DockerTestBase;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.common.utils.Float16Utils;
import io.milvus.common.utils.JsonUtils;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.database.request.*;
import io.milvus.v2.service.index.request.*;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.partition.request.*;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.*;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.*;
import io.milvus.v2.service.vector.request.ranker.RRFRanker;
import io.milvus.v2.service.vector.request.ranker.WeightedRanker;
import io.milvus.v2.service.vector.response.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("system")
class VectorDockerTest extends MilvusV2DockerTestBase {
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
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        GetPersistentSegmentInfoResp pSegInfo = null;
        while (System.currentTimeMillis() < deadline) {
            pSegInfo = client.getPersistentSegmentInfo(GetPersistentSegmentInfoReq.builder()
                    .collectionName(randomCollectionName)
                    .build());
            if (!pSegInfo.getSegmentInfos().isEmpty()) {
                break;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Assertions.fail("Interrupted while waiting for persistent segment info in collection: " + randomCollectionName);
            }
        }
        Assertions.assertNotNull(pSegInfo, "Timed out waiting for persistent segment info response");
        Assertions.assertFalse(pSegInfo.getSegmentInfos().isEmpty(), "Timed out waiting for persistent segment info in collection: " + randomCollectionName);
        Assertions.assertEquals(1, pSegInfo.getSegmentInfos().size());
        GetPersistentSegmentInfoResp.PersistentSegmentInfo pInfo = pSegInfo.getSegmentInfos().get(0);
        Assertions.assertTrue(pInfo.getSegmentID() > 0L);
        Assertions.assertTrue(pInfo.getCollectionID() > 0L);
        Assertions.assertTrue(pInfo.getPartitionID() > 0L);
        Assertions.assertEquals(count, pInfo.getNumOfRows());
        Assertions.assertEquals(randomCollectionName, pInfo.getCollectionName());
        Assertions.assertEquals("Flushed", pInfo.getState());
        Assertions.assertEquals("L1", pInfo.getLevel());
        Assertions.assertNotNull(pInfo.getStorageVersion());
//        Assertions.assertFalse(pInfo.getIsSorted());

        // compact
        CompactResp compactResp = client.compact(CompactReq.builder()
                .collectionName(randomCollectionName)
                .build());
        // there is a segment is flushed by the flush() interface, there could be a compaction task created
        Assertions.assertTrue(compactResp.getCompactionID() == -1L || compactResp.getCompactionID() > 0L);

        // create index
        Map<String, Object> extraParams = new HashMap<>();
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
//        Assertions.assertEquals(vectorFieldName, qInfo.getIndexName());
        Assertions.assertTrue(qInfo.getIndexID() > 0L);
        Assertions.assertEquals("Sealed", qInfo.getState());
        Assertions.assertEquals("L1", qInfo.getLevel());
        Assertions.assertEquals(1, qInfo.getNodeIDs().size());
        Assertions.assertTrue(qInfo.getNodeIDs().get(0) > 0L);
        Assertions.assertNotNull(qInfo.getStorageVersion());
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
        long rowCount = getRowCount("", randomCollectionName);
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
            JsonObject row = data.get(RANDOM.nextInt((int) count));
            targetIDs.add(row.get("id").getAsLong());
            List<Float> vector = JsonUtils.fromJson(row.get(vectorFieldName), new TypeToken<List<Float>>() {
            }.getType());
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
            Map<String, Object> template = new HashMap<>();
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

        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("nlist", 64);
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
        long rowCount = getRowCount("", randomCollectionName);
        Assertions.assertEquals(count, rowCount);

        // search in collection
        int nq = 5;
        int topk = 10;
        List<Long> targetIDs = new ArrayList<>();
        List<BaseVector> targetVectors = new ArrayList<>();
        List<ByteBuffer> targetOriginVectors = new ArrayList<>();
        for (int i = 0; i < nq; i++) {
            JsonObject row = data.get(RANDOM.nextInt((int) count));
            targetIDs.add(row.get("id").getAsLong());
            byte[] vector = JsonUtils.fromJson(row.get(vectorFieldName), new TypeToken<byte[]>() {
            }.getType());
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
        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("nlist", 64);
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
        JsonObject row = data.get((int) targetID);
        List<Float> originVector = new ArrayList<>();
        for (int i = 0; i < DIMENSION; ++i) {
            originVector.add((float) 1 / (i + 1));
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

        // search by ids
        {
            List<Object> ids = Arrays.asList(5L, 88L, 100L);
            SearchResp searchResp = client.search(SearchReq.builder()
                    .collectionName(randomCollectionName)
                    .annsField(bfloat16Field)
                    .ids(ids)
                    .limit(topk)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .outputFields(Collections.singletonList(bfloat16Field))
                    .build());
            List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
            Assertions.assertEquals(3, searchResults.size());
            for (int i = 0; i < searchResults.size(); i++) {
                List<SearchResp.SearchResult> results = searchResults.get(i);
                Assertions.assertEquals(topk, results.size());
                SearchResp.SearchResult firstResult = results.get(0);
                Assertions.assertEquals(ids.get(i), firstResult.getId());
            }
        }

        // get row count
        long rowCount = getRowCount("", randomCollectionName);
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

        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("drop_ratio_build", 0.2);
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
        long rowCount = getRowCount("", randomCollectionName);
        Assertions.assertEquals(count, rowCount);

        // search in collection
        int nq = 5;
        int topk = 10;
        List<Long> targetIDs = new ArrayList<>();
        List<BaseVector> targetVectors = new ArrayList<>();
        for (int i = 0; i < nq; i++) {
            JsonObject row = data.get(RANDOM.nextInt((int) count));
            targetIDs.add(row.get("id").getAsLong());
            SortedMap<Long, Float> vector = JsonUtils.fromJson(row.get(vectorFieldName), new TypeToken<SortedMap<Long, Float>>() {
            }.getType());
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
        Map<String, Object> extraParams = new HashMap<>();
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
            ByteBuffer getVec = (ByteBuffer) entity.get(vectorFieldName);
            Assertions.assertEquals(originVec, getVec);
        }
    }

    @Test
    void testArray() {
        String randomCollectionName = generator.generate(10);
        String pkField = "key";
        String vectorField = "vector";
        String arrayField = "array";
        int capacity = 10;
        int varcharLength = 88;
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(pkField)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(true)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(vectorField)
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(arrayField)
                .description("dummy")
                .dataType(DataType.Array)
                .elementType(DataType.VarChar)
                .maxCapacity(capacity)
                .maxLength(varcharLength)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName(vectorField)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.COSINE)
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(indexParams)
                .build();
        client.createCollection(requestCreate);

        // describe
        DescribeCollectionResp descResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());
        CreateCollectionReq.CollectionSchema descSchema = descResp.getCollectionSchema();
        Assertions.assertEquals(3, descSchema.getFieldSchemaList().size());
        CreateCollectionReq.FieldSchema arraySchema = descSchema.getFieldSchemaList().get(2);
        Assertions.assertEquals(arrayField, arraySchema.getName());
        Assertions.assertEquals("dummy", arraySchema.getDescription());
        Assertions.assertEquals(DataType.Array, arraySchema.getDataType());
        Assertions.assertEquals(DataType.VarChar, arraySchema.getElementType());
        Assertions.assertEquals(capacity, arraySchema.getMaxCapacity());
        Assertions.assertEquals(varcharLength, arraySchema.getMaxLength());

        // insert
        List<JsonObject> rows = new ArrayList<>();
        int count = 20;
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.add(vectorField, JsonUtils.toJsonTree(utils.generateFloatVector()));
            List<String> strArray = new ArrayList<>();
            for (int k = i; k < capacity; k++) {
                strArray.add(String.format("string-%d-%d", i, k));
            }
            row.add(arrayField, JsonUtils.toJsonTree(strArray).getAsJsonArray());
            rows.add(row);
        }

        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(rows)
                .build());
        Assertions.assertEquals(count, insertResp.getInsertCnt());

        // query
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(randomCollectionName)
                .filter(String.format("ARRAY_CONTAINS(%s, \"string-0-9\")", arrayField))
                .limit(5)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Collections.singletonList(arrayField))
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        Assertions.assertEquals(1, queryResults.size());
        Assertions.assertTrue(queryResults.get(0).getEntity().containsKey(arrayField));
        Assertions.assertInstanceOf(List.class, queryResults.get(0).getEntity().get(arrayField));
        List<String> arr = (List<String>) queryResults.get(0).getEntity().get(arrayField);
        Assertions.assertEquals(capacity, arr.size());
    }

    @Test
    void testStruct() {
        String randomCollectionName = generator.generate(10);
        String pkField = "key";
        String normalVectorField = "vector";
        String normalScalarField = "text";
        int structCapacity = 300;
        int varcharLength = 100;
        int st1VectorDimension = 128;
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(pkField)
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(normalVectorField)
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(normalScalarField)
                .dataType(DataType.VarChar)
                .maxLength(varcharLength)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("st1")
                .description("dummy")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(structCapacity)
                .addStructField(AddFieldReq.builder()
                        .fieldName("aaa")
                        .description("dummy")
                        .dataType(DataType.VarChar)
                        .maxLength(varcharLength)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("float_vector")
                        .description("dummy")
                        .dataType(DataType.FloatVector)
                        .dimension(st1VectorDimension)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("binary_vector")
                        .description("dummy")
                        .dataType(DataType.BinaryVector)
                        .dimension(st1VectorDimension)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("float16_vector")
                        .description("dummy")
                        .dataType(DataType.Float16Vector)
                        .dimension(st1VectorDimension)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("bfloat16_vector")
                        .description("dummy")
                        .dataType(DataType.BFloat16Vector)
                        .dimension(st1VectorDimension)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("int8_vector")
                        .description("dummy")
                        .dataType(DataType.Int8Vector)
                        .dimension(st1VectorDimension)
                        .build())
                .build());
        // st1 is a nullable struct field; its sub-fields inherit nullable=true on the wire
        collectionSchema.getStructField("st1").setNullable(Boolean.TRUE);
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("st2")
                .description("dummy")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(structCapacity)
                .addStructField(AddFieldReq.builder()
                        .fieldName("bbb")
                        .description("dummy")
                        .dataType(DataType.VarChar)
                        .maxLength(varcharLength)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("float_vector")
                        .description("dummy")
                        .dataType(DataType.FloatVector)
                        .dimension(64)
                        .build())
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .build();
        client.createCollection(requestCreate);

        IndexParam.MetricType st1FloatVectorMetric = IndexParam.MetricType.MAX_SIM_IP;
        IndexParam.MetricType st1BinaryVectorMetric = IndexParam.MetricType.MAX_SIM_HAMMING;
        IndexParam.MetricType st1Float16VectorMetric = IndexParam.MetricType.MAX_SIM_COSINE;
        IndexParam.MetricType st1BFloat16VectorMetric = IndexParam.MetricType.MAX_SIM_L2;
        IndexParam.MetricType st1Int8VectorMetric = IndexParam.MetricType.MAX_SIM_L2;

        List<IndexParam> indexParams = new ArrayList<>();
        Map<String, Object> ivfExtraParams = new HashMap<>();
        ivfExtraParams.put("nlist", 64);
        indexParams.add(IndexParam.builder()
                .fieldName(normalVectorField)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.COSINE)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("st1[float_vector]")
                .indexName("index_float")
                .indexType(IndexParam.IndexType.HNSW_PQ)
                .metricType(st1FloatVectorMetric)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("st1[binary_vector]")
                .indexName("index_binary")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(st1BinaryVectorMetric)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("st1[float16_vector]")
                .indexName("index_float16")
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(st1Float16VectorMetric)
                .extraParams(ivfExtraParams)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("st1[bfloat16_vector]")
                .indexName("index_bfloat16")
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(st1BFloat16VectorMetric)
                .extraParams(ivfExtraParams)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("st1[int8_vector]")
                .indexName("index_int8")
                .indexType(IndexParam.IndexType.HNSW_SQ)
                .metricType(st1Int8VectorMetric)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("st2[float_vector]")
                .indexName("index2")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .build());
        client.createIndex(CreateIndexReq.builder()
                .collectionName(randomCollectionName)
                .indexParams(indexParams)
                .build());
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        DescribeCollectionResp descResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());
        CreateCollectionReq.CollectionSchema descSchema = descResp.getCollectionSchema();
        Assertions.assertEquals(2, descSchema.getStructFields().size());
        CreateCollectionReq.StructFieldSchema structSchema = descSchema.getStructFields().get(0);
        Assertions.assertEquals("st1", structSchema.getName());
        Assertions.assertEquals("dummy", structSchema.getDescription());
        Assertions.assertEquals(DataType.Array, structSchema.getDataType());
        Assertions.assertEquals(DataType.Struct, structSchema.getElementType());
        Assertions.assertEquals(structCapacity, structSchema.getMaxCapacity());
        Assertions.assertEquals(6, structSchema.getFields().size());
        Assertions.assertEquals(DataType.FloatVector, structSchema.getFields().get(1).getDataType());
        Assertions.assertEquals(DataType.BinaryVector, structSchema.getFields().get(2).getDataType());
        Assertions.assertEquals(DataType.Float16Vector, structSchema.getFields().get(3).getDataType());
        Assertions.assertEquals(DataType.BFloat16Vector, structSchema.getFields().get(4).getDataType());
        Assertions.assertEquals(DataType.Int8Vector, structSchema.getFields().get(5).getDataType());
        // nullable struct field: nullable must be true on the struct and inherited by every sub-field
        Assertions.assertTrue(structSchema.getNullable());
        for (CreateCollectionReq.FieldSchema subField : structSchema.getFields()) {
            Assertions.assertTrue(subField.getIsNullable(), "sub-field " + subField.getName() + " should inherit nullable");
        }
        // st2 is non-nullable; locate it by name so the check is independent of describe ordering
        Assertions.assertFalse(descSchema.getStructField("st2").getNullable());

        DescribeIndexResp binaryIndexDesc = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("st1[binary_vector]")
                .indexName("index_binary")
                .build());
        Assertions.assertEquals(IndexParam.IndexType.HNSW, binaryIndexDesc.getIndexDescriptions().get(0).getIndexType());
        Assertions.assertEquals(st1BinaryVectorMetric, binaryIndexDesc.getIndexDescriptions().get(0).getMetricType());
        DescribeIndexResp float16IndexDesc = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("st1[float16_vector]")
                .indexName("index_float16")
                .build());
        Assertions.assertEquals(IndexParam.IndexType.IVF_FLAT, float16IndexDesc.getIndexDescriptions().get(0).getIndexType());
        Assertions.assertEquals(st1Float16VectorMetric, float16IndexDesc.getIndexDescriptions().get(0).getMetricType());
        DescribeIndexResp bfloat16IndexDesc = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("st1[bfloat16_vector]")
                .indexName("index_bfloat16")
                .build());
        Assertions.assertEquals(IndexParam.IndexType.IVF_FLAT, bfloat16IndexDesc.getIndexDescriptions().get(0).getIndexType());
        Assertions.assertEquals(st1BFloat16VectorMetric, bfloat16IndexDesc.getIndexDescriptions().get(0).getMetricType());
        DescribeIndexResp int8IndexDesc = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("st1[int8_vector]")
                .indexName("index_int8")
                .build());
        Assertions.assertEquals(IndexParam.IndexType.HNSW_SQ, int8IndexDesc.getIndexDescriptions().get(0).getIndexType());
        Assertions.assertEquals(st1Int8VectorMetric, int8IndexDesc.getIndexDescriptions().get(0).getMetricType());

        List<JsonObject> rows = new ArrayList<>();
        int count = 20;
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(pkField, i);
            row.addProperty(normalScalarField, "text_" + i);
            row.add(normalVectorField, JsonUtils.toJsonTree(utils.generateFloatVector()));
            JsonArray structArr1 = new JsonArray();
            JsonArray structArr2 = new JsonArray();
            for (int k = 0; k < i; k++) {
                if (k < 5) {
                    JsonObject struct = new JsonObject();
                    struct.addProperty("aaa", "No." + k);
                    struct.add("float_vector", JsonUtils.toJsonTree(utils.generateFloatVector(st1VectorDimension)));
                    struct.add("binary_vector", JsonUtils.toJsonTree(utils.generateBinaryVector(st1VectorDimension).array()));
                    struct.add("float16_vector", JsonUtils.toJsonTree(utils.generateFloat16Vector(st1VectorDimension).array()));
                    struct.add("bfloat16_vector", JsonUtils.toJsonTree(utils.generateBFloat16Vector(st1VectorDimension).array()));
                    struct.add("int8_vector", JsonUtils.toJsonTree(generateStructInt8Vector(st1VectorDimension).array()));
                    structArr1.add(struct);
                } else {
                    JsonObject struct = new JsonObject();
                    struct.addProperty("bbb", "No." + k);
                    struct.add("float_vector", JsonUtils.toJsonTree(utils.generateFloatVector(64)));
                    structArr2.add(struct);
                }
            }
            if (i % 7 == 0 && i != 0) {
                // st1 is nullable: store a null row (st2 stays valid so the row remains searchable)
                row.add("st1", JsonNull.INSTANCE);
            } else {
                row.add("st1", structArr1);
            }
            row.add("st2", structArr2);
            rows.add(row);
        }

        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(rows)
                .build());
        Assertions.assertEquals(count, insertResp.getInsertCnt());

        QueryResp insertedRowQuery = client.query(QueryReq.builder()
                .collectionName(randomCollectionName)
                .filter(String.format("%s == 10", pkField))
                .limit(1)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Collections.singletonList("st1"))
                .build());
        Assertions.assertEquals(1, insertedRowQuery.getQueryResults().size());
        Map<String, Object> insertedEntity = insertedRowQuery.getQueryResults().get(0).getEntity();
        System.out.println("st1 of pkField=10: " + insertedEntity.get("st1"));
        List<Map<String, Object>> actualStructs = (List<Map<String, Object>>) insertedEntity.get("st1");
        JsonArray expectedStructs = rows.get(10).getAsJsonArray("st1");
        Assertions.assertEquals(expectedStructs.size(), actualStructs.size());
        for (int i = 0; i < expectedStructs.size(); i++) {
            JsonObject expectedStruct = expectedStructs.get(i).getAsJsonObject();
            Map<String, Object> actualStruct = actualStructs.get(i);
            Assertions.assertEquals(expectedStruct.get("aaa").getAsString(), actualStruct.get("aaa"));
            Assertions.assertEquals(
                    JsonUtils.fromJson(expectedStruct.get("float_vector"), new TypeToken<List<Float>>() {
                    }.getType()),
                    actualStruct.get("float_vector"));
            Assertions.assertArrayEquals(
                    JsonUtils.fromJson(expectedStruct.get("binary_vector"), byte[].class),
                    ((ByteBuffer) actualStruct.get("binary_vector")).array());
            Assertions.assertArrayEquals(
                    JsonUtils.fromJson(expectedStruct.get("float16_vector"), byte[].class),
                    ((ByteBuffer) actualStruct.get("float16_vector")).array());
            Assertions.assertArrayEquals(
                    JsonUtils.fromJson(expectedStruct.get("bfloat16_vector"), byte[].class),
                    ((ByteBuffer) actualStruct.get("bfloat16_vector")).array());
            Assertions.assertArrayEquals(
                    JsonUtils.fromJson(expectedStruct.get("int8_vector"), byte[].class),
                    ((ByteBuffer) actualStruct.get("int8_vector")).array());
        }

        JsonObject row = new JsonObject();
        row.addProperty(pkField, 0);
        row.addProperty(normalScalarField, "update_text");
        row.add(normalVectorField, JsonUtils.toJsonTree(utils.generateFloatVector()));
        JsonArray structArr1 = new JsonArray();
        JsonArray structArr2 = new JsonArray();
        for (int k = 0; k < 2; k++) {
            JsonObject struct1 = new JsonObject();
            struct1.addProperty("aaa", "updated_No." + k);
            struct1.add("float_vector", JsonUtils.toJsonTree(utils.generateFloatVector(st1VectorDimension)));
            struct1.add("binary_vector", JsonUtils.toJsonTree(utils.generateBinaryVector(st1VectorDimension).array()));
            struct1.add("float16_vector", JsonUtils.toJsonTree(utils.generateFloat16Vector(st1VectorDimension).array()));
            struct1.add("bfloat16_vector", JsonUtils.toJsonTree(utils.generateBFloat16Vector(st1VectorDimension).array()));
            struct1.add("int8_vector", JsonUtils.toJsonTree(generateStructInt8Vector(st1VectorDimension).array()));
            structArr1.add(struct1);
        }
        row.add("st1", structArr1);
        row.add("st2", structArr2);

        UpsertResp upsertResp = client.upsert(UpsertReq.builder()
                .collectionName(randomCollectionName)
                .data(Collections.singletonList(row))
                .build());
        Assertions.assertEquals(1, upsertResp.getUpsertCnt());

        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(randomCollectionName)
                .filter(String.format("%s == 0 or %s == 9", pkField, pkField))
                .limit(3)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Collections.singletonList("*"))
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        Assertions.assertEquals(2, queryResults.size());

        EmbeddingList embList0 = new EmbeddingList();
        EmbeddingList embList1 = new EmbeddingList();
        EmbeddingList binaryEmbList0 = new EmbeddingList();
        EmbeddingList binaryEmbList1 = new EmbeddingList();
        EmbeddingList float16EmbList0 = new EmbeddingList();
        EmbeddingList float16EmbList1 = new EmbeddingList();
        EmbeddingList bfloat16EmbList0 = new EmbeddingList();
        EmbeddingList bfloat16EmbList1 = new EmbeddingList();
        EmbeddingList int8EmbList0 = new EmbeddingList();
        EmbeddingList int8EmbList1 = new EmbeddingList();
        List<BaseVector> elementSearchList = new ArrayList<>();

        List<Map<String, Object>> structs10 = (List<Map<String, Object>>) queryResults.get(0).getEntity().get("st1");
        Assertions.assertEquals(2, structs10.size());
        for (Map<String, Object> struct : structs10) {
            embList0.add(new FloatVec((List<Float>) struct.get("float_vector")));
            binaryEmbList0.add(new BinaryVec((ByteBuffer) struct.get("binary_vector")));
            float16EmbList0.add(new Float16Vec((ByteBuffer) struct.get("float16_vector")));
            bfloat16EmbList0.add(new BFloat16Vec((ByteBuffer) struct.get("bfloat16_vector")));
            int8EmbList0.add(new Int8Vec((ByteBuffer) struct.get("int8_vector")));
        }
        List<Map<String, Object>> structs11 = (List<Map<String, Object>>) queryResults.get(1).getEntity().get("st1");
        Assertions.assertEquals(5, structs11.size());
        for (Map<String, Object> struct : structs11) {
            embList1.add(new FloatVec((List<Float>) struct.get("float_vector")));
            binaryEmbList1.add(new BinaryVec((ByteBuffer) struct.get("binary_vector")));
            float16EmbList1.add(new Float16Vec((ByteBuffer) struct.get("float16_vector")));
            bfloat16EmbList1.add(new BFloat16Vec((ByteBuffer) struct.get("bfloat16_vector")));
            int8EmbList1.add(new Int8Vec((ByteBuffer) struct.get("int8_vector")));
        }

        List<Map<String, Object>> structs20 = (List<Map<String, Object>>) queryResults.get(1).getEntity().get("st2");
        Assertions.assertEquals(4, structs20.size());
        elementSearchList.add(new FloatVec((List<Float>) structs20.get(1).get("float_vector")));

        int topK = 5;
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField("st1[float_vector]")
                .data(Arrays.asList(embList0, embList1))
                .limit(topK)
                .outputFields(Collections.singletonList("st1[aaa]"))
                .build());
        Assertions.assertEquals(0L, (long) searchResp.getSearchResults().get(0).get(0).getId());
        Assertions.assertEquals(9L, (long) searchResp.getSearchResults().get(1).get(0).getId());

        SearchResp binarySearchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField("st1[binary_vector]")
                .data(Arrays.asList(binaryEmbList0, binaryEmbList1))
                .limit(topK)
                .metricType(st1BinaryVectorMetric)
                .outputFields(Collections.singletonList("st1[aaa]"))
                .build());
        Assertions.assertEquals(0L, (long) binarySearchResp.getSearchResults().get(0).get(0).getId());
        Assertions.assertEquals(9L, (long) binarySearchResp.getSearchResults().get(1).get(0).getId());

        SearchResp float16SearchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField("st1[float16_vector]")
                .data(Arrays.asList(float16EmbList0, float16EmbList1))
                .limit(topK)
                .metricType(st1Float16VectorMetric)
                .outputFields(Collections.singletonList("st1[aaa]"))
                .build());
        Assertions.assertEquals(0L, (long) float16SearchResp.getSearchResults().get(0).get(0).getId());
        Assertions.assertEquals(9L, (long) float16SearchResp.getSearchResults().get(1).get(0).getId());

        SearchResp bfloat16SearchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField("st1[bfloat16_vector]")
                .data(Arrays.asList(bfloat16EmbList0, bfloat16EmbList1))
                .limit(topK)
                .metricType(st1BFloat16VectorMetric)
                .outputFields(Collections.singletonList("st1[aaa]"))
                .build());
        Assertions.assertEquals(0L, (long) bfloat16SearchResp.getSearchResults().get(0).get(0).getId());
        Assertions.assertEquals(9L, (long) bfloat16SearchResp.getSearchResults().get(1).get(0).getId());

        SearchResp int8SearchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField("st1[int8_vector]")
                .data(Arrays.asList(int8EmbList0, int8EmbList1))
                .limit(topK)
                .metricType(st1Int8VectorMetric)
                .outputFields(Collections.singletonList("st1[aaa]"))
                .build());
        Assertions.assertEquals(0L, (long) int8SearchResp.getSearchResults().get(0).get(0).getId());
        Assertions.assertEquals(9L, (long) int8SearchResp.getSearchResults().get(1).get(0).getId());

        // element-level search
        SearchResp elementSearchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField("st2[float_vector]")
                .data(elementSearchList)
                .limit(topK)
                .metricType(IndexParam.MetricType.L2)
                .build());
        Assertions.assertEquals(1, elementSearchResp.getSearchResults().size());
        Assertions.assertEquals(5, elementSearchResp.getSearchResults().get(0).size());
        SearchResp.SearchResult elementResult = elementSearchResp.getSearchResults().get(0).get(0);
        Assertions.assertEquals(9L, (long) elementResult.getId());
        Assertions.assertEquals(1L, (long) elementResult.getElementOffset());

        // nullable struct field: query rows that were inserted with st1 = null
        QueryResp nullableQueryResp = client.query(QueryReq.builder()
                .collectionName(randomCollectionName)
                .filter(String.format("%s == 7 or %s == 14", pkField, pkField))
                .limit(3)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Arrays.asList(pkField, "st1", "st2"))
                .build());
        Assertions.assertEquals(2, nullableQueryResp.getQueryResults().size());
        for (QueryResp.QueryResult result : nullableQueryResp.getQueryResults()) {
            Map<String, Object> entity = result.getEntity();
            Assertions.assertNull(entity.get("st1"), "st1 of pk=" + entity.get(pkField) + " should be null");
            Assertions.assertNotNull(entity.get("st2"));
        }

        // nullable struct field: search on st2 vector of a null-st1 row; the top-1 hit is that
        // row itself and its st1 output must decode back to null
        QueryResp vectorQueryResp = client.query(QueryReq.builder()
                .collectionName(randomCollectionName)
                .filter(String.format("%s == 7", pkField))
                .limit(1)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Collections.singletonList("st2"))
                .build());
        List<Map<String, Object>> row7Structs = (List<Map<String, Object>>)
                vectorQueryResp.getQueryResults().get(0).getEntity().get("st2");
        List<Float> row7Vector = (List<Float>) row7Structs.get(1).get("float_vector");
        SearchResp nullableSearchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField("st2[float_vector]")
                .data(Collections.singletonList(new FloatVec(row7Vector)))
                .limit(topK)
                .metricType(IndexParam.MetricType.L2)
                .outputFields(Arrays.asList(pkField, "st1"))
                .build());
        Assertions.assertEquals(1, nullableSearchResp.getSearchResults().size());
        SearchResp.SearchResult nullableTop = nullableSearchResp.getSearchResults().get(0).get(0);
        Assertions.assertEquals(7L, (long) nullableTop.getId());
        Assertions.assertNull(nullableTop.getEntity().get("st1"));

        client.dropCollection(DropCollectionReq.builder().collectionName(randomCollectionName).build());
    }

    @Test
    void testGeometry() {
        String randomCollectionName = generator.generate(10);
        String pkField = "pk";
        String vectorField = "vector";
        String geoField = "geo";
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(pkField)
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(vectorField)
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(geoField)
                .dataType(DataType.Geometry)
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .build();
        client.createCollection(requestCreate);

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName(vectorField)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.COSINE)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName(geoField)
                .indexType(IndexParam.IndexType.RTREE)
                .build());
        client.createIndex(CreateIndexReq.builder()
                .collectionName(randomCollectionName)
                .indexParams(indexParams)
                .build());
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        // describe
        DescribeCollectionResp descResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());
        CreateCollectionReq.CollectionSchema descSchema = descResp.getCollectionSchema();
        List<CreateCollectionReq.FieldSchema> fields = descSchema.getFieldSchemaList();
        Assertions.assertEquals(collectionSchema.getFieldSchemaList().size(), fields.size());
        Assertions.assertEquals(geoField, fields.get(2).getName());
        Assertions.assertEquals(DataType.Geometry, fields.get(2).getDataType());

        // insert
        List<JsonObject> rows = new ArrayList<>();
        {
            JsonObject row = new JsonObject();
            row.addProperty(pkField, 1);
            row.addProperty(geoField, "POINT (1.0 -1.0)");
            row.add(vectorField, JsonUtils.toJsonTree(utils.generateFloatVector()));
            rows.add(row);
        }
        {
            JsonObject row = new JsonObject();
            row.addProperty(pkField, 2);
            row.addProperty(geoField, "POINT (2.0 2.0)");
            row.add(vectorField, JsonUtils.toJsonTree(utils.generateFloatVector()));
            rows.add(row);
        }
        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(rows)
                .build());
        Assertions.assertEquals(rows.size(), insertResp.getInsertCnt());

        // query
        String filter = String.format("ST_WITHIN(%s, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))')", geoField);
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(randomCollectionName)
                .limit(10)
                .filter(filter)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Arrays.asList(pkField, geoField))
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        Assertions.assertEquals(1, queryResults.size());
        for (QueryResp.QueryResult res : queryResults) {
            Assertions.assertTrue(res.getEntity().containsKey(geoField));
            Assertions.assertEquals(res.getEntity().get(pkField), 2L);
        }

        // search
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField(vectorField)
                .data(Collections.singletonList(new FloatVec(utils.generateFloatVector())))
                .limit(10)
                .filter(filter)
                .outputFields(Arrays.asList(pkField, geoField))
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        Assertions.assertEquals(1, searchResults.size());
        for (List<SearchResp.SearchResult> oneResults : searchResults) {
            Assertions.assertEquals(1, oneResults.size());
            for (SearchResp.SearchResult res : oneResults) {
                Assertions.assertTrue(res.getEntity().containsKey(geoField));
                Assertions.assertEquals(res.getId(), 2L);
            }
        }
    }

    @Test
    void testTimestamp() {
        String randomCollectionName = generator.generate(10);
        String pkField = "pk";
        String vectorField = "vector";
        String timestampField = "timestamp";
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(pkField)
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(vectorField)
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(timestampField)
                .dataType(DataType.Timestamptz)
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .build();
        client.createCollection(requestCreate);

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName(vectorField)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.COSINE)
                .build());
        client.createIndex(CreateIndexReq.builder()
                .collectionName(randomCollectionName)
                .indexParams(indexParams)
                .build());
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        // set database default timezone
        Map<String, String> props = new HashMap<>();
        props.put("timezone", "Asia/Shanghai");
        client.alterDatabaseProperties(AlterDatabasePropertiesReq.builder()
                .databaseName("default")
                .properties(props)
                .build());

        // describe
        DescribeCollectionResp descResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());
        CreateCollectionReq.CollectionSchema descSchema = descResp.getCollectionSchema();
        List<CreateCollectionReq.FieldSchema> fields = descSchema.getFieldSchemaList();
        Assertions.assertEquals(collectionSchema.getFieldSchemaList().size(), fields.size());
        Assertions.assertEquals(timestampField, fields.get(2).getName());
        Assertions.assertEquals(DataType.Timestamptz, fields.get(2).getDataType());

        // insert
        List<JsonObject> rows = new ArrayList<>();
        {
            JsonObject row = new JsonObject();
            row.addProperty(pkField, 1);
            row.addProperty(timestampField, "2025-01-02T00:00:00+08:00"); // Shanghai time
            row.add(vectorField, JsonUtils.toJsonTree(utils.generateFloatVector()));
            rows.add(row);
        }
        {
            JsonObject row = new JsonObject();
            row.addProperty(pkField, 2);
            row.addProperty(timestampField, "2025-01-02T00:00:00-06:00"); // Chicago time
            row.add(vectorField, JsonUtils.toJsonTree(utils.generateFloatVector()));
            rows.add(row);
        }
        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(rows)
                .build());
        Assertions.assertEquals(rows.size(), insertResp.getInsertCnt());

        // query
        Map<String, Object> params = new HashMap<>();
//        params.put("timezone", "America/Chicago");
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(randomCollectionName)
                .limit(10)
                .queryParams(params)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Arrays.asList(pkField, timestampField))
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        Assertions.assertEquals(2, queryResults.size());
        for (QueryResp.QueryResult res : queryResults) {
            Assertions.assertTrue(res.getEntity().containsKey(timestampField));
        }

        // search
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField(vectorField)
                .data(Collections.singletonList(new FloatVec(utils.generateFloatVector())))
                .limit(10)
                .searchParams(params)
                .outputFields(Arrays.asList(pkField, timestampField))
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        Assertions.assertEquals(1, searchResults.size());
        for (List<SearchResp.SearchResult> oneResults : searchResults) {
            Assertions.assertEquals(2, oneResults.size());
            for (SearchResp.SearchResult res : oneResults) {
                Assertions.assertTrue(res.getEntity().containsKey(timestampField));
            }
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
                .extraParams(new HashMap<String, Object>() {{
                    put("nlist", 64);
                }})
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
                .extraParams(new HashMap<String, Object>() {{
                    put("drop_ratio_build", 0.1);
                }})
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
        int topk = 10;
        Function<Map<String, Object>, HybridSearchReq> genRequestFunc =
                config -> {
                    int float_nq = config.containsKey("float_nq") ? (Integer) config.get("float_nq") : 5;
                    int sparse_nq = config.containsKey("sparse_nq") ? (Integer) config.get("sparse_nq") : 5;
                    List<BaseVector> floatVectors = new ArrayList<>();
                    List<BaseVector> binaryVectors = new ArrayList<>();
                    List<BaseVector> sparseVectors = new ArrayList<>();
                    for (int i = 0; i < float_nq; i++) {
                        floatVectors.add(new FloatVec(utils.generateFloatVector()));
                        binaryVectors.add(new BinaryVec(utils.generateBinaryVector()));
                    }

                    for (int i = 0; i < sparse_nq; i++) {
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
                    boolean useFunctionScore = (Boolean) config.get("useFunctionScore");
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
        config.put("float_nq", 0);
        config.put("sparse_nq", 0);
        config.put("useFunctionScore", false);
        // search with an empty nq, return error
        Assertions.assertThrows(MilvusClientException.class, () -> client.hybridSearch(genRequestFunc.apply(config)));

        // unequal nq, return error
        config.put("float_nq", 2);
        config.put("sparse_nq", 1);
        Assertions.assertThrows(MilvusClientException.class, () -> client.hybridSearch(genRequestFunc.apply(config)));

        // TODO: comment out these lines because current milvus master has bug in hybrid-search empty collection
//        // search on empty collection, no result returned
//        config.put("sparse_nq", 2);
//        SearchResp searchResp = client.hybridSearch(genRequestFunc.apply(config));
//        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
//        Assertions.assertEquals(nq, searchResults.size());
//        for (List<SearchResp.SearchResult> result : searchResults) {
//            Assertions.assertTrue(result.isEmpty());
//        }

        // insert rows
        long count = 10000;
        List<JsonObject> data = generateRandomData(collectionSchema, count);
        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(data)
                .build());
        Assertions.assertEquals(count, insertResp.getInsertCnt());

        // get row count
        long rowCount = getRowCount("", randomCollectionName);
        Assertions.assertEquals(count, rowCount);

        // search again, there are results
        config.put("float_nq", 5);
        config.put("sparse_nq", 5);
        config.put("useFunctionScore", true);
        SearchResp searchResp = client.hybridSearch(genRequestFunc.apply(config));
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        Assertions.assertEquals(5, searchResults.size());
        for (int i = 0; i < 5; i++) {
            List<SearchResp.SearchResult> results = searchResults.get(i);
            Assertions.assertEquals(topk, results.size());
        }

        client.dropCollection(DropCollectionReq.builder().collectionName(randomCollectionName).build());
    }

    @Test
    void testText() {
        String collectionName = generator.generate(10);
        String longText = String.join("", Collections.nCopies(70000, "x"));
        List<Float> vector = utils.generateFloatVector();

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("body")
                .dataType(DataType.Text)
                .enableAnalyzer(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("sparse")
                .dataType(DataType.SparseFloatVector)
                .build());
        schema.addFunction(CreateCollectionReq.Function.builder()
                .name("body_bm25")
                .functionType(FunctionType.BM25)
                .inputFieldNames(Collections.singletonList("body"))
                .outputFieldNames(Collections.singletonList("sparse"))
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());
        client.addCollectionField(AddCollectionFieldReq.builder()
                .collectionName(collectionName)
                .fieldName("added_text")
                .dataType(DataType.Text)
                .isNullable(true)
                .build());

        DescribeCollectionResp describeResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionName)
                .build());
        Assertions.assertEquals(DataType.Text, describeResp.getCollectionSchema().getField("body").getDataType());
        Assertions.assertEquals(DataType.Text, describeResp.getCollectionSchema().getField("added_text").getDataType());
        Assertions.assertEquals("body_bm25",
                describeResp.getCollectionSchema().getFunctionList().get(0).getName());

        client.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Arrays.asList(
                        IndexParam.builder()
                                .fieldName("vector")
                                .indexType(IndexParam.IndexType.HNSW)
                                .metricType(IndexParam.MetricType.COSINE)
                                .build(),
                        IndexParam.builder()
                                .fieldName("sparse")
                                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                                .metricType(IndexParam.MetricType.BM25)
                                .build()))
                .build());
        client.loadCollection(LoadCollectionReq.builder().collectionName(collectionName).build());

        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        row.add("vector", JsonUtils.toJsonTree(vector));
        row.addProperty("body", longText);
        client.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(row))
                .build());

        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter("id == 1")
                .outputFields(Collections.singletonList("body"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Map<String, Object> queryEntity = queryResp.getQueryResults().get(0).getEntity();
        Assertions.assertEquals(longText, queryEntity.get("body"));

        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField("vector")
                .data(Collections.singletonList(new FloatVec(vector)))
                .limit(1)
                .outputFields(Collections.singletonList("body"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Map<String, Object> searchEntity = searchResp.getSearchResults().get(0).get(0).getEntity();
        Assertions.assertEquals(longText, searchEntity.get("body"));

        client.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }

}
