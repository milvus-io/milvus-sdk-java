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
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.common.utils.JsonUtils;
import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.grpc.LoadState;
import io.milvus.param.Constant;
import io.milvus.support.TestUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.GetLoadStateResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.database.request.*;
import io.milvus.v2.service.index.request.*;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.partition.request.*;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.*;
import io.milvus.v2.service.vector.response.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("system")
class CollectionDockerTest extends MilvusV2DockerTestBase {
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

        Assertions.assertThrows(MilvusClientException.class, () -> client.describeCollection(DescribeCollectionReq.builder()
                .collectionName("CCC")
                .build()));
    }

    @Test
    void testCacheCollectionSchema() throws InterruptedException {
        String randomCollectionName = generator.generate(10);

        // create a new db
        String testDbName = "test_cache_db";
        client.createDatabase(CreateDatabaseReq.builder()
                .databaseName(testDbName)
                .build());

        // create a collection in the default db
        createSimpleCollection(client, "", randomCollectionName, "pk", false, DIMENSION, ConsistencyLevel.BOUNDED);

        // a temp client connect to the new db
        ConnectConfig config = ConnectConfig.builder()
                .uri(TestUtils.MilvusStandaloneUri)
                .dbName(testDbName)
                .build();
        // fix tempClient not close
        MilvusClientV2 tempClient = null;
        try {
            tempClient = new MilvusClientV2(config);

            // use the temp client to insert correct data into the default collection
            // there will be an entry for this collection in the process-global schema cache
            // there will be a timestamp for this collection in the global timestamp cache
            JsonObject row = new JsonObject();
            row.addProperty("pk", 8);
            row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(DIMENSION)));
            InsertResp insertResp = tempClient.insert(
                    InsertReq.builder().databaseName("default").collectionName(randomCollectionName)
                            .data(Collections.singletonList(row)).build());
            Assertions.assertEquals(1L, insertResp.getInsertCnt());

            // check the timestamp of this collection, must be positive
            long ts11 = CollectionTsCache.getInstance().get("localhost:29530", "default", randomCollectionName);
            Assertions.assertTrue(ts11 > 0L);

            // insert wrong data; the refreshed, valid collection schema remains cached
            row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(7)));
            Assertions.assertThrows(MilvusClientException.class, () -> client.insert(InsertReq.builder()
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
            long ts12 = CollectionTsCache.getInstance().get("localhost:29530", "default", randomCollectionName);
            Assertions.assertTrue(ts12 > ts11);

            // create a new collection with the same name, different schema, in the test db
            createSimpleCollection(tempClient, "", randomCollectionName, "aaa", false, 4, ConsistencyLevel.BOUNDED);

            // use the temp client to insert wrong data, wrong dimension
            row.remove("pk");
            row.addProperty("aaa", 22);
            row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(7)));
            MilvusClientV2 finalTempClient = tempClient;
            Assertions.assertThrows(MilvusClientException.class, () -> finalTempClient.insert(InsertReq.builder()
                    .collectionName(randomCollectionName)
                    .data(Collections.singletonList(row))
                    .build()));

            // check the timestamp of this collection, must be null
            long ts21 = CollectionTsCache.getInstance().get("localhost:29530", testDbName, randomCollectionName);
            Assertions.assertEquals(0L, ts21);

            // use the temp client to do upsert correct data
            TimeUnit.MILLISECONDS.sleep(100);
            row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(4)));
            upsertResp = tempClient.upsert(UpsertReq.builder()
                    .collectionName(randomCollectionName)
                    .data(Collections.singletonList(row))
                    .build());
            Assertions.assertEquals(1L, upsertResp.getUpsertCnt());

            // check the timestamp of this collection, must be positive
            long ts22 = CollectionTsCache.getInstance().get("localhost:29530", testDbName, randomCollectionName);
            Assertions.assertTrue(ts22 > 0L);

            // tempClient delete data
            tempClient.delete(DeleteReq.builder()
                    .collectionName(randomCollectionName)
                    .ids(Collections.singletonList(22L))
                    .build());

            // check the timestamp of this collection, must be greater than previous
            long ts23 = CollectionTsCache.getInstance().get("localhost:29530", testDbName, randomCollectionName);
            Assertions.assertTrue(ts23 > ts22);

            // use the default client to drop the collection in the new db
            client.dropCollection(DropCollectionReq.builder()
                    .databaseName(testDbName)
                    .collectionName(randomCollectionName)
                    .build());

            // check the timestamp of this collection, must be deleted
            long ts31 = CollectionTsCache.getInstance().get("localhost:29530", testDbName, randomCollectionName);
            Assertions.assertEquals(0L, ts31);
        } finally {
            if (tempClient != null) {
                tempClient.close();
            }
        }
    }

    @Test
    void testOperationsAcrossDB() {
        // create a temp database
        String tempDatabaseName = "db_temp";
        Map<String, String> properties = new HashMap<>();
        properties.put(Constant.DATABASE_REPLICA_NUMBER, "5");
        CreateDatabaseReq createDatabaseReq = CreateDatabaseReq.builder()
                .databaseName(tempDatabaseName)
                .properties(properties)
                .build();
        client.createDatabase(createDatabaseReq);

        // create a collection in the temp database
        String randomCollectionName = generator.generate(10);
        String vectorFieldName = "float_vector";
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(true)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(vectorFieldName)
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .build();
        client.createCollection(requestCreate);

        // has collection
        Assertions.assertTrue(client.hasCollection(HasCollectionReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .build()));

        // list collections
        ListCollectionsResp listResp = client.listCollectionsV2(ListCollectionsReq.builder()
                .databaseName(tempDatabaseName)
                .build());
        Assertions.assertTrue(listResp.getCollectionNames().contains(randomCollectionName));

        // specify the temp database name to create index
        IndexParam indexParam = IndexParam.builder()
                .fieldName(vectorFieldName)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build();
        client.createIndex(CreateIndexReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .indexParams(Collections.singletonList(indexParam))
                .sync(true)
                .build());

        // specify the temp database name to list index
        List<String> indexes = client.listIndexes(ListIndexesReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .fieldName(vectorFieldName)
                .build());
        Assertions.assertTrue(indexes.contains(vectorFieldName));

        // specify the temp database name to insert
        JsonObject row = new JsonObject();
        row.add(vectorFieldName, JsonUtils.toJsonTree(utils.generateFloatVector(DIMENSION)));
        client.insert(InsertReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .data(Collections.singletonList(row))
                .build());

        // specify the temp database name to flush collection
        client.flush(FlushReq.builder()
                .databaseName(tempDatabaseName)
                .collectionNames(Collections.singletonList(randomCollectionName))
                .waitFlushedTimeoutMs(5000L)
                .build());

        // specify the temp database name to compact collection
        client.compact(CompactReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .build());

        // specify the temp database name to load collection
        client.loadCollection(LoadCollectionReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .sync(true)
                .build());

        // specify the temp database name to release collection
        client.releaseCollection(ReleaseCollectionReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .build());

        // specify the temp database name to get load state of collection
        Assertions.assertFalse(client.getLoadState(GetLoadStateReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .build()));

        // create a partition in the temp database
        String partitionName = "temp_part";
        client.createPartition(CreatePartitionReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .partitionName(partitionName)
                .build());

        // has partition
        Assertions.assertTrue(client.hasPartition(HasPartitionReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .partitionName(partitionName)
                .build()));

        // list partitions
        List<String> partitions = client.listPartitions(ListPartitionsReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .build());
        Assertions.assertTrue(partitions.contains(partitionName));

        // specify the temp database name to load partition
        client.loadPartitions(LoadPartitionsReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .partitionNames(Collections.singletonList(partitionName))
                .sync(true)
                .build());

        // specify the temp database name to get detailed load state of partition
        GetLoadStateResp loadStateResp = client.getLoadStateV2(GetLoadStateReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .partitionName(partitionName)
                .build());
        Assertions.assertEquals(LoadState.LoadStateLoaded, loadStateResp.getState());
        Assertions.assertEquals(LoadState.LoadStateLoaded.name(), loadStateResp.getStateName());
        Assertions.assertNull(loadStateResp.getProgress());

        // specify the temp database name to release partition
        client.releasePartitions(ReleasePartitionsReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .partitionNames(Collections.singletonList(partitionName))
                .build());

        // specify the temp database name to drop partition
        client.dropPartition(DropPartitionReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .partitionName(partitionName)
                .build());

        // specify the temp database name to drop index
        client.dropIndex(DropIndexReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .fieldName(vectorFieldName)
                .build());

        // set target database name to rename collection
        // the renamed collection will be moved to the target database, and the source collection will be deleted
        String newCollName = "new_name";
        client.renameCollection(RenameCollectionReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(randomCollectionName)
                .newCollectionName(newCollName)
                .targetDbName("default")
                .build());

        Boolean has = client.hasCollection(HasCollectionReq.builder()
                .databaseName(tempDatabaseName)
                .collectionName(newCollName)
                .build());
        Assertions.assertFalse(has);

        has = client.hasCollection(HasCollectionReq.builder()
                .databaseName("default")
                .collectionName(newCollName)
                .build());
        Assertions.assertTrue(has);

        // since the renamed collection is in default db, no need to specify databaseName
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(newCollName)
                .build());

        has = client.hasCollection(HasCollectionReq.builder()
                .collectionName(newCollName)
                .build());
        Assertions.assertFalse(has);
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
        long rowCount = getRowCount("", randomCollectionName);
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

        client.dropCollection(DropCollectionReq.builder().collectionName(randomCollectionName).build());
    }

    @Test
    void testMinHashFunction() {
        String randomCollectionName = generator.generate(10);

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(true)
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.FALSE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("text")
                .dataType(DataType.VarChar)
                .maxLength(65535)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("minhash_signature")
                .dataType(DataType.BinaryVector)
                .dimension(512)
                .build());

        collectionSchema.addFunction(CreateCollectionReq.Function.builder()
                .name("text_to_minhash")
                .description("desc minhash")
                .functionType(FunctionType.MINHASH)
                .inputFieldNames(Collections.singletonList("text"))
                .outputFieldNames(Collections.singletonList("minhash_signature"))
                .param("num_hashes", "16")
                .param("shingle_size", "3")
                .param("token_level", "word")
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName("minhash_signature")
                .indexType(IndexParam.IndexType.MINHASH_LSH)
                .metricType(IndexParam.MetricType.MHJACCARD)
                .extraParams(new HashMap<String, Object>() {{
                    put("mh_lsh_band", 8);
                    put("with_raw_data", true);
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
        List<CreateCollectionReq.Function> functions = descResp.getCollectionSchema().getFunctionList();
        Assertions.assertEquals(1, functions.size());
        Assertions.assertEquals(FunctionType.MINHASH, functions.get(0).getFunctionType());
        Assertions.assertEquals("minhash_signature", functions.get(0).getOutputFieldNames().get(0));

        List<String> texts = Arrays.asList(
                "The quick brown fox jumps over the lazy dog.",
                "A quick brown fox jumped over a lazy dog.",
                "Machine learning is transforming artificial intelligence.");
        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < texts.size(); i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i + 1);
            row.addProperty("text", texts.get(i));
            data.add(row);
        }

        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(data)
                .build());
        Assertions.assertEquals(3, insertResp.getInsertCnt());
        long rowCount = getRowCount("", randomCollectionName);
        Assertions.assertEquals(texts.size(), rowCount);

        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField("minhash_signature")
                .data(Collections.singletonList(new EmbeddedText("The quick brown fox jumps over the lazy dog.")))
                .limit(3)
                .outputFields(Lists.newArrayList("id", "text"))
                .metricType(IndexParam.MetricType.MHJACCARD)
                .searchParams(new HashMap<String, Object>() {{
                    put("mh_search_with_jaccard", true);
                    put("refine_k", 50);
                }})
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        Assertions.assertEquals(1, searchResults.size());
        Assertions.assertFalse(searchResults.get(0).isEmpty());
        Map<String, Object> firstEntity = searchResults.get(0).get(0).getEntity();
        Assertions.assertTrue(firstEntity.containsKey("id"));
        Assertions.assertTrue(firstEntity.containsKey("text"));

        client.dropCollection(DropCollectionReq.builder().collectionName(randomCollectionName).build());
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
        Assertions.assertEquals(100L, (long) countR.getQueryResults().get(0).getEntity().get("count(*)"));

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

        client.dropCollectionField(DropCollectionFieldReq.builder()
                .collectionName(collectionName)
                .fieldName("flag")
                .build());
        descResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionName)
                .build());
        fieldNames = descResp.getFieldNames();
        Assertions.assertFalse(fieldNames.contains("flag"));
        Assertions.assertTrue(fieldNames.contains("text"));

        client.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }

    @Test
    void testFunctionFieldLifecycle() {
        String collectionName = generator.generate(10);
        createSimpleCollection(client, "", collectionName, "id", false, DIMENSION, ConsistencyLevel.BOUNDED);

        client.addCollectionField(AddCollectionFieldReq.builder()
                .collectionName(collectionName)
                .fieldName("text")
                .dataType(DataType.VarChar)
                .maxLength(100)
                .enableAnalyzer(true)
                .enableMatch(true)
                .analyzerParams(new HashMap<String, Object>() {{
                    put("tokenizer", "standard");
                }})
                .isNullable(true)
                .build());

        client.addFunctionField(AddFunctionFieldReq.builder()
                .collectionName(collectionName)
                .fieldName("sparse")
                .dataType(DataType.SparseFloatVector)
                .indexParam(IndexParam.builder()
                        .fieldName("sparse")
                        .indexName("sparse_idx")
                        .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                        .metricType(IndexParam.MetricType.BM25)
                        .extraParams(Collections.singletonMap("drop_ratio_build", 0.2))
                        .build())
                .function(CreateCollectionReq.Function.builder()
                        .name("bm25")
                        .description("desc bm25")
                        .functionType(FunctionType.BM25)
                        .inputFieldNames(Collections.singletonList("text"))
                        .outputFieldNames(Collections.singletonList("sparse"))
                        .build())
                .build());

        DescribeCollectionResp descResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionName)
                .build());
        List<String> fieldNames = descResp.getFieldNames();
        Assertions.assertTrue(fieldNames.contains("text"));
        Assertions.assertTrue(fieldNames.contains("sparse"));
        List<CreateCollectionReq.Function> functions = descResp.getCollectionSchema().getFunctionList();
        Assertions.assertEquals(1, functions.size());
        Assertions.assertEquals("bm25", functions.get(0).getName());
        Assertions.assertEquals("sparse", functions.get(0).getOutputFieldNames().get(0));

        DescribeIndexResp indexResp = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(collectionName)
                .indexName("sparse_idx")
                .build());
        DescribeIndexResp.IndexDesc sparseIndex = indexResp.getIndexDescByIndexName("sparse_idx");
        Assertions.assertNotNull(sparseIndex);
        Assertions.assertEquals("sparse", sparseIndex.getFieldName());
        Assertions.assertEquals(IndexParam.IndexType.SPARSE_INVERTED_INDEX, sparseIndex.getIndexType());
        Assertions.assertEquals(IndexParam.MetricType.BM25, sparseIndex.getMetricType());
        Assertions.assertEquals("0.2", sparseIndex.getExtraParams().get("drop_ratio_build"));

        client.dropFunctionField(DropFunctionFieldReq.builder()
                .collectionName(collectionName)
                .functionName("bm25")
                .build());

        descResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionName)
                .build());
        fieldNames = descResp.getFieldNames();
        Assertions.assertTrue(fieldNames.contains("text"));
        Assertions.assertFalse(fieldNames.contains("sparse"));
        Assertions.assertTrue(descResp.getCollectionSchema().getFunctionList().isEmpty());
        List<String> indexNames = client.listIndexes(ListIndexesReq.builder()
                .collectionName(collectionName)
                .build());
        Assertions.assertFalse(indexNames.contains("sparse_idx"));

        client.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }

}
