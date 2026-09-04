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
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.client.MilvusClientV2Session;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.support.TestUtils;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.orm.iterator.SearchIteratorV2;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.ranker.RRFRanker;
import io.milvus.v2.service.vector.response.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Tag("system")
class MilvusClientV2SessionDockerTest {
    private static MilvusClientV2 client;
    private static final String COLLECTION_NAME = "session_test_collection";
    private static final int DIMENSION = 4;
    private static final File DockerComposeFile = TestUtils.dockerComposeFile("docker-compose.yml");
    private static final File DockerComposeVolumeDirectory = new File("target/milvus-compose");
    private static final List<String> DockerComposeContainerNames = Arrays.asList(
            "milvus-javasdk-etcd", "milvus-javasdk-minio", "milvus-javasdk-standalone");

    @BeforeAll
    public static void setUp() {
        TestUtils.startMilvusStandalone(DockerComposeFile, DockerComposeVolumeDirectory, DockerComposeContainerNames);

        ConnectConfig config = ConnectConfig.builder()
                .uri(TestUtils.MilvusStandaloneUri)
                .build();
        client = new MilvusClientV2(config);
        prepareCollection();
    }

    @AfterAll
    public static void tearDown() throws InterruptedException {
        try {
            if (client != null) {
                client.dropCollection(DropCollectionReq.builder()
                        .collectionName(COLLECTION_NAME)
                        .build());
                client.close(5L);
            }
        } finally {
            TestUtils.stopMilvusStandalone();
        }
    }

    private static void prepareCollection() {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("name")
                .dataType(DataType.VarChar)
                .maxLength(128)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .build());

        Gson gson = new Gson();
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            row.addProperty("name", "item-" + i);
            row.add("vector", gson.toJsonTree(new float[]{
                    (float) i, (float) i / 2, (float) i / 3, (float) i / 4}));
            rows.add(row);
        }
        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());

        client.createIndex(CreateIndexReq.builder()
                .collectionName(COLLECTION_NAME)
                .indexParams(Collections.singletonList(IndexParam.builder()
                        .fieldName("vector")
                        .indexType(IndexParam.IndexType.FLAT)
                        .metricType(IndexParam.MetricType.L2)
                        .build()))
                .build());
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
    }

    private static FloatVec queryVector() {
        return new FloatVec(new float[]{1.0f, 0.5f, 0.33f, 0.25f});
    }

    @Test
    void testGetClusterId() {
        MilvusClientV2Session session = client.session("cluster-a");
        Assertions.assertEquals("cluster-a", session.getClusterId());
    }

    @Test
    void testSessionQuery() {
        MilvusClientV2Session session = client.session("cluster-a");

        QueryResp resp = session.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter("id >= 0 and id < 5")
                .outputFields(Arrays.asList("id", "name"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assertions.assertEquals(5, resp.getQueryResults().size());
        Assertions.assertEquals("item-0", resp.getQueryResults().get(0).getEntity().get("name"));
    }

    @Test
    void testSessionQueryAsync() throws Exception {
        MilvusClientV2Session session = client.session("cluster-a");

        CompletableFuture<QueryResp> future = session.queryAsync(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter("id >= 5 and id < 10")
                .outputFields(Arrays.asList("id", "name"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        QueryResp resp = future.get(10, TimeUnit.SECONDS);
        Assertions.assertEquals(5, resp.getQueryResults().size());
        Assertions.assertEquals("item-5", resp.getQueryResults().get(0).getEntity().get("name"));
    }

    @Test
    void testSessionQueryByIds() {
        MilvusClientV2Session session = client.session("cluster-a");

        QueryResp resp = session.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .ids(Arrays.asList(1L, 3L, 5L))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assertions.assertEquals(3, resp.getQueryResults().size());
    }

    @Test
    void testSessionSearch() {
        MilvusClientV2Session session = client.session("cluster-a");

        SearchResp resp = session.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .annsField("vector")
                .data(Collections.singletonList(queryVector()))
                .limit(5)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        List<List<SearchResp.SearchResult>> results = resp.getSearchResults();
        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals(5, results.get(0).size());
        Assertions.assertEquals(1L, results.get(0).get(0).getId());
    }

    @Test
    void testSessionSearchAsync() throws Exception {
        MilvusClientV2Session session = client.session("cluster-a");

        CompletableFuture<SearchResp> future = session.searchAsync(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .annsField("vector")
                .data(Collections.singletonList(queryVector()))
                .limit(5)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        SearchResp resp = future.get(10, TimeUnit.SECONDS);
        List<List<SearchResp.SearchResult>> results = resp.getSearchResults();
        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals(5, results.get(0).size());
        Assertions.assertEquals(1L, results.get(0).get(0).getId());
    }

    @Test
    void testSessionHybridSearch() {
        MilvusClientV2Session session = client.session("cluster-a");

        AnnSearchReq annReq1 = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(queryVector()))
                .limit(5)
                .build();
        AnnSearchReq annReq2 = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(new FloatVec(new float[]{2.0f, 1.0f, 0.67f, 0.5f})))
                .limit(5)
                .build();

        SearchResp resp = session.hybridSearch(HybridSearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .searchRequests(Arrays.asList(annReq1, annReq2))
                .ranker(RRFRanker.builder().k(60).build())
                .limit(5)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        List<List<SearchResp.SearchResult>> results = resp.getSearchResults();
        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals(5, results.get(0).size());
        Assertions.assertEquals(1L, results.get(0).get(0).getId());
    }

    @Test
    void testSessionHybridSearchAsync() throws Exception {
        MilvusClientV2Session session = client.session("cluster-a");

        AnnSearchReq annReq1 = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(queryVector()))
                .limit(5)
                .build();
        AnnSearchReq annReq2 = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(new FloatVec(new float[]{2.0f, 1.0f, 0.67f, 0.5f})))
                .limit(5)
                .build();

        CompletableFuture<SearchResp> future = session.hybridSearchAsync(HybridSearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .searchRequests(Arrays.asList(annReq1, annReq2))
                .ranker(RRFRanker.builder().k(60).build())
                .limit(5)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        SearchResp resp = future.get(10, TimeUnit.SECONDS);
        List<List<SearchResp.SearchResult>> results = resp.getSearchResults();
        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals(5, results.get(0).size());
    }

    @Test
    void testSessionGet() {
        MilvusClientV2Session session = client.session("cluster-a");

        GetResp resp = session.get(GetReq.builder()
                .collectionName(COLLECTION_NAME)
                .ids(Arrays.asList(1L, 2L, 3L))
                .outputFields(Collections.singletonList("*"))
                .build());
        Assertions.assertEquals(3, resp.getGetResults().size());
    }

    @Test
    void testSessionGetAsync() throws Exception {
        MilvusClientV2Session session = client.session("cluster-a");

        CompletableFuture<GetResp> future = session.getAsync(GetReq.builder()
                .collectionName(COLLECTION_NAME)
                .ids(Arrays.asList(1L, 2L, 3L))
                .outputFields(Collections.singletonList("*"))
                .build());
        GetResp resp = future.get(10, TimeUnit.SECONDS);
        Assertions.assertEquals(3, resp.getGetResults().size());
    }

    @Test
    void testSessionQueryIterator() {
        MilvusClientV2Session session = client.session("cluster-a");

        QueryIterator iterator = session.queryIterator(QueryIteratorReq.builder()
                .collectionName(COLLECTION_NAME)
                .expr("id >= 0")
                .batchSize(10)
                .build());
        int count = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> page = iterator.next();
            if (page.isEmpty()) {
                iterator.close();
                break;
            }
            count += page.size();
        }
        Assertions.assertEquals(100, count);
    }

    @Test
    void testSessionSearchIterator() {
        MilvusClientV2Session session = client.session("cluster-a");

        SearchIterator iterator = session.searchIterator(SearchIteratorReq.builder()
                .collectionName(COLLECTION_NAME)
                .vectorFieldName("vector")
                .metricType(IndexParam.MetricType.L2)
                .vectors(Collections.singletonList(queryVector()))
                .limit(20)
                .batchSize(10)
                .build());
        int count = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> page = iterator.next();
            if (page.isEmpty()) {
                iterator.close();
                break;
            }
            count += page.size();
        }
        Assertions.assertEquals(20, count);
    }

    @Test
    void testSessionSearchIteratorV2() {
        MilvusClientV2Session session = client.session("cluster-a");

        SearchIteratorV2 iterator = session.searchIteratorV2(SearchIteratorReqV2.builder()
                .collectionName(COLLECTION_NAME)
                .vectorFieldName("vector")
                .metricType(IndexParam.MetricType.L2)
                .vectors(Collections.singletonList(queryVector()))
                .limit(20)
                .batchSize(10)
                .build());
        int count = 0;
        while (true) {
            List<SearchResp.SearchResult> page = iterator.next();
            if (page.isEmpty()) {
                iterator.close();
                break;
            }
            count += page.size();
        }
        Assertions.assertEquals(20, count);
    }

    @Test
    void testSessionClosedRejectsOperations() {
        MilvusClientV2Session session = client.session("cluster-a");
        session.close();

        Assertions.assertThrows(MilvusClientException.class, () -> session.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .build()));
        Assertions.assertThrows(MilvusClientException.class, () -> session.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .annsField("vector")
                .data(Collections.singletonList(queryVector()))
                .limit(5)
                .build()));
        Assertions.assertThrows(MilvusClientException.class, () -> session.hybridSearch(HybridSearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .searchRequests(Collections.singletonList(AnnSearchReq.builder()
                        .vectorFieldName("vector")
                        .vectors(Collections.singletonList(queryVector()))
                        .limit(5)
                        .build()))
                .limit(5)
                .build()));
        Assertions.assertThrows(MilvusClientException.class, () -> session.get(GetReq.builder()
                .collectionName(COLLECTION_NAME)
                .ids(Collections.singletonList(1L))
                .build()));
    }
}
