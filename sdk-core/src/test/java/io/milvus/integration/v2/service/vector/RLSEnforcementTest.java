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

package io.milvus.integration.v2.service.vector;

import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.DeleteRequest;
import io.milvus.grpc.HybridSearchRequest;
import io.milvus.grpc.InsertRequest;
import io.milvus.grpc.QueryRequest;
import io.milvus.grpc.SearchRequest;
import io.milvus.grpc.UpsertRequest;
import io.milvus.grpc.SearchResults;
import io.milvus.grpc.SearchResultData;
import io.milvus.grpc.SearchIteratorV2Results;
import io.milvus.grpc.Status;
import io.milvus.support.v2.BaseTest;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.GetReq;
import io.milvus.v2.service.vector.request.HybridSearchReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchIteratorReq;
import io.milvus.v2.service.vector.request.SearchIteratorReqV2;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("integration")
class RLSEnforcementTest extends BaseTest {

    private List<JsonObject> sampleData() {
        List<JsonObject> data = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        row.add("vector", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));
        data.add(row);
        return data;
    }

    @Test
    void testInsertCarriesRlsFields() {
        InsertReq request = InsertReq.builder()
                .collectionName("test")
                .data(sampleData())
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();
        client_v2.insert(request);

        ArgumentCaptor<InsertRequest> captor = ArgumentCaptor.forClass(InsertRequest.class);
        verify(blockingStub).insert(captor.capture());
        assertEquals("alice", captor.getValue().getRlsPrincipal());
        assertEquals(false, captor.getValue().getSkipRls());
    }

    @Test
    void testUpsertCarriesRlsFields() {
        UpsertReq request = UpsertReq.builder()
                .collectionName("test")
                .data(sampleData())
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();
        client_v2.upsert(request);

        ArgumentCaptor<UpsertRequest> captor = ArgumentCaptor.forClass(UpsertRequest.class);
        verify(blockingStub).upsert(captor.capture());
        assertEquals("alice", captor.getValue().getRlsPrincipal());
        assertEquals(false, captor.getValue().getSkipRls());
    }

    @Test
    void testDeleteCarriesRlsFields() {
        DeleteReq request = DeleteReq.builder()
                .collectionName("test")
                .filter("id > 0")
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();
        client_v2.delete(request);

        ArgumentCaptor<DeleteRequest> captor = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(blockingStub).delete(captor.capture());
        assertEquals("alice", captor.getValue().getRlsPrincipal());
        assertEquals(false, captor.getValue().getSkipRls());
    }

    @Test
    void testSearchCarriesRlsFields() {
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();
        client_v2.search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        assertEquals("alice", captor.getValue().getRlsPrincipal());
        assertEquals(false, captor.getValue().getSkipRls());
    }

    @Test
    void testQueryCarriesRlsFields() {
        QueryReq request = QueryReq.builder()
                .collectionName("test")
                .filter("id > 0")
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();
        client_v2.query(request);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        assertEquals("alice", captor.getValue().getRlsPrincipal());
        assertEquals(false, captor.getValue().getSkipRls());
    }

    @Test
    void testGetCarriesRlsFields() {
        GetReq request = GetReq.builder()
                .collectionName("test")
                .ids(Collections.singletonList(1L))
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();
        client_v2.get(request);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        assertEquals("alice", captor.getValue().getRlsPrincipal());
        assertEquals(false, captor.getValue().getSkipRls());
    }

    @Test
    void testQueryIteratorCarriesRlsFields() throws ReflectiveOperationException {
        setIteratorConnectConfig();
        QueryIteratorReq request = QueryIteratorReq.builder()
                .collectionName("test")
                .expr("id > 0")
                .batchSize(10)
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();

        client_v2.queryIterator(request);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        assertEquals("alice", captor.getValue().getRlsPrincipal());
        assertEquals(false, captor.getValue().getSkipRls());
    }

    @Test
    void testSearchIteratorCarriesRlsFields() throws ReflectiveOperationException {
        setIteratorConnectConfig();
        SearchIteratorReq request = SearchIteratorReq.builder()
                .collectionName("test")
                .vectorFieldName("vector")
                .metricType(IndexParam.MetricType.L2)
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .batchSize(10)
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();

        client_v2.searchIterator(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        assertEquals("alice", captor.getValue().getRlsPrincipal());
        assertEquals(false, captor.getValue().getSkipRls());
    }

    @Test
    void testSearchIteratorV2CarriesRlsFields() throws ReflectiveOperationException {
        setIteratorConnectConfig();
        SearchResults iteratorResponse = SearchResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .setResults(SearchResultData.newBuilder()
                        .setSearchIteratorV2Results(SearchIteratorV2Results.newBuilder().setToken("token").build())
                        .build())
                .build();
        when(blockingStub.search(any(SearchRequest.class))).thenReturn(iteratorResponse);

        SearchIteratorReqV2 request = SearchIteratorReqV2.builder()
                .collectionName("test")
                .vectorFieldName("vector")
                .metricType(IndexParam.MetricType.L2)
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .batchSize(10)
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();

        client_v2.searchIteratorV2(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        assertEquals("alice", captor.getValue().getRlsPrincipal());
        assertEquals(false, captor.getValue().getSkipRls());
    }

    private void setIteratorConnectConfig() throws ReflectiveOperationException {
        ConnectConfig config = ConnectConfig.builder().uri("http://localhost:19530").build();
        Field connectConfig = MilvusClientV2.class.getDeclaredField("connectConfig");
        connectConfig.setAccessible(true);
        connectConfig.set(client_v2, config);

        Field cacheEndpoint = MilvusClientV2.class.getDeclaredField("cacheEndpoint");
        cacheEndpoint.setAccessible(true);
        cacheEndpoint.set(client_v2, config.getHost() + ":" + config.getPort());
    }

    @Test
    void testHybridSearchCarriesRlsFields() {
        AnnSearchReq annSearchReq = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        HybridSearchReq request = HybridSearchReq.builder()
                .collectionName("test")
                .searchRequests(Collections.singletonList(annSearchReq))
                .limit(10)
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();
        client_v2.hybridSearch(request);

        ArgumentCaptor<HybridSearchRequest> captor = ArgumentCaptor.forClass(HybridSearchRequest.class);
        verify(blockingStub).hybridSearch(captor.capture());
        assertEquals("alice", captor.getValue().getRlsPrincipal());
        assertEquals(false, captor.getValue().getSkipRls());
    }
}
