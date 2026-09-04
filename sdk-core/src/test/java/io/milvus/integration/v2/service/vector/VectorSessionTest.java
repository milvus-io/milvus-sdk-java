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

import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.support.v2.BaseTest;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.FloatVec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("integration")
class VectorSessionTest extends BaseTest {

    @Test
    void testSessionReturnsClusterId() {
        Assertions.assertEquals("cluster-a", client_v2.session("cluster-a").getClusterId());
    }

    @Test
    void testSessionSearchPassesClusterId() {
        List<Float> vectorList = Arrays.asList(1.0f, 2.0f);
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(vectorList)))
                .limit(10)
                .build();

        client_v2.session("cluster-a").search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getSearchParamsList(), Constant.CLUSTER_ID));
        Assertions.assertNull(request.getClusterId());
    }

    @Test
    void testSessionQueryPassesClusterId() {
        QueryReq request = QueryReq.builder()
                .collectionName("test")
                .clusterId("cluster-b")
                .filter("id > 0")
                .build();

        client_v2.session("cluster-a").query(request);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getQueryParamsList(), Constant.CLUSTER_ID));
        Assertions.assertEquals(1, captor.getValue().getQueryParamsList().stream()
                .filter(param -> Constant.CLUSTER_ID.equals(param.getKey())).count());
        Assertions.assertEquals("cluster-b", request.getClusterId());
    }

    @Test
    void testSessionGetPassesClusterId() {
        GetReq request = GetReq.builder()
                .collectionName("test")
                .ids(Collections.singletonList(1L))
                .build();

        client_v2.session("cluster-a").get(request);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getQueryParamsList(), Constant.CLUSTER_ID));
        Assertions.assertNull(request.getClusterId());
    }

    @Test
    void testSessionHybridSearchPassesClusterId() {
        List<Float> vectorList = Arrays.asList(1.0f, 2.0f);
        AnnSearchReq annSearchReq = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(new FloatVec(vectorList)))
                .limit(10)
                .build();
        HybridSearchReq request = HybridSearchReq.builder()
                .collectionName("test")
                .clusterId("cluster-b")
                .searchRequests(Collections.singletonList(annSearchReq))
                .limit(10)
                .build();

        client_v2.session("cluster-a").hybridSearch(request);

        ArgumentCaptor<HybridSearchRequest> captor = ArgumentCaptor.forClass(HybridSearchRequest.class);
        verify(blockingStub).hybridSearch(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getRankParamsList(), Constant.CLUSTER_ID));
        Assertions.assertEquals(1, captor.getValue().getRankParamsList().stream()
                .filter(param -> Constant.CLUSTER_ID.equals(param.getKey())).count());
        Assertions.assertEquals("cluster-b", request.getClusterId());
    }

    @Test
    void testSessionAsyncOperationsPassClusterId() throws Exception {
        SearchReq searchRequest = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        QueryReq queryRequest = QueryReq.builder()
                .collectionName("test")
                .filter("id > 0")
                .build();
        AnnSearchReq annSearchReq = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        HybridSearchReq hybridSearchRequest = HybridSearchReq.builder()
                .collectionName("test")
                .searchRequests(Collections.singletonList(annSearchReq))
                .limit(10)
                .build();
        GetReq getRequest = GetReq.builder()
                .collectionName("test")
                .ids(Collections.singletonList(1L))
                .build();

        client_v2.session("cluster-a").searchAsync(searchRequest).get(1, TimeUnit.SECONDS);
        client_v2.session("cluster-a").queryAsync(queryRequest).get(1, TimeUnit.SECONDS);
        client_v2.session("cluster-a").hybridSearchAsync(hybridSearchRequest).get(1, TimeUnit.SECONDS);
        client_v2.session("cluster-a").getAsync(getRequest).get(1, TimeUnit.SECONDS);

        Assertions.assertNull(searchRequest.getClusterId());
        Assertions.assertNull(queryRequest.getClusterId());
        Assertions.assertNull(hybridSearchRequest.getClusterId());
        Assertions.assertNull(getRequest.getClusterId());

        ArgumentCaptor<SearchRequest> searchCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(futureStub).search(searchCaptor.capture());
        Assertions.assertEquals("cluster-a",
                getParam(searchCaptor.getValue().getSearchParamsList(), Constant.CLUSTER_ID));

        ArgumentCaptor<QueryRequest> queryCaptor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(futureStub, times(2)).query(queryCaptor.capture());
        for (QueryRequest rpcRequest : queryCaptor.getAllValues()) {
            Assertions.assertEquals("cluster-a",
                    getParam(rpcRequest.getQueryParamsList(), Constant.CLUSTER_ID));
        }

        ArgumentCaptor<HybridSearchRequest> hybridCaptor = ArgumentCaptor.forClass(HybridSearchRequest.class);
        verify(futureStub).hybridSearch(hybridCaptor.capture());
        Assertions.assertEquals("cluster-a",
                getParam(hybridCaptor.getValue().getRankParamsList(), Constant.CLUSTER_ID));
    }

    @Test
    void testSessionQueryIteratorPassesClusterId() throws ReflectiveOperationException {
        setIteratorConnectConfig();
        QueryIteratorReq request = QueryIteratorReq.builder()
                .collectionName("test")
                .expr("id > 0")
                .batchSize(10)
                .build();

        Assertions.assertNotNull(client_v2.session("cluster-a").queryIterator(request));

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getQueryParamsList(), Constant.CLUSTER_ID));
        Assertions.assertNull(request.getClusterId());
    }

    @Test
    void testSessionSearchIteratorPassesClusterId() throws ReflectiveOperationException {
        setIteratorConnectConfig();
        SearchIteratorReq request = SearchIteratorReq.builder()
                .collectionName("test")
                .vectorFieldName("vector")
                .metricType(IndexParam.MetricType.L2)
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .batchSize(10)
                .build();

        Assertions.assertNotNull(client_v2.session("cluster-a").searchIterator(request));

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getSearchParamsList(), Constant.CLUSTER_ID));
        Assertions.assertNull(request.getClusterId());
    }

    @Test
    void testSessionSearchIteratorV2PassesClusterId() throws ReflectiveOperationException {
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
                .build();

        Assertions.assertNotNull(client_v2.session("cluster-a").searchIteratorV2(request));

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getSearchParamsList(), Constant.CLUSTER_ID));
        Assertions.assertNull(request.getClusterId());
    }

    @Test
    void testSessionOverridesRequestClusterIdAndRejectsClosed() {
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .clusterId("cluster-b")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();

        client_v2.session("cluster-a").search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        Assertions.assertEquals("cluster-a", getParam(captor.getValue().getSearchParamsList(), Constant.CLUSTER_ID));
        Assertions.assertEquals(1, captor.getValue().getSearchParamsList().stream()
                .filter(param -> Constant.CLUSTER_ID.equals(param.getKey())).count());
        Assertions.assertEquals("cluster-b", request.getClusterId());

        io.milvus.v2.client.MilvusClientV2Session session = client_v2.session("cluster-a");
        session.close();
        SearchReq closedRequest = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        MilvusClientException closedException = Assertions.assertThrows(MilvusClientException.class,
                () -> session.search(closedRequest));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, closedException.getErrorCode());
        Assertions.assertNull(closedRequest.getClusterId());
    }

    @Test
    void testSearchSessionDeduplicatesRawClusterId() {
        Map<String, Object> rawParams = new HashMap<>();
        rawParams.put("nprobe", 10);
        rawParams.put(Constant.CLUSTER_ID, "user-cluster");
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .searchParams(rawParams)
                .build();

        client_v2.session("cluster-a").search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        List<KeyValuePair> searchParamsList = captor.getValue().getSearchParamsList();
        Assertions.assertEquals(1, searchParamsList.stream()
                .filter(param -> Constant.CLUSTER_ID.equals(param.getKey())).count());
        Assertions.assertEquals("cluster-a", getParam(searchParamsList, Constant.CLUSTER_ID));
        Assertions.assertEquals("10", getParam(searchParamsList, "nprobe"));
    }

    @Test
    void testSearchPreservesRawClusterIdWithoutSession() {
        Map<String, Object> rawParams = new HashMap<>();
        rawParams.put("nprobe", 10);
        rawParams.put(Constant.CLUSTER_ID, "user-cluster");
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .searchParams(rawParams)
                .build();

        client_v2.search(request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(blockingStub).search(captor.capture());
        List<KeyValuePair> searchParamsList = captor.getValue().getSearchParamsList();
        Assertions.assertEquals(1, searchParamsList.stream()
                .filter(param -> Constant.CLUSTER_ID.equals(param.getKey())).count());
        Assertions.assertEquals("user-cluster", getParam(searchParamsList, Constant.CLUSTER_ID));
    }

    @Test
    void testSearchAsyncSessionDeduplicatesRawClusterId() throws Exception {
        Map<String, Object> rawParams = new HashMap<>();
        rawParams.put("nprobe", 10);
        rawParams.put(Constant.CLUSTER_ID, "user-cluster");
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .searchParams(rawParams)
                .build();

        client_v2.session("cluster-a").searchAsync(request).get(1, TimeUnit.SECONDS);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(futureStub).search(captor.capture());
        List<KeyValuePair> searchParamsList = captor.getValue().getSearchParamsList();
        Assertions.assertEquals(1, searchParamsList.stream()
                .filter(param -> Constant.CLUSTER_ID.equals(param.getKey())).count());
        Assertions.assertEquals("cluster-a", getParam(searchParamsList, Constant.CLUSTER_ID));
        Assertions.assertEquals("10", getParam(searchParamsList, "nprobe"));
    }

    private void setIteratorConnectConfig() throws ReflectiveOperationException {
        // Iterator creation reads the RPC deadline and cache endpoint initialized by a real
        // MilvusClientV2 connection. BaseTest constructs the client without connecting, so inject
        // both values for these mocked iterator tests.
        ConnectConfig config = ConnectConfig.builder().uri("http://localhost:19530").build();
        Field connectConfig = MilvusClientV2.class.getDeclaredField("connectConfig");
        connectConfig.setAccessible(true);
        connectConfig.set(client_v2, config);

        Field cacheEndpoint = MilvusClientV2.class.getDeclaredField("cacheEndpoint");
        cacheEndpoint.setAccessible(true);
        cacheEndpoint.set(client_v2, config.getHost() + ":" + config.getPort());
    }

    private String getParam(List<KeyValuePair> params, String key) {
        return params.stream()
                .filter(param -> key.equals(param.getKey()))
                .map(KeyValuePair::getValue)
                .findFirst()
                .orElse(null);
    }
}
