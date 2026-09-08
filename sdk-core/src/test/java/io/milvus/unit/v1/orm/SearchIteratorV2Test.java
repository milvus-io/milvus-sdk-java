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

package io.milvus.unit.v1.orm;
import io.milvus.orm.iterator.RpcStubWrapper;
import io.milvus.orm.iterator.SearchIteratorV2;

import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionRequest;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.IDs;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.SearchIteratorV2Results;
import io.milvus.grpc.SearchRequest;
import io.milvus.grpc.SearchResultData;
import io.milvus.grpc.SearchResults;
import io.milvus.grpc.Status;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.SearchIteratorReqV2;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.SearchResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class SearchIteratorV2Test {
    @Test
    void probePinsGuaranteeTimestampForFirstPage() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(
                searchResults(12345678L), searchResults(22345678L));

        SearchIteratorV2 iterator = new SearchIteratorV2(request(new HashMap<>()),
                testStubWrapper(stub));
        iterator.next();

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(stub, times(2)).search(captor.capture());
        assertEquals(0L, captor.getAllValues().get(0).getGuaranteeTimestamp());
        assertEquals(12345678L, captor.getAllValues().get(1).getGuaranteeTimestamp());
    }

    @Test
    void probeUsesClientTimestampWhenSessionTimestampIsZero() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(
                searchResults(0L), searchResults(0L));

        SearchIteratorV2 iterator = new SearchIteratorV2(request(new HashMap<>()),
                testStubWrapper(stub));
        iterator.next();

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(stub, times(2)).search(captor.capture());
        assertTrue(captor.getAllValues().get(1).getGuaranteeTimestamp() > 0L);
    }

    @Test
    void probePreservesExplicitGuaranteeTimestamp() {
        Map<String, Object> searchParams = new HashMap<>();
        searchParams.put("guarantee_timestamp", 42);
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(
                searchResults(12345678L), searchResults(22345678L));

        SearchIteratorV2 iterator = new SearchIteratorV2(request(searchParams),
                testStubWrapper(stub));
        iterator.next();

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(stub, times(2)).search(captor.capture());
        assertEquals(42L, captor.getAllValues().get(0).getGuaranteeTimestamp());
        assertEquals(42L, captor.getAllValues().get(1).getGuaranteeTimestamp());
    }

    @Test
    void limitLargerThanIntegerMaxValueDoesNotAppearExhausted() {
        long limit = (long) Integer.MAX_VALUE + 1L;
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(
                searchResults(12345678L), searchResults(22345678L));

        SearchIteratorV2 iterator = new SearchIteratorV2(request(new HashMap<>(), limit),
                testStubWrapper(stub));
        iterator.next();

        verify(stub, times(2)).search(any(SearchRequest.class));
    }

    @Test
    void truncatedFinalPageIsAnIndependentCopy() throws ReflectiveOperationException {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(
                searchResults(12345678L), searchResults(22345678L, 1L, 2L));

        SearchIteratorV2 iterator = new SearchIteratorV2(request(new HashMap<>(), 1L),
                testStubWrapper(stub));
        List<SearchResp.SearchResult> result = iterator.next();

        assertEquals(1, result.size());
        assertEquals(ArrayList.class, result.getClass());

        java.lang.reflect.Field leftResCntField = SearchIteratorV2.class.getDeclaredField("leftResCnt");
        leftResCntField.setAccessible(true);
        assertEquals(0L, leftResCntField.get(iterator));
    }

    @Test
    void externalFilterCacheIsClearedWhenLimitIsReached() throws ReflectiveOperationException {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(
                searchResults(12345678L), searchResults(22345678L, 1L, 2L));
        SearchIteratorReqV2 req = request(new HashMap<>(), 1L);
        req.setExternalFilterFunc(hits -> hits);

        SearchIteratorV2 iterator = new SearchIteratorV2(req, testStubWrapper(stub));
        List<SearchResp.SearchResult> result = iterator.next();

        assertEquals(1, result.size());
        assertEquals(0, cacheSize(iterator));
    }

    @Test
    void closeClearsExternalFilterCache() throws ReflectiveOperationException {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(
                searchResults(12345678L), searchResults(22345678L, 1L, 2L, 3L));
        SearchIteratorReqV2 req = request(new HashMap<>(), 3L);
        req.setExternalFilterFunc(hits -> hits);

        SearchIteratorV2 iterator = new SearchIteratorV2(req, testStubWrapper(stub));
        iterator.next();
        assertEquals(1, cacheSize(iterator));

        iterator.close();
        assertEquals(0, cacheSize(iterator));
    }

    private static int cacheSize(SearchIteratorV2 iterator) throws ReflectiveOperationException {
        java.lang.reflect.Field cacheField = SearchIteratorV2.class.getDeclaredField("cache");
        cacheField.setAccessible(true);
        return ((List<?>) cacheField.get(iterator)).size();
    }

    private static RpcStubWrapper testStubWrapper(
            MilvusServiceGrpc.MilvusServiceBlockingStub stub) {
        return new RpcStubWrapper(stub, 0L, "host:19530", "default");
    }

    private static MilvusServiceGrpc.MilvusServiceBlockingStub mockStub(
            SearchResults probeResponse, SearchResults pageResponse) {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.describeCollection(any(DescribeCollectionRequest.class)))
                .thenReturn(describeCollectionResponse());
        when(stub.search(any(SearchRequest.class))).thenReturn(probeResponse, pageResponse);
        return stub;
    }

    private static SearchIteratorReqV2 request(Map<String, Object> searchParams) {
        return request(searchParams, -1L);
    }

    private static SearchIteratorReqV2 request(Map<String, Object> searchParams, long limit) {
        return SearchIteratorReqV2.builder()
                .collectionName("test")
                .vectorFieldName("vector")
                .metricType(IndexParam.MetricType.L2)
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .batchSize(2)
                .limit(limit)
                .searchParams(searchParams)
                .build();
    }

    private static DescribeCollectionResponse describeCollectionResponse() {
        return DescribeCollectionResponse.newBuilder()
                .setStatus(successStatus())
                .setCollectionName("test")
                .setCollectionID(100L)
                .setSchema(CollectionSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setName("id")
                                .setDataType(DataType.Int64)
                                .setIsPrimaryKey(true)
                                .build())
                        .addFields(FieldSchema.newBuilder()
                                .setName("vector")
                                .setDataType(DataType.FloatVector)
                                .build())
                        .build())
                .build();
    }

    private static SearchResults searchResults(long sessionTs) {
        return searchResults(sessionTs, new Long[0]);
    }

    private static SearchResults searchResults(long sessionTs, Long... ids) {
        SearchResultData.Builder resultBuilder = SearchResultData.newBuilder()
                .setNumQueries(1)
                .setTopK(ids.length)
                .addTopks(ids.length)
                .setSearchIteratorV2Results(SearchIteratorV2Results.newBuilder()
                        .setToken("token")
                        .build());
        if (ids.length > 0) {
            resultBuilder.setIds(IDs.newBuilder()
                    .setIntId(LongArray.newBuilder().addAllData(Arrays.asList(ids)).build())
                    .build());
            for (int i = 0; i < ids.length; i++) {
                resultBuilder.addScores((float) i);
            }
        }
        return SearchResults.newBuilder()
                .setStatus(successStatus())
                .setSessionTs(sessionTs)
                .setResults(resultBuilder.build())
                .build();
    }

    private static Status successStatus() {
        return Status.newBuilder().setCode(0).build();
    }
}
