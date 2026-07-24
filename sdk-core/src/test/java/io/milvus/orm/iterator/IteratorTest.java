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

package io.milvus.orm.iterator;

import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.IDs;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.QueryRequest;
import io.milvus.grpc.QueryResults;
import io.milvus.grpc.ScalarField;
import io.milvus.grpc.SearchRequest;
import io.milvus.grpc.SearchResultData;
import io.milvus.grpc.SearchResults;
import io.milvus.grpc.Status;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.SearchIteratorReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IteratorTest {
    @Test
    void queryIteratorDrainsCachedResultsAndReleasesCacheAtLimit() throws ReflectiveOperationException {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(
                queryResults(Collections.emptyList(), 100L),
                queryResults(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L), 100L));

        QueryIterator iterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("id"))
                        .batchSize(2)
                        .limit(3)
                        .build(),
                new RpcStubWrapper(stub, 0L),
                primaryField());

        assertEquals(Arrays.asList(1L, 2L), ids(iterator.next()));
        assertEquals(Collections.singletonList(3L), ids(iterator.next()));
        assertEquals(3L, longField(iterator, "returnedCount"));
        assertEquals(0, cacheSize(iterator, "cacheIdInUse"));

        assertTrue(iterator.next().isEmpty());
        verify(stub, times(2)).query(any(QueryRequest.class));
    }

    @Test
    void searchIteratorDrainsCachedResultsAndReleasesCacheAtLimit() throws ReflectiveOperationException {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.search(any(SearchRequest.class))).thenReturn(searchResults(1L, 2L, 3L, 4L, 5L));

        SearchIterator iterator = new SearchIterator(
                SearchIteratorReq.builder()
                        .collectionName("test")
                        .vectorFieldName("vector")
                        .metricType(IndexParam.MetricType.L2)
                        .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                        .batchSize(2)
                        .limit(3)
                        .build(),
                new RpcStubWrapper(stub, 0L),
                primaryField());

        assertEquals(Arrays.asList(1L, 2L), ids(iterator.next()));
        assertEquals(Collections.singletonList(3L), ids(iterator.next()));
        assertEquals(3L, longField(iterator, "returnedCount"));
        assertEquals(0, cacheSize(iterator, "cacheId"));

        assertTrue(iterator.next().isEmpty());
        verify(stub).search(any(SearchRequest.class));
    }

    @Test
    void queryIteratorWithZeroBatchSizeReturnsEmptyPage() throws ReflectiveOperationException {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(queryResults(Collections.emptyList(), 100L));

        QueryIterator iterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .batchSize(0)
                        .limit(3)
                        .build(),
                new RpcStubWrapper(stub, 0L),
                primaryField());

        assertTrue(iterator.next().isEmpty());
        assertEquals(0, cacheSize(iterator, "cacheIdInUse"));
        verify(stub).query(any(QueryRequest.class));
    }

    private static QueryResults queryResults(List<Long> ids, long sessionTs) {
        QueryResults.Builder builder = QueryResults.newBuilder()
                .setStatus(successStatus())
                .setSessionTs(sessionTs);
        if (!ids.isEmpty()) {
            builder.addOutputFields("id")
                    .addFieldsData(FieldData.newBuilder()
                            .setFieldName("id")
                            .setType(DataType.Int64)
                            .setScalars(ScalarField.newBuilder()
                                    .setLongData(LongArray.newBuilder().addAllData(ids).build())
                                    .build())
                            .build());
        }
        return builder.build();
    }

    private static SearchResults searchResults(Long... ids) {
        SearchResultData.Builder data = SearchResultData.newBuilder()
                .setPrimaryFieldName("id")
                .setNumQueries(1)
                .setTopK(ids.length)
                .addTopks(ids.length)
                .setIds(IDs.newBuilder()
                        .setIntId(LongArray.newBuilder().addAllData(Arrays.asList(ids)).build())
                        .build());
        for (int i = 0; i < ids.length; i++) {
            data.addScores((float) i);
        }
        return SearchResults.newBuilder()
                .setStatus(successStatus())
                .setSessionTs(100L)
                .setResults(data.build())
                .build();
    }

    private static Status successStatus() {
        return Status.newBuilder().setCode(0).build();
    }

    private static CreateCollectionReq.FieldSchema primaryField() {
        return CreateCollectionReq.FieldSchema.builder()
                .name("id")
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .build();
    }

    private static List<Long> ids(List<QueryResultsWrapper.RowRecord> records) {
        List<Long> ids = new ArrayList<>();
        for (QueryResultsWrapper.RowRecord record : records) {
            ids.add((Long) record.get("id"));
        }
        return ids;
    }

    private static int cacheSize(Object iterator, String cacheIdFieldName)
            throws ReflectiveOperationException {
        Field cacheField = iterator.getClass().getDeclaredField("iteratorCache");
        cacheField.setAccessible(true);
        IteratorCache cache = (IteratorCache) cacheField.get(iterator);

        Field cacheIdField = iterator.getClass().getDeclaredField(cacheIdFieldName);
        cacheIdField.setAccessible(true);
        return cache.size(cacheIdField.getInt(iterator));
    }

    private static long longField(Object object, String fieldName) throws ReflectiveOperationException {
        Field field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getLong(object);
    }
}
