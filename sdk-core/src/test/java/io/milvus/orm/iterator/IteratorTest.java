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
import io.milvus.grpc.ElementIndices;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.IDs;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.QueryRequest;
import io.milvus.grpc.QueryResults;
import io.milvus.grpc.ScalarField;
import io.milvus.grpc.SearchRequest;
import io.milvus.grpc.SearchResultData;
import io.milvus.grpc.SearchResults;
import io.milvus.grpc.Status;
import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.SearchIteratorReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IteratorTest {
    private static final long TEST_COLLECTION_ID = 1001L;

    @Test
    void rpcStubWrapperCarriesCacheIdentity() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);

        RpcStubWrapper wrapper = new RpcStubWrapper(stub, 0L, "host:19530", "db");

        assertEquals("host:19530", wrapper.getEndpoint());
        assertEquals("db", wrapper.getDatabaseName());
    }

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
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);

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
                testStubWrapper(stub),
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
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);

        assertTrue(iterator.next().isEmpty());
        assertEquals(0, cacheSize(iterator, "cacheIdInUse"));
        verify(stub).query(any(QueryRequest.class));
    }

    @Test
    void queryIteratorUsesPyMilvusFlagsForSetupSeekAndNext() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(
                queryResults(Collections.emptyList(), 100L),
                queryResults(Arrays.asList(1L, 2L), 100L),
                queryResults(Collections.singletonList(3L), 100L));

        QueryIterator iterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("id"))
                        .batchSize(10)
                        .offset(2)
                        .build(),
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);
        iterator.next();

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(stub, times(3)).query(captor.capture());
        List<QueryRequest> requests = captor.getAllValues();

        assertEquals("true", queryParam(requests.get(0), Constant.ITERATOR_FIELD));
        assertEquals("true", queryParam(requests.get(0), Constant.REDUCE_STOP_FOR_BEST));
        assertEquals("false", queryParam(requests.get(1), Constant.ITERATOR_FIELD));
        assertEquals("false", queryParam(requests.get(1), Constant.REDUCE_STOP_FOR_BEST));
        assertEquals("true", queryParam(requests.get(2), Constant.ITERATOR_FIELD));
        assertEquals("true", queryParam(requests.get(2), Constant.REDUCE_STOP_FOR_BEST));
        for (QueryRequest request : requests) {
            assertEquals(String.valueOf(TEST_COLLECTION_ID),
                    queryParam(request, Constant.COLLECTION_ID));
        }
    }

    @Test
    void queryIteratorSendsElementFilterCursor() {
        String filter = "element_filter(structA, $[int_val] >= 20000)";
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(
                queryResults(Collections.emptyList(), 100L),
                elementQueryResults(7L, 0L, 1L),
                queryResults(Collections.emptyList(), 100L));

        QueryIterator iterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("id"))
                        .expr(filter)
                        .batchSize(10)
                        .build(),
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);
        List<QueryResultsWrapper.RowRecord> firstPage = iterator.next();
        assertEquals(Arrays.asList(7L, 7L), ids(firstPage));
        assertEquals(Arrays.asList(0L, 1L), offsets(firstPage));
        iterator.next();

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(stub, times(3)).query(captor.capture());
        QueryRequest request = captor.getAllValues().get(2);
        assertEquals("id >= 7 and (" + filter + ")", request.getExpr());
        assertEquals("7", queryParam(request, Constant.QUERY_ITER_LAST_PK));
        assertEquals("1", queryParam(request, Constant.QUERY_ITER_LAST_ELEMENT_OFFSET));
    }

    @Test
    void queryIteratorGetCursorCapturesPosition() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(
                queryResults(Collections.emptyList(), 100L),
                queryResults(Arrays.asList(1L, 2L, 3L), 100L));

        QueryIterator iterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("id"))
                        .batchSize(10)
                        .build(),
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);
        iterator.next();

        QueryIteratorCursor cursor = iterator.getCursor();
        assertEquals(100L, cursor.getSessionTs());
        assertEquals(Long.valueOf(3L), cursor.getIntPk());
        assertNull(cursor.getStrPk());
        assertNull(cursor.getLastElementOffset());

        QueryIteratorCursor restored = QueryIteratorCursor.fromProto(cursor.toProto());
        assertEquals(100L, restored.getSessionTs());
        assertEquals(Long.valueOf(3L), restored.getIntPk());
        assertNull(restored.getStrPk());
    }

    @Test
    void queryIteratorGetCursorVarcharPk() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(
                varcharQueryResults(Collections.emptyList(), 100L),
                varcharQueryResults(Arrays.asList("a", "b", "c"), 100L));

        QueryIterator iterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("pk"))
                        .batchSize(10)
                        .build(),
                testStubWrapper(stub),
                varcharPrimaryField(),
                TEST_COLLECTION_ID);
        iterator.next();

        QueryIteratorCursor cursor = iterator.getCursor();
        assertEquals("c", cursor.getStrPk());
        assertNull(cursor.getIntPk());
    }

    @Test
    void queryIteratorGetCursorElementFilterCapturesOffset() {
        String filter = "element_filter(structA, $[int_val] >= 20000)";
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(
                queryResults(Collections.emptyList(), 100L),
                elementQueryResults(7L, 0L, 1L));

        QueryIterator iterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("id"))
                        .expr(filter)
                        .batchSize(10)
                        .build(),
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);
        iterator.next();

        QueryIteratorCursor cursor = iterator.getCursor();
        assertEquals(Long.valueOf(7L), cursor.getIntPk());
        assertEquals(Long.valueOf(1L), cursor.getLastElementOffset());
    }

    @Test
    void queryIteratorResumesFromCursor() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(
                queryResults(Collections.emptyList(), 100L),
                queryResults(Arrays.asList(1L, 2L), 100L),
                queryResults(Collections.singletonList(3L), 100L));

        QueryIterator iterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("id"))
                        .batchSize(10)
                        .build(),
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);
        iterator.next();
        QueryIteratorCursor cursor = iterator.getCursor();

        QueryIterator resumed = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("id"))
                        .batchSize(10)
                        .cursor(cursor)
                        .build(),
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);
        assertEquals(Collections.singletonList(3L), ids(resumed.next()));

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(stub, times(3)).query(captor.capture());
        QueryRequest resumedRequest = captor.getAllValues().get(2);
        assertEquals("id > 2", resumedRequest.getExpr());
        assertEquals(100L, resumedRequest.getGuaranteeTimestamp());
    }

    @Test
    void queryIteratorResumesElementFilterFromCursor() {
        String filter = "element_filter(structA, $[int_val] >= 20000)";
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(
                queryResults(Collections.emptyList(), 100L),
                elementQueryResults(7L, 0L, 1L),
                queryResults(Collections.emptyList(), 100L));

        QueryIterator iterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("id"))
                        .expr(filter)
                        .batchSize(10)
                        .build(),
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);
        iterator.next();
        QueryIteratorCursor cursor = iterator.getCursor();

        QueryIterator resumed = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("id"))
                        .expr(filter)
                        .batchSize(10)
                        .cursor(cursor)
                        .build(),
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);
        resumed.next();

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(stub, times(3)).query(captor.capture());
        QueryRequest resumedRequest = captor.getAllValues().get(2);
        assertEquals("id >= 7 and (" + filter + ")", resumedRequest.getExpr());
        assertEquals("7", queryParam(resumedRequest, Constant.QUERY_ITER_LAST_PK));
        assertEquals("1", queryParam(resumedRequest, Constant.QUERY_ITER_LAST_ELEMENT_OFFSET));
    }

    @Test
    void queryIteratorElementFilterCursorRejectsProtoSerialization() {
        String filter = "element_filter(structA, $[int_val] >= 20000)";
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(
                queryResults(Collections.emptyList(), 100L),
                elementQueryResults(7L, 0L, 1L));

        QueryIterator iterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("id"))
                        .expr(filter)
                        .batchSize(10)
                        .build(),
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);
        iterator.next();

        QueryIteratorCursor cursor = iterator.getCursor();
        assertNotNull(cursor.getLastElementOffset());
        assertThrows(ParamException.class, cursor::toProto);
    }

    @Test
    void queryIteratorResumeIgnoresRequestOffset() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(
                queryResults(Collections.emptyList(), 100L),
                queryResults(Arrays.asList(1L, 2L), 100L),
                queryResults(Collections.singletonList(3L), 100L));

        QueryIterator iterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("id"))
                        .batchSize(10)
                        .build(),
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);
        iterator.next();
        QueryIteratorCursor cursor = iterator.getCursor();

        QueryIterator resumed = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("id"))
                        .batchSize(10)
                        .offset(5)
                        .cursor(cursor)
                        .build(),
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);
        assertEquals(Collections.singletonList(3L), ids(resumed.next()));

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(stub, times(3)).query(captor.capture());
        QueryRequest resumedRequest = captor.getAllValues().get(2);
        assertNull(queryParam(resumedRequest, Constant.OFFSET));
    }

    @Test
    void queryIteratorReqToStringIncludesCursor() {
        QueryIteratorCursor cursor = QueryIteratorCursor.builder()
                .sessionTs(100L)
                .intPk(3L)
                .build();
        QueryIteratorReq req = QueryIteratorReq.builder()
                .collectionName("test")
                .cursor(cursor)
                .build();
        assertTrue(req.toString().contains("cursor=" + cursor));
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

    private static QueryResults varcharQueryResults(List<String> pks, long sessionTs) {
        QueryResults.Builder builder = QueryResults.newBuilder()
                .setStatus(successStatus())
                .setSessionTs(sessionTs);
        if (!pks.isEmpty()) {
            builder.addOutputFields("pk")
                    .addFieldsData(FieldData.newBuilder()
                            .setFieldName("pk")
                            .setType(DataType.VarChar)
                            .setScalars(ScalarField.newBuilder()
                                    .setStringData(io.milvus.grpc.StringArray.newBuilder()
                                            .addAllData(pks).build())
                                    .build())
                            .build());
        }
        return builder.build();
    }

    private static RpcStubWrapper testStubWrapper(
            MilvusServiceGrpc.MilvusServiceBlockingStub stub) {
        return new RpcStubWrapper(stub, 0L, "host:19530", "default");
    }

    private static QueryResults elementQueryResults(long id, Long... offsets) {
        return QueryResults.newBuilder()
                .setStatus(successStatus())
                .setSessionTs(100L)
                .addOutputFields("id")
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder().addData(id).build())
                                .build())
                        .build())
                .addElementIndices(ElementIndices.newBuilder()
                        .setIndices(LongArray.newBuilder().addAllData(Arrays.asList(offsets)).build())
                        .build())
                .build();
    }

    private static String queryParam(QueryRequest request, String key) {
        for (KeyValuePair param : request.getQueryParamsList()) {
            if (key.equals(param.getKey())) {
                return param.getValue();
            }
        }
        return null;
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

    private static CreateCollectionReq.FieldSchema varcharPrimaryField() {
        return CreateCollectionReq.FieldSchema.builder()
                .name("pk")
                .dataType(io.milvus.v2.common.DataType.VarChar)
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

    private static List<Long> offsets(List<QueryResultsWrapper.RowRecord> records) {
        List<Long> offsets = new ArrayList<>();
        for (QueryResultsWrapper.RowRecord record : records) {
            offsets.add((Long) record.get(Constant.OFFSET));
        }
        return offsets;
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
