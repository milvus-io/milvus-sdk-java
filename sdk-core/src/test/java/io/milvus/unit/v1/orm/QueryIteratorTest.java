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

import com.google.protobuf.ByteString;
import io.milvus.exception.ParamException;
import io.milvus.grpc.DataType;
import io.milvus.grpc.ElementIndices;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.JSONArray;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.QueryRequest;
import io.milvus.grpc.QueryResults;
import io.milvus.grpc.ScalarField;
import io.milvus.grpc.Status;
import io.milvus.orm.iterator.IteratorCache;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.QueryIteratorCursor;
import io.milvus.orm.iterator.RpcStubWrapper;
import io.milvus.param.Constant;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import org.junit.jupiter.api.Tag;
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

@Tag("unit")
class QueryIteratorTest {
    private static final long TEST_COLLECTION_ID = 1001L;

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
    void queryIteratorElementFilterCollisionKeepsRealOffsetAndUsesUnderscore() {
        String filter = "element_filter(structA, $[int_val] >= 20000)";
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(
                queryResults(Collections.emptyList(), 100L),
                elementQueryResultsWithRealOffsetField(7L, 99L, 0L, 1L),
                queryResults(Collections.emptyList(), 100L));

        QueryIterator iterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Arrays.asList("id", "offset"))
                        .expr(filter)
                        .batchSize(10)
                        .build(),
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);
        List<QueryResultsWrapper.RowRecord> firstPage = iterator.next();
        assertEquals(Arrays.asList(7L, 7L), ids(firstPage));
        // the real "offset" field value is preserved, not clobbered by the element position
        assertEquals(Arrays.asList(99L, 99L), realOffsets(firstPage));
        assertEquals(Arrays.asList(0L, 1L), elementOffsets(firstPage));
        iterator.next();

        // the element cursor is tracked from the "_offset" column
        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(stub, times(3)).query(captor.capture());
        QueryRequest request = captor.getAllValues().get(2);
        assertEquals("id >= 7 and (" + filter + ")", request.getExpr());
        assertEquals("7", queryParam(request, Constant.QUERY_ITER_LAST_PK));
        assertEquals("1", queryParam(request, Constant.QUERY_ITER_LAST_ELEMENT_OFFSET));
    }

    @Test
    void queryIteratorElementFilterSparseDynamicOffsetUsesUnderscore() {
        String filter = "element_filter(structA, $[int_val] >= 20000)";
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(
                queryResults(Collections.emptyList(), 100L),
                elementQueryResultsWithSparseDynamicOffset(7L, 8L, 99L),
                queryResults(Collections.emptyList(), 100L));

        QueryIterator iterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Arrays.asList("id", "offset"))
                        .expr(filter)
                        .batchSize(10)
                        .build(),
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);
        List<QueryResultsWrapper.RowRecord> firstPage = iterator.next();
        assertEquals(Arrays.asList(7L, 7L, 8L), ids(firstPage));
        // a dynamic "offset" present only on a later row still triggers the rename
        assertEquals(Arrays.asList(0L, 1L, 0L), elementOffsets(firstPage));
        assertEquals(99L, firstPage.get(2).get(Constant.OFFSET));
        assertNull(firstPage.get(0).get(Constant.OFFSET));
        iterator.next();

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(stub, times(3)).query(captor.capture());
        QueryRequest request = captor.getAllValues().get(2);
        assertEquals("id >= 8 and (" + filter + ")", request.getExpr());
        assertEquals("8", queryParam(request, Constant.QUERY_ITER_LAST_PK));
        assertEquals("0", queryParam(request, Constant.QUERY_ITER_LAST_ELEMENT_OFFSET));
    }

    @Test
    void queryIteratorElementFilterBothOffsetNamesUseCollisionFreeAlias() {
        String filter = "element_filter(structA, $[int_val] >= 20000)";
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(
                queryResults(Collections.emptyList(), 100L),
                elementQueryResultsWithBothOffsetNames(7L, 99L, 88L, 0L, 1L),
                queryResults(Collections.emptyList(), 100L));

        QueryIterator iterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Arrays.asList("id", "offset", "_offset"))
                        .expr(filter)
                        .batchSize(10)
                        .build(),
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);
        List<QueryResultsWrapper.RowRecord> firstPage = iterator.next();
        assertEquals(Arrays.asList(7L, 7L), ids(firstPage));
        // both real columns are preserved, the element position falls back to "__offset"
        assertEquals(Arrays.asList(99L, 99L), realOffsets(firstPage));
        assertEquals(Arrays.asList(88L, 88L), underscoreOffsets(firstPage));
        assertEquals(Arrays.asList(0L, 1L), doubleUnderscoreOffsets(firstPage));
        iterator.next();

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(stub, times(3)).query(captor.capture());
        QueryRequest request = captor.getAllValues().get(2);
        assertEquals("7", queryParam(request, Constant.QUERY_ITER_LAST_PK));
        assertEquals("1", queryParam(request, Constant.QUERY_ITER_LAST_ELEMENT_OFFSET));
    }

    @Test
    void queryIteratorElementFilterSeekRunsWithIteratorTrueAndElementParams() {
        String filter = "element_filter(structA, $[int_val] >= 20000)";
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        int[] callCount = {0};
        when(stub.query(any(QueryRequest.class))).thenAnswer(invocation -> {
            QueryRequest request = invocation.getArgument(0);
            callCount[0]++;
            if (callCount[0] == 1) {
                // session-setup probe
                return queryResults(Collections.emptyList(), 100L);
            }
            String lastOffset = queryParam(request, Constant.QUERY_ITER_LAST_ELEMENT_OFFSET);
            if (lastOffset == null) {
                // first seek chunk: no element cursor yet, serve elements from position 0
                return elementQueryResults(7L, 0L, 1L);
            }
            // resumed seek/next chunks: continue after the recorded element offset, exactly like
            // a real server (reenter_offset = last_element_offset + 1)
            long start = Long.parseLong(lastOffset) + 1;
            return elementQueryResults(7L, start, start + 1);
        });

        QueryIterator iterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("id"))
                        .expr(filter)
                        .batchSize(10)
                        .offset(4)
                        .build(),
                testStubWrapper(stub),
                primaryField(),
                TEST_COLLECTION_ID);
        // the seek consumed exactly the configured 4 elements (0,1 then 2,3); the next page
        // continues at element 4, so an inclusive re-read would instead re-deliver 2,3
        List<QueryResultsWrapper.RowRecord> delivered = iterator.next();
        assertEquals(Arrays.asList(4L, 5L), offsets(delivered));

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(stub, times(4)).query(captor.capture());
        List<QueryRequest> requests = captor.getAllValues();

        // setup-ts probe: iterator=true, no element resume params yet
        assertEquals("true", queryParam(requests.get(0), Constant.ITERATOR_FIELD));
        assertNull(queryParam(requests.get(0), Constant.QUERY_ITER_LAST_PK));
        // first seek chunk: element-filter seek runs with iterator=true so the server returns
        // element_indices (matched-element offset semantics), still no element cursor
        assertEquals("true", queryParam(requests.get(1), Constant.ITERATOR_FIELD));
        assertEquals("false", queryParam(requests.get(1), Constant.REDUCE_STOP_FOR_BEST));
        assertNull(queryParam(requests.get(1), Constant.QUERY_ITER_LAST_PK));
        assertEquals(filter, requests.get(1).getExpr());
        // second seek chunk: resumes with an inclusive pk >= predicate plus the element params,
        // continuing after the recorded element offset (1 -> serves elements 2,3)
        assertEquals("true", queryParam(requests.get(2), Constant.ITERATOR_FIELD));
        assertEquals("false", queryParam(requests.get(2), Constant.REDUCE_STOP_FOR_BEST));
        assertEquals("id >= 7 and (" + filter + ")", requests.get(2).getExpr());
        assertEquals("7", queryParam(requests.get(2), Constant.QUERY_ITER_LAST_PK));
        assertEquals("1", queryParam(requests.get(2), Constant.QUERY_ITER_LAST_ELEMENT_OFFSET));
        // next batch: iterator=true with the element params advanced to 3
        assertEquals("true", queryParam(requests.get(3), Constant.ITERATOR_FIELD));
        assertEquals("true", queryParam(requests.get(3), Constant.REDUCE_STOP_FOR_BEST));
        assertEquals("7", queryParam(requests.get(3), Constant.QUERY_ITER_LAST_PK));
        assertEquals("3", queryParam(requests.get(3), Constant.QUERY_ITER_LAST_ELEMENT_OFFSET));
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
    void queryIteratorGetCursorVarcharPkAndElementOffset() {
        MilvusServiceGrpc.MilvusServiceBlockingStub varcharStub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(varcharStub.query(any(QueryRequest.class))).thenReturn(
                varcharQueryResults(Collections.emptyList(), 100L),
                varcharQueryResults(Arrays.asList("a", "b", "c"), 100L));

        QueryIterator varcharIterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("pk"))
                        .batchSize(10)
                        .build(),
                testStubWrapper(varcharStub),
                varcharPrimaryField(),
                TEST_COLLECTION_ID);
        varcharIterator.next();

        QueryIteratorCursor varcharCursor = varcharIterator.getCursor();
        assertEquals("c", varcharCursor.getStrPk());
        assertNull(varcharCursor.getIntPk());

        String filter = "element_filter(structA, $[int_val] >= 20000)";
        MilvusServiceGrpc.MilvusServiceBlockingStub elementStub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(elementStub.query(any(QueryRequest.class))).thenReturn(
                queryResults(Collections.emptyList(), 100L),
                elementQueryResults(7L, 0L, 1L));

        QueryIterator elementIterator = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("id"))
                        .expr(filter)
                        .batchSize(10)
                        .build(),
                testStubWrapper(elementStub),
                primaryField(),
                TEST_COLLECTION_ID);
        elementIterator.next();

        QueryIteratorCursor elementCursor = elementIterator.getCursor();
        assertEquals(Long.valueOf(7L), elementCursor.getIntPk());
        assertEquals(Long.valueOf(1L), elementCursor.getLastElementOffset());
    }

    @Test
    void queryIteratorResumesVarcharPkFromCursor() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(
                varcharQueryResults(Collections.emptyList(), 100L),
                varcharQueryResults(Arrays.asList("a", "b"), 100L),
                varcharQueryResults(Collections.singletonList("c"), 100L));

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
        assertEquals("b", cursor.getStrPk());

        QueryIterator resumed = new QueryIterator(
                QueryIteratorReq.builder()
                        .collectionName("test")
                        .outputFields(Collections.singletonList("pk"))
                        .batchSize(10)
                        .cursor(cursor)
                        .build(),
                testStubWrapper(stub),
                varcharPrimaryField(),
                TEST_COLLECTION_ID);
        assertEquals(Collections.singletonList("c"), strPks(resumed.next()));

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(stub, times(3)).query(captor.capture());
        QueryRequest resumedRequest = captor.getAllValues().get(2);
        assertEquals("pk > \"b\"", resumedRequest.getExpr());
        assertEquals(100L, resumedRequest.getGuaranteeTimestamp());
    }

    @Test
    void queryIteratorCloseReleasesCache() throws ReflectiveOperationException {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.query(any(QueryRequest.class))).thenReturn(
                queryResults(Collections.emptyList(), 100L),
                queryResults(Collections.singletonList(1L), 100L));

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
        iterator.close();
        assertEquals(0, cacheSize(iterator, "cacheIdInUse"));
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

    private static QueryResults elementQueryResultsWithRealOffsetField(long id, long realOffset, Long... offsets) {
        return QueryResults.newBuilder()
                .setStatus(successStatus())
                .setSessionTs(100L)
                .addOutputFields("id")
                .addOutputFields("offset")
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder().addData(id).build())
                                .build())
                        .build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("offset")
                        .setType(DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder().addData(realOffset).build())
                                .build())
                        .build())
                .addElementIndices(ElementIndices.newBuilder()
                        .setIndices(LongArray.newBuilder().addAllData(Arrays.asList(offsets)).build())
                        .build())
                .build();
    }

    private static QueryResults elementQueryResultsWithSparseDynamicOffset(long id1, long id2, long realOffset) {
        // the dynamic "offset" exists only in the second row's $meta, so the first row's record
        // carries no "offset" key and a first-row-only collision check would miss it
        JSONArray.Builder metaBuilder = JSONArray.newBuilder();
        metaBuilder.addData(ByteString.copyFromUtf8("{\"a\": 1}"));
        metaBuilder.addData(ByteString.copyFromUtf8("{\"a\": 1, \"offset\": " + realOffset + "}"));
        return QueryResults.newBuilder()
                .setStatus(successStatus())
                .setSessionTs(100L)
                .addOutputFields("id")
                .addOutputFields("offset")
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder().addData(id1).addData(id2).build())
                                .build())
                        .build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("$meta")
                        .setType(DataType.JSON)
                        .setIsDynamic(true)
                        .setScalars(ScalarField.newBuilder().setJsonData(metaBuilder.build()).build())
                        .build())
                .addElementIndices(ElementIndices.newBuilder()
                        .setIndices(LongArray.newBuilder().addData(0L).addData(1L).build())
                        .build())
                .addElementIndices(ElementIndices.newBuilder()
                        .setIndices(LongArray.newBuilder().addData(0L).build())
                        .build())
                .build();
    }

    private static QueryResults elementQueryResultsWithBothOffsetNames(long id, long realOffset,
                                                                      long realUnderscoreOffset, Long... offsets) {
        return QueryResults.newBuilder()
                .setStatus(successStatus())
                .setSessionTs(100L)
                .addOutputFields("id")
                .addOutputFields("offset")
                .addOutputFields("_offset")
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder().addData(id).build())
                                .build())
                        .build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("offset")
                        .setType(DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder().addData(realOffset).build())
                                .build())
                        .build())
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("_offset")
                        .setType(DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder().addData(realUnderscoreOffset).build())
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

    private static List<String> strPks(List<QueryResultsWrapper.RowRecord> records) {
        List<String> pks = new ArrayList<>();
        for (QueryResultsWrapper.RowRecord record : records) {
            pks.add((String) record.get("pk"));
        }
        return pks;
    }

    private static List<Long> offsets(List<QueryResultsWrapper.RowRecord> records) {
        List<Long> offsets = new ArrayList<>();
        for (QueryResultsWrapper.RowRecord record : records) {
            offsets.add((Long) record.get(Constant.OFFSET));
        }
        return offsets;
    }

    private static List<Long> realOffsets(List<QueryResultsWrapper.RowRecord> records) {
        List<Long> offsets = new ArrayList<>();
        for (QueryResultsWrapper.RowRecord record : records) {
            offsets.add((Long) record.get(Constant.OFFSET));
        }
        return offsets;
    }

    private static List<Long> elementOffsets(List<QueryResultsWrapper.RowRecord> records) {
        List<Long> offsets = new ArrayList<>();
        for (QueryResultsWrapper.RowRecord record : records) {
            offsets.add((Long) record.get("_offset"));
        }
        return offsets;
    }

    private static List<Long> underscoreOffsets(List<QueryResultsWrapper.RowRecord> records) {
        List<Long> offsets = new ArrayList<>();
        for (QueryResultsWrapper.RowRecord record : records) {
            offsets.add((Long) record.get("_offset"));
        }
        return offsets;
    }

    private static List<Long> doubleUnderscoreOffsets(List<QueryResultsWrapper.RowRecord> records) {
        List<Long> offsets = new ArrayList<>();
        for (QueryResultsWrapper.RowRecord record : records) {
            offsets.add((Long) record.get("__offset"));
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
