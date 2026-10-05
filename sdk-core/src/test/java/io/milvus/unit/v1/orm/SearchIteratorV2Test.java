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
import io.milvus.grpc.StringArray;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class SearchIteratorV2Test {
    @Test
    void firstPagePinsGuaranteeTimestampForFollowingPage() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(
                searchResults(12345678L, 1L), searchResults(22345678L, 2L));

        SearchIteratorV2 iterator = new SearchIteratorV2(request(new HashMap<>()),
                testStubWrapper(stub));
        iterator.next();
        iterator.next();

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(stub, times(2)).search(captor.capture());
        assertEquals(0L, captor.getAllValues().get(0).getGuaranteeTimestamp());
        assertEquals(12345678L, captor.getAllValues().get(1).getGuaranteeTimestamp());
    }

    @Test
    void legacyFirstPageUsesClientTimestampWhenSessionTimestampIsZero() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(
                searchResults(0L, 1L), searchResults(0L, 2L));

        SearchIteratorV2 iterator = new SearchIteratorV2(request(new HashMap<>()),
                testStubWrapper(stub));
        iterator.next();
        iterator.next();

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(stub, times(2)).search(captor.capture());
        assertTrue(captor.getAllValues().get(1).getGuaranteeTimestamp() > 0L);
    }

    @Test
    void firstPagePreservesExplicitGuaranteeTimestamp() {
        Map<String, Object> searchParams = new HashMap<>();
        searchParams.put("guarantee_timestamp", 42);
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(
                searchResults(12345678L, 1L), searchResults(22345678L, 2L));

        SearchIteratorV2 iterator = new SearchIteratorV2(request(searchParams),
                testStubWrapper(stub));
        iterator.next();
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
                searchResults(12345678L, 1L), searchResults(22345678L, 2L));

        SearchIteratorV2 iterator = new SearchIteratorV2(request(new HashMap<>(), limit),
                testStubWrapper(stub));
        iterator.next();
        iterator.next();

        verify(stub, times(2)).search(any(SearchRequest.class));
    }

    @Test
    void truncatedFinalPageIsAnIndependentCopy() throws ReflectiveOperationException {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(
                searchResults(12345678L, 1L, 2L), searchResults(22345678L));

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
                searchResults(12345678L, 1L, 2L), searchResults(22345678L));
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
                searchResults(12345678L, 1L, 2L, 3L), searchResults(22345678L));
        SearchIteratorReqV2 req = request(new HashMap<>(), 3L);
        req.setExternalFilterFunc(hits -> hits);

        SearchIteratorV2 iterator = new SearchIteratorV2(req, testStubWrapper(stub));
        iterator.next();
        assertEquals(1, cacheSize(iterator));

        iterator.close();
        assertEquals(0, cacheSize(iterator));
    }

    @Test
    void realFirstBatchIsReturnedWithoutProbeAndRequestMapIsNotMutated() {
        Map<String, Object> options = new HashMap<>();
        options.put("search_iter_last_pk", "999");
        options.put("search_iter_last_pk_type", "int64");
        options.put("search_iter_cursor_version", "2");
        Map<String, Object> original = new HashMap<>(options);
        long high = 9007199254740993L;
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(
                pkResponse(100L, high, high + 1), pkResponse(200L, high + 2));
        SearchIteratorReqV2 req = request(options);
        SearchIteratorV2 iterator = new SearchIteratorV2(req, testStubWrapper(stub));
        assertEquals(2, iterator.next().size());
        verify(stub, times(1)).search(any(SearchRequest.class));
        iterator.next();
        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(stub, times(2)).search(captor.capture());
        Map<String, String> first = params(captor.getAllValues().get(0));
        Map<String, String> second = params(captor.getAllValues().get(1));
        assertEquals("2", first.get("topk"));
        assertEquals("2", first.get("search_iter_batch_size"));
        assertEquals("2", first.get("search_iter_cursor_version"));
        assertFalse(first.containsKey("search_iter_last_pk"));
        assertEquals(Long.toString(high + 1), second.get("search_iter_last_pk"));
        assertEquals(100L, captor.getAllValues().get(1).getGuaranteeTimestamp());
        assertEquals(original, options);
        // Reusing the same request creates a new cursor, not a continuation.
        new SearchIteratorV2(req, testStubWrapper(stub));
        verify(stub, times(3)).search(captor.capture());
        assertFalse(params(captor.getValue()).containsKey("search_iter_last_pk"));
    }

    @Test
    void malformedCursorDoesNotAdvanceAndRetryCanGetTheSamePage() {
        SearchResults bad = pkResponse(100L, 2L).toBuilder().setStatus(
                pkResponse(100L, 2L).getStatus().toBuilder().putExtraInfo("search_iter_last_pk", "3")).build();
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(pkResponse(100L, 1L), bad);
        when(stub.search(any(SearchRequest.class))).thenReturn(pkResponse(100L, 1L), bad, pkResponse(100L, 2L));
        SearchIteratorV2 iterator = new SearchIteratorV2(pkRequest(new HashMap<>()), testStubWrapper(stub));
        iterator.next();
        assertThrows(IllegalStateException.class, iterator::next);
        assertEquals(1, iterator.next().size());
        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(stub, times(3)).search(captor.capture());
        assertEquals("1", params(captor.getAllValues().get(1)).get("search_iter_last_pk"));
        assertEquals("1", params(captor.getAllValues().get(2)).get("search_iter_last_pk"));
    }

    @Test
    void pkModeCannotLoseCapabilityAndLegacyModeCannotSilentlyUpgrade() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(pkResponse(100L, 1L), searchResults(100L, 2L));
        SearchIteratorV2 iterator = new SearchIteratorV2(pkRequest(new HashMap<>()), testStubWrapper(stub));
        iterator.next();
        assertThrows(IllegalStateException.class, iterator::next);
        stub = mockStub(searchResults(100L, 1L), pkResponse(100L, 2L));
        SearchIteratorV2 legacy = new SearchIteratorV2(pkRequest(new HashMap<>()), testStubWrapper(stub));
        legacy.next();
        assertThrows(IllegalStateException.class, legacy::next);
        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(stub, times(2)).search(captor.capture());
        assertFalse(params(captor.getAllValues().get(1)).containsKey("search_iter_cursor_version"));
    }

    @Test
    void pkModeRequiresServerSnapshotUnlessUserProvidesOne() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(pkResponse(0L, 1L), pkResponse(0L, 2L));
        assertThrows(IllegalStateException.class, () -> new SearchIteratorV2(pkRequest(new HashMap<>()), testStubWrapper(stub)));
        Map<String, Object> options = new HashMap<>();
        options.put("guarantee_timestamp", 42L);
        SearchIteratorV2 iterator = new SearchIteratorV2(pkRequest(options), testStubWrapper(stub));
        assertEquals(1, iterator.next().size());
    }

    @Test
    void int64BoundsAndVarcharIncludingEmptyRemainExact() {
        for (long pk : new long[]{Long.MIN_VALUE, Long.MAX_VALUE, 9007199254740993L}) {
            MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(pkResponse(100L, pk), pkResponse(100L, pk));
            SearchIteratorV2 iterator = new SearchIteratorV2(pkRequest(new HashMap<>()), testStubWrapper(stub));
            assertEquals(pk, iterator.next().get(0).getId());
        }
        for (String pk : new String[]{"", "quote\"\\雪", "nul\u0000尾"}) {
            SearchResults response = varcharResponse(pk);
            MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(response, pkResponse(100L));
            when(stub.describeCollection(any(DescribeCollectionRequest.class))).thenReturn(
                    describeCollectionResponse().toBuilder().setSchema(CollectionSchema.newBuilder()
                            .addFields(FieldSchema.newBuilder().setName("id").setIsPrimaryKey(true).setDataType(DataType.VarChar))).build());
            SearchIteratorV2 iterator = new SearchIteratorV2(pkRequest(new HashMap<>()), testStubWrapper(stub));
            assertEquals(pk, iterator.next().get(0).getId());
            iterator.next();
            ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
            verify(stub, times(2)).search(captor.capture());
            assertEquals(pk, params(captor.getAllValues().get(1)).get("search_iter_last_pk"));
        }
    }

    @Test
    void callbackFailureReprocessesTheSameImmutableProtoAndEmptyEndIsLatched() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(pkResponse(100L, 1L, 2L), pkResponse(100L));
        SearchIteratorReqV2 req = pkRequest(new HashMap<>());
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        req.setExternalFilterFunc(hits -> {
            if (calls.getAndIncrement() == 0) {
                hits.clear();
                throw new IllegalArgumentException("callback");
            }
            return hits;
        });
        SearchIteratorV2 iterator = new SearchIteratorV2(req, testStubWrapper(stub));
        assertThrows(IllegalArgumentException.class, iterator::next);
        assertEquals(2, iterator.next().size());
        verify(stub, times(1)).search(any(SearchRequest.class));
        assertTrue(iterator.next().isEmpty());
        assertTrue(iterator.next().isEmpty());
        verify(stub, times(2)).search(any(SearchRequest.class));
    }

    @Test
    void failedStatusAndMissingTokenAreErrorsNotEndOfResults() {
        SearchResults failure = pkResponse(100L, 2L).toBuilder().setStatus(Status.newBuilder().setCode(1).setReason("failure")).build();
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(pkResponse(100L, 1L), failure);
        SearchIteratorV2 iterator = new SearchIteratorV2(pkRequest(new HashMap<>()), testStubWrapper(stub));
        iterator.next();
        assertThrows(RuntimeException.class, iterator::next);
        SearchResults noToken = searchResults(100L, 1L).toBuilder().setResults(
                searchResults(100L, 1L).getResults().toBuilder().clearSearchIteratorV2Results()).build();
        MilvusServiceGrpc.MilvusServiceBlockingStub unsupported = mockStub(noToken, noToken);
        assertThrows(RuntimeException.class, () -> new SearchIteratorV2(pkRequest(new HashMap<>()), testStubWrapper(unsupported)));
    }

    @Test
    void malformedTypedCursorFieldsAndDecodeErrorsAreRejected() {
        for (String value : new String[]{"9223372036854775808", "1.0", "+1", "01", "2"}) {
            SearchResults bad = pkResponse(100L, 1L).toBuilder().setStatus(
                    pkResponse(100L, 1L).getStatus().toBuilder().putExtraInfo("search_iter_last_pk", value)).build();
            MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(bad, bad);
            assertThrows(RuntimeException.class, () -> new SearchIteratorV2(pkRequest(new HashMap<>()), testStubWrapper(stub)));
        }
        SearchResults malformed = pkResponse(100L, 2L).toBuilder().setResults(
                pkResponse(100L, 2L).getResults().toBuilder().setNumQueries(2)).build();
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(pkResponse(100L, 1L), malformed);
        when(stub.search(any(SearchRequest.class))).thenReturn(pkResponse(100L, 1L), malformed, pkResponse(100L, 2L));
        SearchIteratorV2 iterator = new SearchIteratorV2(pkRequest(new HashMap<>()), testStubWrapper(stub));
        iterator.next();
        assertThrows(RuntimeException.class, iterator::next);
        iterator.next();
        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(stub, times(3)).search(captor.capture());
        assertEquals("1", params(captor.getAllValues().get(1)).get("search_iter_last_pk"));
        assertEquals("1", params(captor.getAllValues().get(2)).get("search_iter_last_pk"));
    }

    @Test
    void manualLegacyContinuationKeepsBoundTokenAndExplicitSnapshotWithoutPkOptIn() {
        Map<String, Object> options = new HashMap<>();
        options.put("search_iter_last_bound", 0.5f);
        options.put("search_iter_id", "token");
        options.put("guarantee_timestamp", 42L);
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(searchResults(100L, 2L), searchResults(100L, 3L));
        SearchIteratorV2 iterator = new SearchIteratorV2(request(options), testStubWrapper(stub));
        assertEquals(2L, iterator.next().get(0).getId());
        assertEquals(3L, iterator.next().get(0).getId());
        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(stub, times(2)).search(captor.capture());
        assertFalse(params(captor.getAllValues().get(0)).containsKey("search_iter_cursor_version"));
        assertEquals("0.5", params(captor.getAllValues().get(0)).get("search_iter_last_bound"));
        assertEquals("token", params(captor.getAllValues().get(0)).get("search_iter_id"));
        assertEquals(42L, captor.getAllValues().get(0).getGuaranteeTimestamp());
        assertEquals(42L, captor.getAllValues().get(1).getGuaranteeTimestamp());
        assertEquals(0.5f, options.get("search_iter_last_bound"));
        MilvusServiceGrpc.MilvusServiceBlockingStub wrong = mockStub(pkResponse(100L, 2L), pkResponse(100L, 3L));
        assertThrows(IllegalStateException.class, () -> new SearchIteratorV2(request(options), testStubWrapper(wrong)));
    }

    @Test
    void defaultDistanceModeDoesNotRequestPkScanAndUnsupportedPreferenceIsRejected() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(searchResults(100L, 1L), searchResults(100L, 2L));
        SearchIteratorV2 iterator = new SearchIteratorV2(request(new HashMap<>()), testStubWrapper(stub));
        iterator.next(); iterator.next();
        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(stub, times(2)).search(captor.capture());
        assertFalse(params(captor.getAllValues().get(0)).containsKey("search_iter_cursor_version"));
        assertFalse(params(captor.getAllValues().get(1)).containsKey("search_iter_cursor_version"));
        MilvusServiceGrpc.MilvusServiceBlockingStub unsolicited = mockStub(pkResponse(100L, 1L), pkResponse(100L, 2L));
        assertThrows(IllegalStateException.class, () -> new SearchIteratorV2(request(new HashMap<>()), testStubWrapper(unsolicited)));
        Map<String, Object> options = new HashMap<>(); options.put("search_iter_cursor_version", "3");
        assertThrows(IllegalArgumentException.class, () -> new SearchIteratorV2(request(options), testStubWrapper(stub)));
    }

    private static SearchIteratorReqV2 pkRequest(Map<String, Object> options) {
        options.put("search_iter_cursor_version", "2");
        return request(options);
    }

    @Test
    void rawShapeAndBoundMismatchDoNotAdvanceCursorAndRoundingUsesRawScore() {
        for (int kind = 0; kind < 3; kind++) {
            SearchResults correct = pkResponse(100L, 2L);
            SearchResultData.Builder data = correct.getResults().toBuilder();
            if (kind == 0) data.clearTopks().addTopks(2);
            if (kind == 1) data.addScores(1.0f);
            if (kind == 2) data.setSearchIteratorV2Results(data.getSearchIteratorV2Results().toBuilder().setLastBound(1.0f));
            SearchResults bad = correct.toBuilder().setResults(data).build();
            MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(pkResponse(100L, 1L), bad);
            when(stub.search(any(SearchRequest.class))).thenReturn(pkResponse(100L, 1L), bad, correct);
            SearchIteratorV2 iterator = new SearchIteratorV2(pkRequest(new HashMap<>()), testStubWrapper(stub));
            iterator.next();
            assertThrows(RuntimeException.class, iterator::next);
            iterator.next();
            ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
            verify(stub, times(3)).search(captor.capture());
            assertEquals("1", params(captor.getAllValues().get(1)).get("search_iter_last_pk"));
            assertEquals("1", params(captor.getAllValues().get(2)).get("search_iter_last_pk"));
        }
        SearchResults original = pkResponse(100L, 1L);
        SearchResults rounded = original.toBuilder().setResults(original.getResults().toBuilder()
                .clearScores().addScores(0.123456f)
                .setSearchIteratorV2Results(original.getResults().getSearchIteratorV2Results().toBuilder().setLastBound(0.123456f))).build();
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(rounded, pkResponse(100L));
        SearchIteratorReqV2 req = pkRequest(new HashMap<>());
        req.setRoundDecimal(2);
        SearchIteratorV2 iterator = new SearchIteratorV2(req, testStubWrapper(stub));
        iterator.next(); iterator.next();
        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(stub, times(2)).search(captor.capture());
        assertEquals("0.123456", params(captor.getAllValues().get(1)).get("search_iter_last_bound"));
    }

    private static Map<String, String> params(SearchRequest request) {
        Map<String, String> result = new HashMap<>();
        request.getSearchParamsList().forEach(kv -> result.put(kv.getKey(), kv.getValue()));
        return result;
    }

    @Test
    void duplicateOnlyPageAdvancesRawCursorWithoutConsumingDistinctLimit() {
        long high = 9007199254740993L;
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(
                withScore(pkResponse(100L, high), 0f), withScore(pkResponse(100L, high), 1f));
        when(stub.search(any(SearchRequest.class))).thenReturn(
                withScore(pkResponse(100L, high), 0f), withScore(pkResponse(100L, high), 1f),
                withScore(pkResponse(100L, high + 1), 2f));
        SearchIteratorReqV2 req = pkRequest(new HashMap<>());
        req.setBatchSize(1);
        req.setLimit(2);
        SearchIteratorV2 iterator = new SearchIteratorV2(req, testStubWrapper(stub));
        assertEquals(high, iterator.next().get(0).getId());
        assertEquals(high + 1, iterator.next().get(0).getId());
        assertTrue(iterator.next().isEmpty());
        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(stub, times(3)).search(captor.capture());
        assertEquals(Long.toString(high), params(captor.getAllValues().get(2)).get("search_iter_last_pk"));
        assertEquals("1.0", params(captor.getAllValues().get(2)).get("search_iter_last_bound"));
    }

    @Test
    void samePageDuplicatesConsumeOneAcceptedPrimaryKey() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(pkResponse(100L, 1L, 1L), pkResponse(100L, 2L));
        SearchIteratorReqV2 req = pkRequest(new HashMap<>());
        req.setLimit(2);
        SearchIteratorV2 iterator = new SearchIteratorV2(req, testStubWrapper(stub));
        assertEquals(1, iterator.next().size());
        assertEquals(2L, iterator.next().get(0).getId());
        assertTrue(iterator.next().isEmpty());
        verify(stub, times(2)).search(any(SearchRequest.class));
    }

    @Test
    void emptyVarcharDuplicateIsSkippedAcrossScoresUntilAnotherPrimaryKey() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(varcharResponse(""), varcharResponse(""));
        when(stub.search(any(SearchRequest.class))).thenReturn(
                withScore(varcharResponse(""), 0f), withScore(varcharResponse(""), 1f),
                withScore(varcharResponse("雪"), 2f));
        when(stub.describeCollection(any(DescribeCollectionRequest.class))).thenReturn(
                describeCollectionResponse().toBuilder().setSchema(CollectionSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder().setName("id").setIsPrimaryKey(true)
                                .setDataType(DataType.VarChar))).build());
        SearchIteratorReqV2 req = pkRequest(new HashMap<>());
        req.setBatchSize(1);
        req.setLimit(2);
        SearchIteratorV2 iterator = new SearchIteratorV2(req, testStubWrapper(stub));
        assertEquals("", iterator.next().get(0).getId());
        assertEquals("雪", iterator.next().get(0).getId());
        assertTrue(iterator.next().isEmpty());
        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(stub, times(3)).search(captor.capture());
        assertEquals("", params(captor.getAllValues().get(2)).get("search_iter_last_pk"));
    }

    @Test
    void failingFilterDoesNotCommitDuplicateKeysAndRejectedVersionCanLaterBeAccepted() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(pkResponse(100L, 1L), pkResponse(100L, 1L));
        when(stub.search(any(SearchRequest.class))).thenReturn(
                withScore(pkResponse(100L, 1L), 0f), withScore(pkResponse(100L, 1L), 1f),
                withScore(pkResponse(100L, 1L), 2f), withScore(pkResponse(100L, 2L), 3f));
        SearchIteratorReqV2 req = pkRequest(new HashMap<>());
        req.setBatchSize(1);
        req.setLimit(2);
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        req.setExternalFilterFunc(hits -> {
            int call = calls.getAndIncrement();
            if (call == 0) return Collections.emptyList();
            if (call == 1) {
                hits.clear();
                throw new IllegalArgumentException("filter");
            }
            return hits;
        });
        SearchIteratorV2 iterator = new SearchIteratorV2(req, testStubWrapper(stub));
        assertThrows(IllegalArgumentException.class, iterator::next);
        assertEquals(1L, iterator.next().get(0).getId());
        assertEquals(2L, iterator.next().get(0).getId());
        assertTrue(iterator.next().isEmpty());
        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(stub, times(4)).search(captor.capture());
        assertEquals("0.0", params(captor.getAllValues().get(1)).get("search_iter_last_bound"));
        assertEquals("1.0", params(captor.getAllValues().get(2)).get("search_iter_last_bound"));
        assertEquals("2.0", params(captor.getAllValues().get(3)).get("search_iter_last_bound"));
    }

    @Test
    void distanceModeRetainsExistingDuplicateResultBehavior() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mockStub(searchResults(100L, 1L), searchResults(100L, 1L));
        SearchIteratorReqV2 req = request(new HashMap<>(), 2L);
        req.setBatchSize(1);
        SearchIteratorV2 iterator = new SearchIteratorV2(req, testStubWrapper(stub));
        assertEquals(1L, iterator.next().get(0).getId());
        assertEquals(1L, iterator.next().get(0).getId());
        assertTrue(iterator.next().isEmpty());
        verify(stub, times(2)).search(any(SearchRequest.class));
    }

    private static SearchResults withScore(SearchResults result, float score) {
        return result.toBuilder().setResults(result.getResults().toBuilder().clearScores().addScores(score)
                .setSearchIteratorV2Results(result.getResults().getSearchIteratorV2Results().toBuilder()
                        .setLastBound(score))).build();
    }

    private static SearchResults pkResponse(long timestamp, Long... ids) {
        Status.Builder status = successStatus().toBuilder().putExtraInfo("search_iter_cursor_version", "2");
        if (ids.length > 0) status.putExtraInfo("search_iter_last_pk_type", "int64")
                .putExtraInfo("search_iter_last_pk", Long.toString(ids[ids.length - 1]));
        SearchResults result = searchResults(timestamp, ids);
        return result.toBuilder().setStatus(status).setResults(result.getResults().toBuilder()
                .setSearchIteratorV2Results(result.getResults().getSearchIteratorV2Results().toBuilder()
                        .setLastBound(ids.length == 0 ? 0.0f : result.getResults().getScores(ids.length - 1)))).build();
    }

    private static SearchResults varcharResponse(String pk) {
        return searchResults(100L, 1L).toBuilder()
                .setStatus(successStatus().toBuilder().putExtraInfo("search_iter_cursor_version", "2")
                        .putExtraInfo("search_iter_last_pk_type", "varchar").putExtraInfo("search_iter_last_pk", pk))
                .setResults(searchResults(100L, 1L).getResults().toBuilder()
                        .setIds(IDs.newBuilder().setStrId(StringArray.newBuilder().addData(pk))))
                .build();
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
