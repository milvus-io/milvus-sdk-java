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
import io.milvus.support.v2.BaseTest;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.FloatVec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@Tag("integration")
class VectorCloseTest extends BaseTest {

    @Test
    void testCallAfterCloseFailsFast() throws Exception {
        client_v2.close(3);

        GetReq getRequest = GetReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .build();
        QueryReq queryRequest = QueryReq.builder()
                .collectionName("book")
                .filter("id > 0")
                .build();
        SearchReq searchRequest = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();

        assertSyncCallFailsAfterClose(() -> client_v2.get(getRequest));
        assertSyncCallFailsAfterClose(() -> client_v2.query(queryRequest));
        assertSyncCallFailsAfterClose(() -> client_v2.search(searchRequest));

        assertAsyncCallFailsAfterClose(client_v2.getAsync(getRequest));
        assertAsyncCallFailsAfterClose(client_v2.queryAsync(queryRequest));
        assertAsyncCallFailsAfterClose(client_v2.searchAsync(searchRequest));
        assertAsyncCallFailsAfterClose(client_v2.hybridSearchAsync(HybridSearchReq.builder()
                .collectionName("test")
                .searchRequests(Collections.singletonList(AnnSearchReq.builder()
                        .vectorFieldName("vector")
                        .vectors(Collections.singletonList(
                                new FloatVec(Arrays.asList(1.0f, 2.0f))))
                        .limit(10)
                        .build()))
                .limit(10)
                .build()));

        verify(blockingStub, never()).query(any(QueryRequest.class));
        verify(blockingStub, never()).search(any(SearchRequest.class));
        verify(futureStub, never()).query(any(QueryRequest.class));
        verify(futureStub, never()).search(any(SearchRequest.class));
        verify(futureStub, never()).hybridSearch(any(HybridSearchRequest.class));
    }

    private static void assertSyncCallFailsAfterClose(Supplier<?> call) {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                call::get);
        Assertions.assertEquals(ErrorCode.CLIENT_ERROR, exception.getErrorCode());
        Assertions.assertEquals("MilvusClient is closed", exception.getMessage());
    }

    private static <T> void assertAsyncCallFailsAfterClose(CompletableFuture<T> future) {
        ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                () -> future.get(1, TimeUnit.SECONDS));
        Assertions.assertTrue(exception.getCause() instanceof MilvusClientException);
        Assertions.assertEquals(ErrorCode.CLIENT_ERROR,
                ((MilvusClientException) exception.getCause()).getErrorCode());
        Assertions.assertEquals("MilvusClient is closed", exception.getCause().getMessage());
    }
}
