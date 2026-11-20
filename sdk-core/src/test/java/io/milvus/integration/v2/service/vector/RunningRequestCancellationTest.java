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
 */


package io.milvus.integration.v2.service.vector;

import io.milvus.grpc.QueryResults;
import io.milvus.grpc.SearchResults;
import io.milvus.grpc.Status;
import io.milvus.support.v2.BaseTest;
import io.milvus.v2.client.RetryConfig;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.HybridSearchReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("integration")
class RunningRequestCancellationTest extends BaseTest {
    private static final int REQUEST_CANCELED = 3002;
    private static final String REASON = "operator canceled\nreason: running request regression";
    private static final Status CANCELED = Status.newBuilder().setCode(REQUEST_CANCELED).setReason(REASON).build();

    @BeforeEach
    void enableRetries() {
        client_v2.retryConfig(RetryConfig.builder().maxRetryTimes(3).build());
    }

    @Test
    void searchCancellationIsTerminal() {
        when(blockingStub.search(any())).thenReturn(SearchResults.newBuilder().setStatus(CANCELED).build());

        assertCanceled(() -> client_v2.search(SearchReq.builder().collectionName("test")
                .annsField("vector").data(vectors()).limit(10).build()));
        verify(blockingStub).search(any());
    }

    @Test
    void queryCancellationIsTerminal() {
        when(blockingStub.query(any())).thenReturn(QueryResults.newBuilder().setStatus(CANCELED).build());

        assertCanceled(() -> client_v2.query(QueryReq.builder().collectionName("test")
                .filter("id >= 0").limit(10).build()));
        verify(blockingStub).query(any());
    }

    @Test
    void hybridSearchCancellationIsTerminal() {
        when(blockingStub.hybridSearch(any())).thenReturn(SearchResults.newBuilder().setStatus(CANCELED).build());
        AnnSearchReq branch = AnnSearchReq.builder().vectorFieldName("vector").vectors(vectors()).limit(10).build();

        assertCanceled(() -> client_v2.hybridSearch(HybridSearchReq.builder().collectionName("test")
                .searchRequests(Collections.singletonList(branch)).limit(10).build()));
        verify(blockingStub).hybridSearch(any());
    }

    private static List<BaseVector> vectors() {
        return Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f)));
    }

    private static void assertCanceled(Executable invoke) {
        MilvusClientException error = assertThrows(MilvusClientException.class, invoke);
        assertEquals(REQUEST_CANCELED, error.getServerErrCode());
        assertEquals(REASON, error.getMessage());
    }
}
