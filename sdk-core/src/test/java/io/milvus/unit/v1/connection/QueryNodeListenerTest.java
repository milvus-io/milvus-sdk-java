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

package io.milvus.unit.v1.connection;

import io.milvus.client.MilvusClient;
import io.milvus.connection.QueryNodeListener;
import io.milvus.connection.ServerSetting;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.IDs;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.SearchResultData;
import io.milvus.grpc.SearchResults;
import io.milvus.param.QueryNodeSingleSearch;
import io.milvus.param.R;
import io.milvus.param.ServerAddress;
import io.milvus.param.dml.SearchParam;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("unit")
class QueryNodeListenerTest {

    private QueryNodeSingleSearch createSingleSearch() {
        return QueryNodeSingleSearch.newBuilder()
                .withCollectionName("heartbeat_coll")
                .withVectorFieldName("float_vector")
                .withVectors(Collections.singletonList(Arrays.asList(1.0f, 2.0f)))
                .build();
    }

    private ServerSetting createServerSetting(MilvusClient client) {
        return ServerSetting.newBuilder()
                .withHost(ServerAddress.newBuilder().build())
                .withMilvusClient(client)
                .build();
    }

    private MilvusClient createMockClient() {
        MilvusClient client = mock(MilvusClient.class);
        when(client.withTimeout(4, TimeUnit.SECONDS)).thenReturn(client);
        return client;
    }

    private SearchResults createSuccessResults() {
        SearchResultData data = SearchResultData.newBuilder()
                .setTopK(1)
                .addTopks(1)
                .setNumQueries(1)
                .setIds(IDs.newBuilder().setIntId(LongArray.newBuilder().addData(1L).build()).build())
                .addAllScores(Collections.singletonList(1.0f))
                .build();
        return SearchResults.newBuilder().setResults(data).build();
    }

    @Test
    void testConstructorBuildsSearchParam() {
        QueryNodeListener listener = new QueryNodeListener(createSingleSearch());
        assertNotNull(listener);
    }

    @Test
    void testHeartbeatSucceedsOnSearchResults() {
        MilvusClient client = createMockClient();
        when(client.search(any(SearchParam.class))).thenReturn(R.success(createSuccessResults()));
        QueryNodeListener listener = new QueryNodeListener(createSingleSearch());
        assertTrue(listener.heartBeat(createServerSetting(client)));
    }

    @Test
    void testHeartbeatReturnsFalseOnRpcFailure() {
        MilvusClient client = createMockClient();
        when(client.search(any(SearchParam.class))).thenReturn(R.failed(ErrorCode.UnexpectedError, "heartbeat failed"));
        QueryNodeListener listener = new QueryNodeListener(createSingleSearch());
        assertFalse(listener.heartBeat(createServerSetting(client)));
    }

    @Test
    void testHeartbeatReturnsFalseOnException() {
        MilvusClient client = createMockClient();
        when(client.search(any(SearchParam.class))).thenThrow(new RuntimeException("connection lost"));
        QueryNodeListener listener = new QueryNodeListener(createSingleSearch());
        assertFalse(listener.heartBeat(createServerSetting(client)));
    }
}
