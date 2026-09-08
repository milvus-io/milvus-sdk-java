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

package io.milvus.unit.v2.client;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.client.MilvusClientV2Session;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.GetReq;
import io.milvus.v2.service.vector.request.HybridSearchReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchIteratorReq;
import io.milvus.v2.service.vector.request.SearchIteratorReqV2;
import io.milvus.v2.service.vector.request.SearchReq;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

@Tag("unit")
class MilvusClientV2SessionTest {
    private MilvusClientV2 parent;
    private MilvusClientV2Session session;

    @BeforeEach
    void setUp() throws Exception {
        parent = mock(MilvusClientV2.class);
        session = newSession(parent, "c1");
    }

    private static MilvusClientV2Session newSession(MilvusClientV2 parent, String clusterId) throws Exception {
        Constructor<MilvusClientV2Session> constructor =
                MilvusClientV2Session.class.getDeclaredConstructor(MilvusClientV2.class, String.class);
        constructor.setAccessible(true);
        return constructor.newInstance(parent, clusterId);
    }

    @Test
    void getClusterIdReturnsBoundCluster() {
        assertEquals("c1", session.getClusterId());
    }

    @Test
    void openSessionRoutesOperationsToParent() {
        assertNull(session.search(SearchReq.builder().build()));
        assertNull(session.searchAsync(SearchReq.builder().build()));
        assertNull(session.hybridSearch(HybridSearchReq.builder().build()));
        assertNull(session.hybridSearchAsync(HybridSearchReq.builder().build()));
        assertNull(session.query(QueryReq.builder().build()));
        assertNull(session.queryAsync(QueryReq.builder().build()));
        assertNull(session.queryIterator(QueryIteratorReq.builder().build()));
        assertNull(session.searchIterator(SearchIteratorReq.builder().build()));
        assertNull(session.searchIteratorV2(SearchIteratorReqV2.builder().build()));
        assertNull(session.get(GetReq.builder().build()));
        assertNull(session.getAsync(GetReq.builder().build()));
    }

    @Test
    void closedSessionFailsAllOperations() {
        session.close();

        assertThrows(MilvusClientException.class, () -> session.search(SearchReq.builder().build()));
        assertThrows(MilvusClientException.class, () -> session.searchAsync(SearchReq.builder().build()));
        assertThrows(MilvusClientException.class, () -> session.hybridSearch(HybridSearchReq.builder().build()));
        assertThrows(MilvusClientException.class, () -> session.hybridSearchAsync(HybridSearchReq.builder().build()));
        assertThrows(MilvusClientException.class, () -> session.query(QueryReq.builder().build()));
        assertThrows(MilvusClientException.class, () -> session.queryAsync(QueryReq.builder().build()));
        assertThrows(MilvusClientException.class, () -> session.queryIterator(QueryIteratorReq.builder().build()));
        assertThrows(MilvusClientException.class, () -> session.searchIterator(SearchIteratorReq.builder().build()));
        assertThrows(MilvusClientException.class, () -> session.searchIteratorV2(SearchIteratorReqV2.builder().build()));
        assertThrows(MilvusClientException.class, () -> session.get(GetReq.builder().build()));
        assertThrows(MilvusClientException.class, () -> session.getAsync(GetReq.builder().build()));
    }

    @Test
    void clusterIdRemainsAvailableAfterClose() {
        session.close();

        assertEquals("c1", session.getClusterId());
    }

    @Test
    void closeIsIdempotent() {
        session.close();
        session.close();

        assertEquals("c1", session.getClusterId());
        assertThrows(MilvusClientException.class, () -> session.search(SearchReq.builder().build()));
    }

    @Test
    void eachSessionKeepsItsOwnClusterId() throws Exception {
        MilvusClientV2Session other = newSession(parent, "c2");

        assertEquals("c1", session.getClusterId());
        assertEquals("c2", other.getClusterId());

        other.close();
        assertDoesNotThrow(() -> other.close());
        assertThrows(MilvusClientException.class, () -> other.query(QueryReq.builder().build()));
        assertNull(session.search(SearchReq.builder().build()));
    }
}
