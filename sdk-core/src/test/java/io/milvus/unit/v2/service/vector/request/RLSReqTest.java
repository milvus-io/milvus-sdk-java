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

package io.milvus.unit.v2.service.vector.request;

import io.milvus.orm.iterator.IteratorAdapterV2;
import io.milvus.param.MetricType;
import io.milvus.param.dml.SearchIteratorParam;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.GetReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchIteratorReq;
import io.milvus.v2.service.vector.request.SearchIteratorReqV2;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class RLSReqTest {

    @Test
    void insertReqExposesRlsFields() {
        InsertReq req = InsertReq.builder()
                .collectionName("col")
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();
        assertEquals("alice", req.getRlsPrincipal());
        assertEquals(false, req.getSkipRls());

        req.setRlsPrincipal("bob");
        req.setSkipRls(true);
        assertEquals("bob", req.getRlsPrincipal());
        assertEquals(true, req.getSkipRls());

        InsertReq unset = InsertReq.builder().collectionName("col").build();
        assertNull(unset.getRlsPrincipal());
        assertNull(unset.getSkipRls());
    }

    @Test
    void upsertReqExposesRlsFields() {
        UpsertReq req = UpsertReq.builder()
                .collectionName("col")
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();
        assertEquals("alice", req.getRlsPrincipal());
        assertEquals(false, req.getSkipRls());
    }

    @Test
    void deleteReqExposesRlsFields() {
        DeleteReq req = DeleteReq.builder()
                .collectionName("col")
                .filter("pk > 0")
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();
        assertEquals("alice", req.getRlsPrincipal());
        assertEquals(false, req.getSkipRls());
    }

    @Test
    void searchReqExposesRlsFields() {
        SearchReq req = SearchReq.builder()
                .collectionName("col")
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();
        assertEquals("alice", req.getRlsPrincipal());
        assertEquals(false, req.getSkipRls());
    }

    @Test
    void queryReqExposesRlsFields() {
        QueryReq req = QueryReq.builder()
                .collectionName("col")
                .filter("pk > 0")
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();
        assertEquals("alice", req.getRlsPrincipal());
        assertEquals(false, req.getSkipRls());
    }

    @Test
    void queryIteratorReqExposesRlsFields() {
        QueryIteratorReq req = QueryIteratorReq.builder()
                .collectionName("col")
                .rlsPrincipal("alice")
                .skipRls(true)
                .build();
        assertEquals("alice", req.getRlsPrincipal());
        assertEquals(true, req.getSkipRls());
    }

    @Test
    void searchIteratorReqExposesRlsFields() {
        SearchIteratorReq req = SearchIteratorReq.builder()
                .collectionName("col")
                .rlsPrincipal("alice")
                .skipRls(true)
                .build();
        assertEquals("alice", req.getRlsPrincipal());
        assertEquals(true, req.getSkipRls());

        SearchIteratorReqV2 reqV2 = SearchIteratorReqV2.builder()
                .collectionName("col")
                .rlsPrincipal("alice")
                .skipRls(true)
                .build();
        assertEquals("alice", reqV2.getRlsPrincipal());
        assertEquals(true, reqV2.getSkipRls());
    }

    @Test
    void getReqExposesRlsFields() {
        GetReq req = GetReq.builder()
                .collectionName("col")
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();
        assertEquals("alice", req.getRlsPrincipal());
        assertEquals(false, req.getSkipRls());
    }

    @Test
    void toStringIncludesRlsFields() {
        String insert = InsertReq.builder().collectionName("col").rlsPrincipal("alice").skipRls(false).build().toString();
        assertTrue(insert.contains("rlsPrincipal='alice'"));
        assertTrue(insert.contains("skipRls=false"));

        String search = SearchReq.builder().collectionName("col").rlsPrincipal("alice").skipRls(true).build().toString();
        assertTrue(search.contains("rlsPrincipal='alice'"));
        assertTrue(search.contains("skipRls=true"));

        String query = QueryReq.builder().collectionName("col").rlsPrincipal("alice").skipRls(false).build().toString();
        assertTrue(query.contains("rlsPrincipal='alice'"));

        String delete = DeleteReq.builder().collectionName("col").rlsPrincipal("alice").skipRls(false).build().toString();
        assertTrue(delete.contains("rlsPrincipal='alice'"));
        assertTrue(delete.contains("skipRls=false"));

        String upsert = UpsertReq.builder().collectionName("col").rlsPrincipal("alice").skipRls(false).build().toString();
        assertTrue(upsert.contains("rlsPrincipal='alice'"));

        String get = GetReq.builder().collectionName("col").rlsPrincipal("alice").skipRls(false).build().toString();
        assertTrue(get.contains("rlsPrincipal='alice'"));
        assertTrue(get.contains("skipRls=false"));

        String queryIter = QueryIteratorReq.builder().collectionName("col").rlsPrincipal("alice").skipRls(false).build().toString();
        assertTrue(queryIter.contains("rlsPrincipal='alice'"));

        String searchIter = SearchIteratorReq.builder().collectionName("col").rlsPrincipal("alice").skipRls(false).build().toString();
        assertTrue(searchIter.contains("rlsPrincipal='alice'"));

        String searchIterV2 = SearchIteratorReqV2.builder().collectionName("col").rlsPrincipal("alice").skipRls(false).build().toString();
        assertTrue(searchIterV2.contains("rlsPrincipal='alice'"));
    }

    @Test
    void searchIteratorParamToStringIncludesRlsFields() {
        SearchIteratorParam param = SearchIteratorParam.newBuilder()
                .withCollectionName("col")
                .withVectorFieldName("vector")
                .withMetricType(MetricType.L2)
                .withFloatVectors(Collections.singletonList(Collections.singletonList(1.0f)))
                .withBatchSize(10L)
                .withRlsPrincipal("alice")
                .withSkipRls(false)
                .build();
        assertTrue(param.toString().contains("rlsPrincipal='alice'"));
        assertTrue(param.toString().contains("skipRls=false"));
    }

    @Test
    void searchIteratorAdapterForwardsRlsFieldsToV1Param() {
        SearchIteratorReq req = SearchIteratorReq.builder()
                .collectionName("col")
                .vectorFieldName("vector")
                .metricType(IndexParam.MetricType.COSINE)
                .vectors(Collections.singletonList(new FloatVec(Collections.singletonList(1.0f))))
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();

        SearchIteratorParam param = IteratorAdapterV2.convertV2Req(req);
        assertEquals("alice", param.getRlsPrincipal());
        assertEquals(false, param.getSkipRls());
    }
}
