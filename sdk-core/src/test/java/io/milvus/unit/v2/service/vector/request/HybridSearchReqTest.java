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

import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.FunctionScore;
import io.milvus.v2.service.vector.request.HybridSearchReq;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class HybridSearchReqTest {

    @Test
    void builderSetsAllFields() {
        AnnSearchReq annReq = AnnSearchReq.builder().vectorFieldName("vector").limit(5).build();
        CreateCollectionReq.Function ranker = CreateCollectionReq.Function.builder().name("rank").build();
        FunctionScore functionScore = FunctionScore.builder().build();

        HybridSearchReq req = HybridSearchReq.builder()
                .databaseName("db")
                .collectionName("col")
                .clusterId("cluster-1")
                .partitionNames(Arrays.asList("p0", "p1"))
                .searchRequests(Collections.singletonList(annReq))
                .ranker(ranker)
                .functionScore(functionScore)
                .limit(20)
                .outFields(Arrays.asList("id", "name"))
                .offset(2)
                .roundDecimal(3)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .groupByFieldName("color")
                .groupSize(4)
                .strictGroupSize(true)
                .build();

        assertEquals("db", req.getDatabaseName());
        assertEquals("col", req.getCollectionName());
        assertEquals("cluster-1", req.getClusterId());
        assertEquals(Arrays.asList("p0", "p1"), req.getPartitionNames());
        assertEquals(Collections.singletonList(annReq), req.getSearchRequests());
        assertEquals(ranker, req.getRanker());
        assertEquals(functionScore, req.getFunctionScore());
        assertEquals(20, req.getLimit());
        assertEquals(20, req.getTopK());
        assertEquals(Arrays.asList("id", "name"), req.getOutFields());
        assertEquals(2, req.getOffset());
        assertEquals(3, req.getRoundDecimal());
        assertEquals(ConsistencyLevel.STRONG, req.getConsistencyLevel());
        assertEquals("color", req.getGroupByFieldName());
        assertEquals(4, req.getGroupSize());
        assertEquals(true, req.getStrictGroupSize());
    }

    @Test
    void builderDefaults() {
        HybridSearchReq req = HybridSearchReq.builder().build();

        assertNull(req.getDatabaseName());
        assertNull(req.getCollectionName());
        assertNull(req.getClusterId());
        assertNull(req.getPartitionNames());
        assertNull(req.getSearchRequests());
        assertNull(req.getRanker());
        assertNull(req.getFunctionScore());
        assertEquals(0, req.getTopK());
        assertEquals(0L, req.getLimit());
        assertNull(req.getOutFields());
        assertEquals(0, req.getOffset());
        assertEquals(-1, req.getRoundDecimal());
        assertNull(req.getConsistencyLevel());
        assertNull(req.getGroupByFieldName());
        assertNull(req.getGroupSize());
        assertNull(req.getStrictGroupSize());
    }

    @Test
    void topKAndLimitRemainInSync() {
        HybridSearchReq req = HybridSearchReq.builder().topK(6).build();
        assertEquals(6, req.getTopK());
        assertEquals(6L, req.getLimit());

        req.setTopK(9);
        assertEquals(9, req.getTopK());
        assertEquals(9L, req.getLimit());

        req.setLimit(11);
        assertEquals(11, req.getLimit());
        assertEquals(11, req.getTopK());
    }

    @Test
    void settersUpdateFields() {
        HybridSearchReq req = HybridSearchReq.builder().build();

        req.setDatabaseName("db2");
        req.setCollectionName("col2");
        req.setClusterId("cluster-2");
        req.setPartitionNames(Collections.singletonList("p2"));
        req.setSearchRequests(Collections.singletonList(
                AnnSearchReq.builder().vectorFieldName("vector").build()));
        req.setRanker(CreateCollectionReq.Function.builder().name("r").build());
        req.setFunctionScore(FunctionScore.builder().build());
        req.setOutFields(Collections.singletonList("id"));
        req.setOffset(1);
        req.setRoundDecimal(2);
        req.setConsistencyLevel(ConsistencyLevel.EVENTUALLY);
        req.setGroupByFieldName("g");
        req.setGroupSize(2);
        req.setStrictGroupSize(false);

        assertEquals("db2", req.getDatabaseName());
        assertEquals("col2", req.getCollectionName());
        assertEquals("cluster-2", req.getClusterId());
        assertEquals(1, req.getPartitionNames().size());
        assertEquals(1, req.getSearchRequests().size());
        assertNotNull(req.getRanker());
        assertNotNull(req.getFunctionScore());
        assertEquals(1, req.getOutFields().size());
        assertEquals(1, req.getOffset());
        assertEquals(2, req.getRoundDecimal());
        assertEquals(ConsistencyLevel.EVENTUALLY, req.getConsistencyLevel());
        assertEquals("g", req.getGroupByFieldName());
        assertEquals(2, req.getGroupSize());
        assertEquals(false, req.getStrictGroupSize());
    }

    @Test
    void toStringContainsFields() {
        HybridSearchReq req = HybridSearchReq.builder().collectionName("col").build();
        assertNotNull(req.toString());
        assertTrue(req.toString().contains("col"));
    }
}
