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

package io.milvus.unit.v2.service.vector.response;

import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.QueryResp.QueryResult;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class QueryRespTest {

    @Test
    void builderSetsAllFields() {
        QueryResult result = QueryResult.builder()
                .entity(Collections.singletonMap("id", 1L))
                .elementOffset(3L)
                .build();
        QueryResp resp = QueryResp.builder()
                .queryResults(Collections.singletonList(result))
                .sessionTs(42L)
                .cost(10L)
                .scannedRemoteBytes(100L)
                .scannedTotalBytes(200L)
                .cacheHitRatio(0.5f)
                .build();

        assertEquals(Collections.singletonList(result), resp.getQueryResults());
        assertEquals(42L, resp.getSessionTs());
        assertEquals(10L, resp.getCost());
        assertEquals(100L, resp.getScannedRemoteBytes());
        assertEquals(200L, resp.getScannedTotalBytes());
        assertEquals(0.5f, resp.getCacheHitRatio());
    }

    @Test
    void builderDefaults() {
        QueryResp resp = QueryResp.builder().build();
        assertEquals(1L, resp.getSessionTs());
        assertNotNull(resp.getQueryResults());
        assertNull(resp.getCost());
        assertNull(resp.getScannedRemoteBytes());
        assertNull(resp.getScannedTotalBytes());
        assertNull(resp.getCacheHitRatio());
    }

    @Test
    void settersUpdateFields() {
        QueryResp resp = QueryResp.builder().build();

        resp.setQueryResults(Collections.singletonList(QueryResult.builder().build()));
        resp.setSessionTs(7L);
        resp.setCost(9L);
        resp.setScannedRemoteBytes(11L);
        resp.setScannedTotalBytes(12L);
        resp.setCacheHitRatio(0.9f);

        assertEquals(1, resp.getQueryResults().size());
        assertEquals(7L, resp.getSessionTs());
        assertEquals(9L, resp.getCost());
        assertEquals(11L, resp.getScannedRemoteBytes());
        assertEquals(12L, resp.getScannedTotalBytes());
        assertEquals(0.9f, resp.getCacheHitRatio());
    }

    @Test
    void queryResultBuilderAndAccessors() {
        Map<String, Object> entity = new HashMap<>();
        entity.put("id", 1L);
        QueryResult result = QueryResult.builder()
                .entity(entity)
                .elementOffset(2L)
                .build();

        assertEquals(entity, result.getEntity());
        assertEquals(2L, result.getElementOffset());

        result.setEntity(Collections.singletonMap("id", 9L));
        result.setElementOffset(5L);
        assertEquals(9L, result.getEntity().get("id"));
        assertEquals(5L, result.getElementOffset());

        assertNotNull(result.toString());
    }

    @Test
    void queryResultDefaultElementOffsetNull() {
        QueryResult result = QueryResult.builder().build();
        assertNull(result.getElementOffset());
        assertNotNull(result.getEntity());
    }

    @Test
    void toStringContainsFields() {
        QueryResp resp = QueryResp.builder().build();
        assertNotNull(resp.toString());
    }
}
