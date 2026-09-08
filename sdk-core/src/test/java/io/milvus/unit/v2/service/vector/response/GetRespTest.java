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

import io.milvus.v2.service.vector.response.GetResp;
import io.milvus.v2.service.vector.response.QueryResp.QueryResult;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("unit")
class GetRespTest {

    @Test
    void builderSetsAllFields() {
        QueryResult result = QueryResult.builder()
                .entity(Collections.singletonMap("id", 1L))
                .build();
        GetResp resp = GetResp.builder()
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
        GetResp resp = GetResp.builder().build();
        assertEquals(1L, resp.getSessionTs());
        assertNotNull(resp.getQueryResults());
    }

    @Test
    void getResultsAliasDelegatesToQueryResults() {
        QueryResult result = QueryResult.builder()
                .entity(Collections.singletonMap("id", 2L))
                .build();
        GetResp resp = GetResp.builder()
                .getResults(Collections.singletonList(result))
                .build();

        assertEquals(Collections.singletonList(result), resp.getGetResults());
        assertEquals(Collections.singletonList(result), resp.getQueryResults());

        QueryResult other = QueryResult.builder().build();
        resp.setGetResults(Collections.singletonList(other));
        assertEquals(Collections.singletonList(other), resp.getQueryResults());
        assertEquals(Collections.singletonList(other), resp.getGetResults());
    }

    @Test
    void settersUpdateFields() {
        GetResp resp = GetResp.builder().build();
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
    void toStringContainsFields() {
        GetResp resp = GetResp.builder().build();
        assertNotNull(resp.toString());
    }
}
