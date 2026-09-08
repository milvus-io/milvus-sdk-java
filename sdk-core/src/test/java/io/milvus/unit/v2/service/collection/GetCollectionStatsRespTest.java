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

package io.milvus.unit.v2.service.collection;

import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class GetCollectionStatsRespTest {

    @Test
    void builderBuildsWithAllFields() {
        Map<String, String> stats = Collections.singletonMap("row_count", "100");
        GetCollectionStatsResp response = GetCollectionStatsResp.builder()
                .numOfEntities(100L)
                .stats(stats)
                .build();

        assertEquals(100L, response.getNumOfEntities());
        assertEquals(stats, response.getStats());
    }

    @Test
    void settersUpdateGetters() {
        GetCollectionStatsResp response = GetCollectionStatsResp.builder().build();

        response.setNumOfEntities(50L);
        assertEquals(50L, response.getNumOfEntities());

        Map<String, String> stats = Collections.singletonMap("k", "v");
        response.setStats(stats);
        assertEquals(stats, response.getStats());
    }

    @Test
    void defaultsApply() {
        GetCollectionStatsResp response = GetCollectionStatsResp.builder().build();
        assertNull(response.getNumOfEntities());
        assertNotNull(response.getStats());
        assertTrue(response.getStats().isEmpty());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(GetCollectionStatsResp.builder());
    }

    @Test
    void toStringContainsFields() {
        GetCollectionStatsResp response = GetCollectionStatsResp.builder().numOfEntities(10L).build();
        assertTrue(response.toString().contains("10"));
    }
}
