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

package io.milvus.unit.v2.service.partition;

import io.milvus.v2.service.partition.response.GetPartitionStatsResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class GetPartitionStatsRespTest {
    @Test
    void builderBuildsAllFields() {
        Map<String, String> stats = new HashMap<>();
        stats.put("row_count", "100");

        GetPartitionStatsResp response = GetPartitionStatsResp.builder()
                .numOfEntities(1000L)
                .stats(stats)
                .build();

        assertEquals(Long.valueOf(1000L), response.getNumOfEntities());
        assertEquals(stats, response.getStats());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        GetPartitionStatsResp response = GetPartitionStatsResp.builder().build();

        assertNull(response.getNumOfEntities());
        assertNotNull(response.getStats());
        assertTrue(response.getStats().isEmpty());
    }

    @Test
    void settersUpdateFields() {
        GetPartitionStatsResp response = GetPartitionStatsResp.builder().build();

        response.setNumOfEntities(50L);
        response.setStats(Collections.singletonMap("key", "value"));

        assertEquals(Long.valueOf(50L), response.getNumOfEntities());
        assertEquals(Collections.singletonMap("key", "value"), response.getStats());
    }

    @Test
    void boundaryNumOfEntitiesIsAccepted() {
        GetPartitionStatsResp zero = GetPartitionStatsResp.builder().numOfEntities(0L).build();
        GetPartitionStatsResp negative = GetPartitionStatsResp.builder().numOfEntities(-5L).build();

        assertEquals(Long.valueOf(0L), zero.getNumOfEntities());
        assertEquals(Long.valueOf(-5L), negative.getNumOfEntities());
    }
}
