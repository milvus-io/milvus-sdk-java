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

package io.milvus.unit.v2.service.vector.response.aggregation;

import io.milvus.v2.service.vector.response.aggregation.AggregationHit;

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
class AggregationHitTest {

    @Test
    void builderSetsFields() {
        Map<String, Object> fields = new HashMap<>();
        fields.put("title", "hello");
        Map<String, Long> fieldIds = new HashMap<>();
        fieldIds.put("title", 1L);

        AggregationHit hit = AggregationHit.builder()
                .id(42L)
                .score(0.9f)
                .fields(fields)
                .fieldIds(fieldIds)
                .build();

        assertEquals(42L, hit.getId());
        assertEquals(0.9f, hit.getScore());
        assertEquals(fields, hit.getFields());
        assertEquals(fieldIds, hit.getFieldIds());
    }

    @Test
    void addFieldAddsValueAndOptionalId() {
        AggregationHit hit = AggregationHit.builder()
                .addField("title", "hello", 1L)
                .addField("score", 0.5, null)
                .build();

        assertEquals("hello", hit.getFields().get("title"));
        assertEquals(1L, hit.getFieldIds().get("title"));
        assertTrue(hit.getFields().containsKey("score"));
        assertTrue(!hit.getFieldIds().containsKey("score"));
    }

    @Test
    void builderDefaultsToEmptyMaps() {
        AggregationHit hit = AggregationHit.builder().build();
        assertNull(hit.getId());
        assertNull(hit.getScore());
        assertNotNull(hit.getFields());
        assertTrue(hit.getFields().isEmpty());
        assertNotNull(hit.getFieldIds());
        assertTrue(hit.getFieldIds().isEmpty());
    }

    @Test
    void toStringContainsFields() {
        AggregationHit hit = AggregationHit.builder().id(1L).build();
        assertNotNull(hit.toString());
    }
}
