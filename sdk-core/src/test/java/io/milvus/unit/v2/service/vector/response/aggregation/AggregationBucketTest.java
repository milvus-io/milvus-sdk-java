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

import io.milvus.v2.service.vector.response.aggregation.AggregationBucket;
import io.milvus.v2.service.vector.response.aggregation.AggregationBucket.KeyEntry;
import io.milvus.v2.service.vector.response.aggregation.AggregationHit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class AggregationBucketTest {

    @Test
    void builderSetsAllFields() {
        KeyEntry key = KeyEntry.builder().fieldId(1L).fieldName("color").value("red").build();
        AggregationHit hit = AggregationHit.builder().id(1L).build();
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("avg_score", 0.8);
        AggregationBucket subGroup = AggregationBucket.builder().count(2).build();

        AggregationBucket bucket = AggregationBucket.builder()
                .key(Collections.singletonList(key))
                .count(10)
                .metrics(metrics)
                .hits(Collections.singletonList(hit))
                .subGroups(Collections.singletonList(subGroup))
                .build();

        assertEquals(Collections.singletonList(key), bucket.getKey());
        assertEquals(10, bucket.getCount());
        assertEquals(metrics, bucket.getMetrics());
        assertEquals(Collections.singletonList(hit), bucket.getHits());
        assertEquals(Collections.singletonList(subGroup), bucket.getSubGroups());
    }

    @Test
    void builderDefaults() {
        AggregationBucket bucket = AggregationBucket.builder().build();
        assertTrue(bucket.getKey().isEmpty());
        assertEquals(0, bucket.getCount());
        assertNotNull(bucket.getMetrics());
        assertTrue(bucket.getMetrics().isEmpty());
        assertTrue(bucket.getHits().isEmpty());
        assertTrue(bucket.getSubGroups().isEmpty());
    }

    @Test
    void keyEntryBuilderAndAccessors() {
        KeyEntry entry = KeyEntry.builder()
                .fieldId(3L)
                .fieldName("size")
                .value("L")
                .build();

        assertEquals(3L, entry.getFieldId());
        assertEquals("size", entry.getFieldName());
        assertEquals("L", entry.getValue());
    }

    @Test
    void keyEntryDefaults() {
        KeyEntry entry = KeyEntry.builder().build();
        assertEquals(0, entry.getFieldId());
        assertNull(entry.getFieldName());
        assertNull(entry.getValue());
    }

    @Test
    void toStringContainsFields() {
        AggregationBucket bucket = AggregationBucket.builder().count(1).build();
        assertNotNull(bucket.toString());
        assertNotNull(KeyEntry.builder().fieldName("f").build().toString());
    }
}
