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

package io.milvus.unit.v1.param;

import io.milvus.param.dml.ranker.WeightedRanker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class WeightedRankerTest {

    @Test
    void builderSetsWeights() {
        WeightedRanker ranker = WeightedRanker.newBuilder()
                .withWeights(Arrays.asList(0.5F, 0.5F))
                .build();

        assertEquals(Arrays.asList(0.5F, 0.5F), ranker.getWeights());
    }

    @Test
    void weightsDefaultToEmpty() {
        WeightedRanker ranker = WeightedRanker.newBuilder().build();

        assertTrue(ranker.getWeights().isEmpty());
    }

    @Test
    void nullWeightsIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> WeightedRanker.newBuilder().withWeights(null));
    }

    @Test
    void getPropertiesUsesWeightedStrategy() {
        WeightedRanker ranker = WeightedRanker.newBuilder()
                .withWeights(Arrays.asList(0.3F, 0.7F))
                .build();

        Map<String, String> properties = ranker.getProperties();
        assertEquals("weighted", properties.get("strategy"));
        assertTrue(properties.containsKey("params"));
        assertTrue(properties.get("params").contains("0.3"));
        assertTrue(properties.get("params").contains("0.7"));
    }

    @Test
    void getPropertiesEmptyWeights() {
        WeightedRanker ranker = WeightedRanker.newBuilder().build();

        Map<String, String> properties = ranker.getProperties();
        assertEquals("weighted", properties.get("strategy"));
        assertTrue(properties.get("params").contains("weights"));
    }
}
