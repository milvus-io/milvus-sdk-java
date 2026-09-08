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

package io.milvus.unit.v2.service.vector.request.ranker;

import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.service.vector.request.ranker.WeightedRanker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class WeightedRankerTest {

    @Test
    void builderDefaultsToEmptyWeights() {
        WeightedRanker ranker = WeightedRanker.builder().build();
        assertTrue(ranker.getWeights().isEmpty());
        assertEquals(FunctionType.RERANK, ranker.getFunctionType());
    }

    @Test
    void builderSetsWeightsAndInheritedFields() {
        List<Float> weights = Arrays.asList(0.2f, 0.5f, 0.6f);
        WeightedRanker ranker = WeightedRanker.builder()
                .weights(weights)
                .name("weighted")
                .build();

        assertEquals(weights, ranker.getWeights());
        assertEquals("weighted", ranker.getName());
    }

    @Test
    void deprecatedConstructorSetsWeights() {
        WeightedRanker ranker = new WeightedRanker(Arrays.asList(0.4f, 0.6f));
        assertEquals(Arrays.asList(0.4f, 0.6f), ranker.getWeights());
    }

    @Test
    void setterUpdatesWeights() {
        WeightedRanker ranker = WeightedRanker.builder().build();
        ranker.setWeights(Collections.singletonList(1.0f));
        assertEquals(Collections.singletonList(1.0f), ranker.getWeights());
    }

    @Test
    void getParamsContainsStrategyAndWeightsJson() {
        WeightedRanker ranker = WeightedRanker.builder()
                .weights(Arrays.asList(0.2f, 0.5f, 0.6f))
                .build();
        Map<String, String> params = ranker.getParams();

        assertEquals("weighted", params.get("strategy"));
        assertTrue(params.containsKey("params"));
        assertTrue(params.get("params").contains("[0.2"));
        assertTrue(params.get("params").contains("0.6]"));
    }

    @Test
    void toStringContainsFields() {
        WeightedRanker ranker = WeightedRanker.builder().build();
        assertNotNull(ranker.toString());
    }
}
