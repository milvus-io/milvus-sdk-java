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
import io.milvus.v2.service.vector.request.ranker.BoostRanker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class BoostRankerTest {

    @Test
    void builderSetsFields() {
        BoostRanker ranker = BoostRanker.builder()
                .name("xxx_boost")
                .description("boost on xxx")
                .filter("xxx == 2")
                .weight(0.5f)
                .randomScoreSeed(123L)
                .randomScoreField("id")
                .build();

        assertEquals("xxx_boost", ranker.getName());
        assertEquals("boost on xxx", ranker.getDescription());
        assertEquals("xxx == 2", ranker.getFilter());
        assertEquals(0.5f, ranker.getWeight());
        assertEquals(123L, ranker.getRandomScoreSeed());
        assertEquals("id", ranker.getRandomScoreField());
        assertEquals(FunctionType.RERANK, ranker.getFunctionType());
    }

    @Test
    void builderDefaults() {
        BoostRanker ranker = BoostRanker.builder().build();
        assertNull(ranker.getFilter());
        assertNull(ranker.getWeight());
        assertNull(ranker.getRandomScoreSeed());
        assertNull(ranker.getRandomScoreField());
    }

    @Test
    void getParamsDefaultOnlyContainsReranker() {
        BoostRanker ranker = BoostRanker.builder().build();
        Map<String, String> params = ranker.getParams();

        assertEquals("boost", params.get("reranker"));
        assertFalse(params.containsKey("filter"));
        assertFalse(params.containsKey("weight"));
        assertFalse(params.containsKey("random_score"));
    }

    @Test
    void getParamsIncludesSetFields() {
        BoostRanker ranker = BoostRanker.builder()
                .filter("xxx == 2")
                .weight(0.5f)
                .randomScoreSeed(123L)
                .randomScoreField("id")
                .build();
        Map<String, String> params = ranker.getParams();

        assertEquals("boost", params.get("reranker"));
        assertEquals("xxx == 2", params.get("filter"));
        assertEquals("0.5", params.get("weight"));
        String randomScore = params.get("random_score");
        assertNotNull(randomScore);
        assertTrue(randomScore.contains("123"));
        assertTrue(randomScore.contains("\"field\":\"id\"") || randomScore.contains("\"field\": \"id\""));
    }

    @Test
    void getParamsRandomScoreFromSeedOnly() {
        BoostRanker ranker = BoostRanker.builder().randomScoreSeed(7L).build();
        Map<String, String> params = ranker.getParams();

        assertTrue(params.get("random_score").contains("7"));
        assertFalse(params.get("random_score").contains("field"));
    }

    @Test
    void getParamsRandomScoreFromFieldOnly() {
        BoostRanker ranker = BoostRanker.builder().randomScoreField("id").build();
        Map<String, String> params = ranker.getParams();

        assertTrue(params.get("random_score").contains("id"));
        assertFalse(params.get("random_score").contains("seed"));
    }
}
