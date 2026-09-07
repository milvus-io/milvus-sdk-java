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
import io.milvus.v2.service.vector.request.ranker.RRFRanker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class RRFRankerTest {

    @Test
    void builderDefaultKIs60() {
        RRFRanker ranker = RRFRanker.builder().build();
        assertEquals(60, ranker.getK());
        assertEquals(FunctionType.RERANK, ranker.getFunctionType());
    }

    @Test
    void builderSetsKAndInheritedFunctionFields() {
        RRFRanker ranker = RRFRanker.builder()
                .k(20)
                .name("rrf")
                .description("rank fusion")
                .inputFieldNames(Collections.singletonList("$score"))
                .outputFieldNames(Collections.singletonList("score"))
                .build();

        assertEquals(20, ranker.getK());
        assertEquals("rrf", ranker.getName());
        assertEquals("rank fusion", ranker.getDescription());
        assertEquals(Collections.singletonList("$score"), ranker.getInputFieldNames());
        assertEquals(Collections.singletonList("score"), ranker.getOutputFieldNames());
    }

    @Test
    void deprecatedConstructorSetsK() {
        RRFRanker ranker = new RRFRanker(10);
        assertEquals(10, ranker.getK());
    }

    @Test
    void setterUpdatesK() {
        RRFRanker ranker = RRFRanker.builder().build();
        ranker.setK(15);
        assertEquals(15, ranker.getK());
    }

    @Test
    void getParamsContainsStrategyAndParamsJson() {
        RRFRanker ranker = RRFRanker.builder().k(10).build();
        Map<String, String> params = ranker.getParams();

        assertEquals("rrf", params.get("strategy"));
        assertTrue(params.containsKey("params"));
        assertEquals("{\"k\":10}", params.get("params"));
    }

    @Test
    void toStringContainsFields() {
        RRFRanker ranker = RRFRanker.builder().k(60).build();
        assertNotNull(ranker.toString());
    }
}
