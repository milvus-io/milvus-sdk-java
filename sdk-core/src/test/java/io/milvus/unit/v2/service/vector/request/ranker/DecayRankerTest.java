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
import io.milvus.v2.service.vector.request.ranker.DecayRanker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DecayRankerTest {

    @Test
    void builderDefaultFunctionIsGauss() {
        DecayRanker ranker = DecayRanker.builder().build();
        assertEquals("gauss", ranker.getFunction());
        assertEquals(FunctionType.RERANK, ranker.getFunctionType());
    }

    @Test
    void builderSetsFieldsAndInheritedFunction() {
        DecayRanker ranker = DecayRanker.builder()
                .name("time_decay")
                .description("time decay")
                .inputFieldNames(Collections.singletonList("timestamp"))
                .function("exp")
                .origin(100)
                .offset(24)
                .scale(50)
                .decay(0.5)
                .build();

        assertEquals("time_decay", ranker.getName());
        assertEquals("time decay", ranker.getDescription());
        assertEquals(Collections.singletonList("timestamp"), ranker.getInputFieldNames());
        assertEquals("exp", ranker.getFunction());
        assertEquals(100, ranker.getOrigin());
        assertEquals(24, ranker.getOffset());
        assertEquals(50, ranker.getScale());
        assertEquals(0.5, ranker.getDecay());
    }

    @Test
    void builderDefaultsNumericFieldsToNull() {
        DecayRanker ranker = DecayRanker.builder().build();
        assertNull(ranker.getOrigin());
        assertNull(ranker.getOffset());
        assertNull(ranker.getScale());
        assertNull(ranker.getDecay());
    }

    @Test
    void settersUpdateFields() {
        DecayRanker ranker = DecayRanker.builder().build();
        ranker.setFunction("linear");
        ranker.setOrigin(10);
        ranker.setOffset(1);
        ranker.setScale(5);
        ranker.setDecay(0.9);

        assertEquals("linear", ranker.getFunction());
        assertEquals(10, ranker.getOrigin());
        assertEquals(1, ranker.getOffset());
        assertEquals(5, ranker.getScale());
        assertEquals(0.9, ranker.getDecay());
    }

    @Test
    void getParamsDefaultContainsFunctionOnly() {
        DecayRanker ranker = DecayRanker.builder().build();
        Map<String, String> params = ranker.getParams();

        assertEquals("decay", params.get("reranker"));
        assertEquals("gauss", params.get("function"));
        assertFalse(params.containsKey("origin"));
        assertFalse(params.containsKey("offset"));
        assertFalse(params.containsKey("scale"));
        assertFalse(params.containsKey("decay"));
    }

    @Test
    void getParamsIncludesSetNumericFields() {
        DecayRanker ranker = DecayRanker.builder()
                .origin(100)
                .offset(24)
                .scale(50)
                .decay(0.5)
                .build();
        Map<String, String> params = ranker.getParams();

        assertEquals("100", params.get("origin"));
        assertEquals("24", params.get("offset"));
        assertEquals("50", params.get("scale"));
        assertEquals("0.5", params.get("decay"));
    }

    @Test
    void toStringContainsFields() {
        DecayRanker ranker = DecayRanker.builder().build();
        assertNotNull(ranker.toString());
    }
}
