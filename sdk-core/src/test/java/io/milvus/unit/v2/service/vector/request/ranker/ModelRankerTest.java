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
import io.milvus.v2.service.vector.request.ranker.ModelRanker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ModelRankerTest {

    @Test
    void builderDefaultProviderIsTei() {
        ModelRanker ranker = ModelRanker.builder().build();
        assertEquals("tei", ranker.getProvider());
        assertTrue(ranker.getQueries().isEmpty());
        assertNull(ranker.getEndpoint());
        assertEquals(FunctionType.RERANK, ranker.getFunctionType());
    }

    @Test
    void builderSetsFieldsAndInheritedFunction() {
        ModelRanker ranker = ModelRanker.builder()
                .name("semantic_ranker")
                .description("semantic ranker")
                .inputFieldNames(Collections.singletonList("document"))
                .provider("vllm")
                .queries(Arrays.asList("machine learning for time series", "forecasting"))
                .endpoint("http://model-service:8080")
                .build();

        assertEquals("semantic_ranker", ranker.getName());
        assertEquals("semantic ranker", ranker.getDescription());
        assertEquals(Collections.singletonList("document"), ranker.getInputFieldNames());
        assertEquals("vllm", ranker.getProvider());
        assertEquals(Arrays.asList("machine learning for time series", "forecasting"), ranker.getQueries());
        assertEquals("http://model-service:8080", ranker.getEndpoint());
    }

    @Test
    void settersUpdateFields() {
        ModelRanker ranker = ModelRanker.builder().build();
        ranker.setProvider("tei");
        ranker.setQueries(Collections.singletonList("q"));
        ranker.setEndpoint("http://localhost:8080");

        assertEquals("tei", ranker.getProvider());
        assertEquals(Collections.singletonList("q"), ranker.getQueries());
        assertEquals("http://localhost:8080", ranker.getEndpoint());
    }

    @Test
    void getParamsContainsProviderAndQueries() {
        ModelRanker ranker = ModelRanker.builder()
                .provider("tei")
                .queries(Arrays.asList("a", "b"))
                .endpoint("http://model-service:8080")
                .build();
        Map<String, String> params = ranker.getParams();

        assertEquals("model", params.get("reranker"));
        assertEquals("tei", params.get("provider"));
        assertTrue(params.get("queries").contains("a"));
        assertTrue(params.get("queries").contains("b"));
        assertEquals("http://model-service:8080", params.get("endpoint"));
    }

    @Test
    void getParamsDefaultQueriesEmptyAndNoEndpoint() {
        ModelRanker ranker = ModelRanker.builder().build();
        Map<String, String> params = ranker.getParams();

        assertEquals("[]", params.get("queries"));
        assertFalse(params.containsKey("endpoint"));
    }

    @Test
    void toStringContainsFields() {
        ModelRanker ranker = ModelRanker.builder().build();
        assertNotNull(ranker.toString());
    }
}
