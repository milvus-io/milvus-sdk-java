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

package io.milvus.unit.v2.service.vector.request;

import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.FunctionScore;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class FunctionScoreTest {

    private CreateCollectionReq.Function function(String name) {
        return CreateCollectionReq.Function.builder().name(name).build();
    }

    @Test
    void builderSetsFunctionsAndParams() {
        Map<String, String> params = new HashMap<>();
        params.put("strategy", "rrf");
        FunctionScore score = FunctionScore.builder()
                .functions(Arrays.asList(function("f1"), function("f2")))
                .params(params)
                .build();

        assertEquals(2, score.getFunctions().size());
        assertEquals("f1", score.getFunctions().get(0).getName());
        assertEquals(params, score.getParams());
    }

    @Test
    void builderAddFunctionAccumulates() {
        FunctionScore score = FunctionScore.builder()
                .addFunction(function("a"))
                .addFunction(function("b"))
                .build();

        assertEquals(2, score.getFunctions().size());
        assertEquals("a", score.getFunctions().get(0).getName());
        assertEquals("b", score.getFunctions().get(1).getName());
        assertTrue(score.getParams().isEmpty());
    }

    @Test
    void builderDefaultsToEmptyCollections() {
        FunctionScore score = FunctionScore.builder().build();

        assertNotNull(score.getFunctions());
        assertTrue(score.getFunctions().isEmpty());
        assertNotNull(score.getParams());
        assertTrue(score.getParams().isEmpty());
    }

    @Test
    void settersUpdateFields() {
        FunctionScore score = FunctionScore.builder().build();

        score.setFunctions(Arrays.asList(function("x")));
        score.setParams(Collections.singletonMap("k", "v"));

        assertEquals(1, score.getFunctions().size());
        assertEquals("x", score.getFunctions().get(0).getName());
        assertEquals("v", score.getParams().get("k"));
    }

    @Test
    void toStringContainsFields() {
        FunctionScore score = FunctionScore.builder().addFunction(function("f")).build();
        assertNotNull(score.toString());
    }
}
