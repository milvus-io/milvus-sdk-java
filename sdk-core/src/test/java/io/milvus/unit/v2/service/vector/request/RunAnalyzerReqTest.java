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

import io.milvus.v2.service.vector.request.RunAnalyzerReq;

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
class RunAnalyzerReqTest {

    @Test
    void builderSetsAllFields() {
        Map<String, Object> analyzerParams = new HashMap<>();
        analyzerParams.put("tokenizer", "standard");
        RunAnalyzerReq req = RunAnalyzerReq.builder()
                .texts(Arrays.asList("hello", "world"))
                .analyzerParams(analyzerParams)
                .withDetail(true)
                .withHash(true)
                .databaseName("db")
                .collectionName("col")
                .fieldName("text")
                .analyzerNames(Arrays.asList("a1", "a2"))
                .build();

        assertEquals(Arrays.asList("hello", "world"), req.getTexts());
        assertEquals(analyzerParams, req.getAnalyzerParams());
        assertEquals(true, req.getWithDetail());
        assertEquals(true, req.getWithHash());
        assertEquals("db", req.getDatabaseName());
        assertEquals("col", req.getCollectionName());
        assertEquals("text", req.getFieldName());
        assertEquals(Arrays.asList("a1", "a2"), req.getAnalyzerNames());
    }

    @Test
    void builderDefaults() {
        RunAnalyzerReq req = RunAnalyzerReq.builder().build();

        assertTrue(req.getTexts().isEmpty());
        assertTrue(req.getAnalyzerParams().isEmpty());
        assertEquals(false, req.getWithDetail());
        assertEquals(false, req.getWithHash());
        assertEquals("", req.getDatabaseName());
        assertEquals("", req.getCollectionName());
        assertEquals("", req.getFieldName());
        assertTrue(req.getAnalyzerNames().isEmpty());
    }

    @Test
    void settersUpdateFields() {
        RunAnalyzerReq req = RunAnalyzerReq.builder().build();

        req.setTexts(Collections.singletonList("solo"));
        req.setAnalyzerParams(Collections.singletonMap("filter", "lowercase"));
        req.setWithDetail(true);
        req.setWithHash(false);
        req.setDatabaseName("db2");
        req.setCollectionName("col2");
        req.setFieldName("body");
        req.setAnalyzerNames(Collections.singletonList("standard"));

        assertEquals(Collections.singletonList("solo"), req.getTexts());
        assertEquals("lowercase", req.getAnalyzerParams().get("filter"));
        assertEquals(true, req.getWithDetail());
        assertEquals(false, req.getWithHash());
        assertEquals("db2", req.getDatabaseName());
        assertEquals("col2", req.getCollectionName());
        assertEquals("body", req.getFieldName());
        assertEquals(Collections.singletonList("standard"), req.getAnalyzerNames());
    }

    @Test
    void toStringContainsFields() {
        RunAnalyzerReq req = RunAnalyzerReq.builder().collectionName("col").build();
        assertNotNull(req.toString());
    }
}
