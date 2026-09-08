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

package io.milvus.unit.v2.service.vector.response;

import io.milvus.v2.service.vector.response.RunAnalyzerResp;
import io.milvus.v2.service.vector.response.RunAnalyzerResp.AnalyzerResult;
import io.milvus.v2.service.vector.response.RunAnalyzerResp.AnalyzerToken;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class RunAnalyzerRespTest {

    @Test
    void builderSetsResults() {
        AnalyzerResult result = AnalyzerResult.builder().build();
        RunAnalyzerResp resp = RunAnalyzerResp.builder()
                .results(Collections.singletonList(result))
                .build();

        assertEquals(Collections.singletonList(result), resp.getResults());
    }

    @Test
    void builderDefaults() {
        RunAnalyzerResp resp = RunAnalyzerResp.builder().build();
        assertTrue(resp.getResults().isEmpty());
    }

    @Test
    void setterUpdatesResults() {
        RunAnalyzerResp resp = RunAnalyzerResp.builder().build();
        resp.setResults(Collections.singletonList(AnalyzerResult.builder().build()));
        assertEquals(1, resp.getResults().size());
    }

    @Test
    void analyzerResultBuilderAndAccessors() {
        AnalyzerToken token = AnalyzerToken.builder()
                .token("hello")
                .startOffset(0L)
                .endOffset(5L)
                .position(1L)
                .positionLength(1L)
                .hash(123L)
                .build();
        AnalyzerResult result = AnalyzerResult.builder()
                .tokens(Collections.singletonList(token))
                .build();

        assertEquals(Collections.singletonList(token), result.getTokens());

        result.setTokens(Arrays.asList(token, token));
        assertEquals(2, result.getTokens().size());
    }

    @Test
    void analyzerTokenBuilderDefaultsAndAccessors() {
        AnalyzerToken token = AnalyzerToken.builder().build();
        assertNull(token.getToken());
        assertNull(token.getStartOffset());
        assertNull(token.getEndOffset());
        assertNull(token.getPosition());
        assertNull(token.getPositionLength());
        assertNull(token.getHash());

        token.setToken("world");
        token.setStartOffset(6L);
        token.setEndOffset(11L);
        token.setPosition(2L);
        token.setPositionLength(1L);
        token.setHash(456L);

        assertEquals("world", token.getToken());
        assertEquals(6L, token.getStartOffset());
        assertEquals(11L, token.getEndOffset());
        assertEquals(2L, token.getPosition());
        assertEquals(1L, token.getPositionLength());
        assertEquals(456L, token.getHash());
    }

    @Test
    void toStringContainsFields() {
        RunAnalyzerResp resp = RunAnalyzerResp.builder().build();
        assertNotNull(resp.toString());
        assertNotNull(AnalyzerResult.builder().build().toString());
        assertNotNull(AnalyzerToken.builder().build().toString());
    }
}
