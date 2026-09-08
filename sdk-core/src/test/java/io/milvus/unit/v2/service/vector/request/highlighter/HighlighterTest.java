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

package io.milvus.unit.v2.service.vector.request.highlighter;

import io.milvus.v2.service.vector.request.highlighter.Highlighter;
import io.milvus.v2.service.vector.request.highlighter.LexicalHighlighter;
import io.milvus.v2.service.vector.request.highlighter.SemanticHighlighter;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class HighlighterTest {

    @Test
    void bothImplementationsExposeTypeAndParams() {
        Highlighter lexical = LexicalHighlighter.builder()
                .fragmentSize(10)
                .build();
        Highlighter semantic = SemanticHighlighter.builder()
                .threshold(0.2f)
                .build();

        assertEquals("Lexical", lexical.highlightType());
        assertEquals("Semantic", semantic.highlightType());

        Map<String, String> lexicalParams = lexical.getParams();
        Map<String, String> semanticParams = semantic.getParams();
        assertNotNull(lexicalParams);
        assertNotNull(semanticParams);
        assertTrue(lexicalParams.containsKey("fragment_size"));
        assertTrue(semanticParams.containsKey("threshold"));
    }

    @Test
    void highlightTypesAreDistinct() {
        assertFalse(LexicalHighlighter.builder().build().highlightType()
                .equals(SemanticHighlighter.builder().build().highlightType()));
    }
}
