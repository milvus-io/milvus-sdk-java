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

import io.milvus.v2.service.vector.request.highlighter.LexicalHighlighter;
import io.milvus.v2.service.vector.request.highlighter.LexicalHighlighter.HighlightQuery;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class LexicalHighlighterTest {

    private static List<String> jsonArrayToList(String json) {
        return new Gson().fromJson(json, new TypeToken<List<String>>() {}.getType());
    }

    @Test
    void highlightTypeIsLexical() {
        assertEquals("Lexical", LexicalHighlighter.builder().build().highlightType());
    }

    @Test
    void builderSetsAllParams() {
        HighlightQuery query = new HighlightQuery("match", "content", "vector database");
        LexicalHighlighter highlighter = LexicalHighlighter.builder()
                .highlightQueries(Collections.singletonList(query))
                .highlightSearchText(true)
                .preTags(Arrays.asList("<em>"))
                .postTags(Arrays.asList("</em>"))
                .fragmentOffset(5)
                .fragmentSize(10)
                .numOfFragments(3)
                .build();

        Map<String, String> params = highlighter.getParams();
        assertEquals("true", params.get("highlight_search_text"));
        assertEquals(Arrays.asList("<em>"), jsonArrayToList(params.get("pre_tags")));
        assertEquals(Arrays.asList("</em>"), jsonArrayToList(params.get("post_tags")));
        assertEquals("5", params.get("fragment_offset"));
        assertEquals("10", params.get("fragment_size"));
        assertEquals("3", params.get("num_of_fragments"));
        assertTrue(params.get("highlight_query").contains("vector database"));
    }

    @Test
    void builderAddMethodsAccumulate() {
        LexicalHighlighter highlighter = LexicalHighlighter.builder()
                .addHighlightQuery(new HighlightQuery("match", "body", "hello"))
                .addHighlightQuery(new HighlightQuery("match_phrase", "body", "world"))
                .addPreTag("<b>")
                .addPostTag("</b>")
                .build();

        Map<String, String> params = highlighter.getParams();
        assertTrue(params.get("highlight_query").contains("hello"));
        assertTrue(params.get("highlight_query").contains("world"));
        assertEquals(Arrays.asList("<b>"), jsonArrayToList(params.get("pre_tags")));
        assertEquals(Arrays.asList("</b>"), jsonArrayToList(params.get("post_tags")));
    }

    @Test
    void builderDefaultParamsEmpty() {
        Map<String, String> params = LexicalHighlighter.builder().build().getParams();
        assertNotNull(params);
        assertTrue(params.isEmpty());
    }

    @Test
    void nullOptionalParamsAreOmitted() {
        Map<String, String> params = LexicalHighlighter.builder()
                .fragmentOffset(1)
                .build()
                .getParams();
        assertFalse(params.containsKey("highlight_query"));
        assertFalse(params.containsKey("highlight_search_text"));
        assertFalse(params.containsKey("pre_tags"));
        assertFalse(params.containsKey("post_tags"));
        assertFalse(params.containsKey("fragment_size"));
        assertFalse(params.containsKey("num_of_fragments"));
        assertEquals("1", params.get("fragment_offset"));
    }

    @Test
    void highlightQueryCarriesFields() {
        HighlightQuery query = new HighlightQuery("match", "content", "text");
        assertEquals("match", query.type);
        assertEquals("content", query.field);
        assertEquals("text", query.text);
        assertNotNull(query.toString());
    }
}
