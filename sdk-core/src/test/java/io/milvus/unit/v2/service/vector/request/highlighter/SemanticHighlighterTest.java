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

import io.milvus.v2.service.vector.request.highlighter.SemanticHighlighter;

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
class SemanticHighlighterTest {

    private static List<String> jsonArrayToList(String json) {
        return new Gson().fromJson(json, new TypeToken<List<String>>() {}.getType());
    }

    @Test
    void highlightTypeIsSemantic() {
        assertEquals("Semantic", SemanticHighlighter.builder().build().highlightType());
    }

    @Test
    void builderSetsAllParams() {
        SemanticHighlighter highlighter = SemanticHighlighter.builder()
                .queries(Arrays.asList("machine learning", "forecasting"))
                .inputFields(Arrays.asList("content", "summary"))
                .preTags(Arrays.asList("<em>"))
                .postTags(Arrays.asList("</em>"))
                .threshold(0.2f)
                .highlightOnly(true)
                .modelDeploymentID("deploy-1")
                .maxClientBatchSize(64)
                .build();

        Map<String, String> params = highlighter.getParams();
        assertTrue(params.get("queries").contains("machine learning"));
        assertTrue(params.get("input_fields").contains("summary"));
        assertEquals(Arrays.asList("<em>"), jsonArrayToList(params.get("pre_tags")));
        assertEquals(Arrays.asList("</em>"), jsonArrayToList(params.get("post_tags")));
        assertEquals("0.2", params.get("threshold"));
        assertEquals("true", params.get("highlight_only"));
        assertEquals("deploy-1", params.get("model_deployment_id"));
        assertEquals("64", params.get("max_client_batch_size"));
    }

    @Test
    void builderAddMethodsAccumulate() {
        SemanticHighlighter highlighter = SemanticHighlighter.builder()
                .addQuery("q1")
                .addQuery("q2")
                .addInputField("f1")
                .addInputField("f2")
                .addPreTag("<b>")
                .addPostTag("</b>")
                .build();

        Map<String, String> params = highlighter.getParams();
        assertTrue(params.get("queries").contains("q1"));
        assertTrue(params.get("queries").contains("q2"));
        assertTrue(params.get("input_fields").contains("f1"));
        assertTrue(params.get("input_fields").contains("f2"));
        assertEquals(Arrays.asList("<b>"), jsonArrayToList(params.get("pre_tags")));
        assertEquals(Arrays.asList("</b>"), jsonArrayToList(params.get("post_tags")));
    }

    @Test
    void builderDefaultParamsEmpty() {
        Map<String, String> params = SemanticHighlighter.builder().build().getParams();
        assertNotNull(params);
        assertTrue(params.isEmpty());
    }

    @Test
    void nullOptionalParamsAreOmitted() {
        Map<String, String> params = SemanticHighlighter.builder()
                .threshold(0.5f)
                .build()
                .getParams();
        assertFalse(params.containsKey("queries"));
        assertFalse(params.containsKey("input_fields"));
        assertFalse(params.containsKey("pre_tags"));
        assertFalse(params.containsKey("post_tags"));
        assertFalse(params.containsKey("highlight_only"));
        assertFalse(params.containsKey("model_deployment_id"));
        assertFalse(params.containsKey("max_client_batch_size"));
        assertEquals("0.5", params.get("threshold"));
    }
}
