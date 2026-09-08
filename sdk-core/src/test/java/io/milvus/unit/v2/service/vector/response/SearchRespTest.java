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

import io.milvus.v2.service.vector.response.SearchResp;
import io.milvus.v2.service.vector.response.SearchResp.HighlightResult;
import io.milvus.v2.service.vector.response.SearchResp.SearchResult;
import io.milvus.v2.service.vector.response.aggregation.AggregationBucket;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class SearchRespTest {

    @Test
    void builderSetsAllFields() {
        SearchResult result = SearchResult.builder().id(1L).score(0.9f).build();
        AggregationBucket bucket = AggregationBucket.builder().count(5).build();
        SearchResp resp = SearchResp.builder()
                .searchResults(Collections.singletonList(Collections.singletonList(result)))
                .sessionTs(42L)
                .recalls(Arrays.asList(1.0f, 0.5f))
                .cost(10L)
                .scannedRemoteBytes(100L)
                .scannedTotalBytes(200L)
                .cacheHitRatio(0.5f)
                .aggregationBuckets(Collections.singletonList(Collections.singletonList(bucket)))
                .build();

        assertEquals(Collections.singletonList(Collections.singletonList(result)), resp.getSearchResults());
        assertEquals(42L, resp.getSessionTs());
        assertEquals(Arrays.asList(1.0f, 0.5f), resp.getRecalls());
        assertEquals(10L, resp.getCost());
        assertEquals(100L, resp.getScannedRemoteBytes());
        assertEquals(200L, resp.getScannedTotalBytes());
        assertEquals(0.5f, resp.getCacheHitRatio());
        assertEquals(1, resp.getAggregationBuckets().size());
        assertEquals(5, resp.getAggregationBuckets().get(0).get(0).getCount());
    }

    @Test
    void builderDefaults() {
        SearchResp resp = SearchResp.builder().build();
        assertEquals(1L, resp.getSessionTs());
        assertNotNull(resp.getSearchResults());
        assertNotNull(resp.getRecalls());
        assertNotNull(resp.getAggregationBuckets());
    }

    @Test
    void settersUpdateFields() {
        SearchResp resp = SearchResp.builder().build();

        resp.setSearchResults(Collections.singletonList(Collections.singletonList(
                SearchResult.builder().id(1L).build())));
        resp.setSessionTs(7L);
        resp.setRecalls(Collections.singletonList(0.9f));
        resp.setCost(9L);
        resp.setScannedRemoteBytes(11L);
        resp.setScannedTotalBytes(12L);
        resp.setCacheHitRatio(0.9f);
        resp.setAggregationBuckets(Collections.singletonList(Collections.singletonList(
                AggregationBucket.builder().count(2).build())));

        assertEquals(1, resp.getSearchResults().size());
        assertEquals(7L, resp.getSessionTs());
        assertEquals(1, resp.getRecalls().size());
        assertEquals(9L, resp.getCost());
        assertEquals(11L, resp.getScannedRemoteBytes());
        assertEquals(12L, resp.getScannedTotalBytes());
        assertEquals(0.9f, resp.getCacheHitRatio());
        assertEquals(2, resp.getAggregationBuckets().get(0).get(0).getCount());
    }

    @Test
    void searchResultBuilderAndAccessors() {
        Map<String, Object> entity = new HashMap<>();
        entity.put("vector", Arrays.asList(1.0f, 2.0f));
        HighlightResult highlight = HighlightResult.builder()
                .fieldName("content")
                .addFragment("frag")
                .addScore(0.8f)
                .build();

        SearchResult result = SearchResult.builder()
                .entity(entity)
                .score(0.95f)
                .id(42L)
                .primaryKey("pk")
                .addHighlightResult("content", highlight)
                .elementOffset(1L)
                .build();

        assertEquals(entity, result.getEntity());
        assertEquals(0.95f, result.getScore());
        assertEquals(42L, result.getId());
        assertEquals("pk", result.getPrimaryKey());
        assertEquals(highlight, result.getHighlightResult("content"));
        assertEquals(1L, result.getElementOffset());

        result.setEntity(Collections.singletonMap("id", 1L));
        result.setScore(0.1f);
        result.setId("id1");
        result.setPrimaryKey("id");
        result.setElementOffset(2L);
        assertEquals(1L, result.getEntity().get("id"));
        assertEquals(0.1f, result.getScore());
        assertEquals("id1", result.getId());
        assertEquals("id", result.getPrimaryKey());
        assertEquals(2L, result.getElementOffset());
    }

    @Test
    void searchResultDefaults() {
        SearchResult result = SearchResult.builder().build();
        assertEquals("id", result.getPrimaryKey());
        assertNull(result.getScore());
        assertNull(result.getId());
        assertNull(result.getElementOffset());
        assertNotNull(result.getEntity());
        assertNotNull(result.getHighlightResults());
    }

    @Test
    void highlightResultBuilderAndAccessors() {
        HighlightResult result = HighlightResult.builder()
                .fieldName("content")
                .fragments(Arrays.asList("f1", "f2"))
                .scores(Arrays.asList(0.9f, 0.8f))
                .build();

        assertEquals("content", result.getFieldName());
        assertEquals(Arrays.asList("f1", "f2"), result.getFragments());
        assertEquals(Arrays.asList(0.9f, 0.8f), result.getScores());

        HighlightResult added = HighlightResult.builder()
                .addFragment("f3")
                .addScore(0.7f)
                .build();
        assertEquals(Arrays.asList("f3"), added.getFragments());
        assertEquals(Arrays.asList(0.7f), added.getScores());
        assertNotNull(added.toString());
    }

    @Test
    void toStringContainsFields() {
        SearchResp resp = SearchResp.builder().build();
        assertNotNull(resp.toString());
        assertNotNull(SearchResult.builder().id(1L).build().toString());
    }
}
