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

import io.milvus.param.Constant;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.SearchIteratorReqV2;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.SearchResp;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class SearchIteratorReqV2Test {

    @Test
    void builderSetsAllFields() {
        FloatVec vec = new FloatVec(Arrays.asList(1.0f, 2.0f));
        Map<String, Object> searchParams = new HashMap<>();
        searchParams.put("nprobe", 10);
        Map<String, Object> templateValues = new HashMap<>();
        templateValues.put("age", 3);
        Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> filterFunc = list ->
                list.stream().filter(r -> r.getScore() > 0.5f).collect(Collectors.toList());

        SearchIteratorReqV2 req = SearchIteratorReqV2.builder()
                .databaseName("db")
                .collectionName("col")
                .clusterId("cluster-1")
                .partitionNames(Arrays.asList("p0", "p1"))
                .metricType(IndexParam.MetricType.COSINE)
                .vectorFieldName("vector")
                .limit(10)
                .filter("pk > 3")
                .outputFields(Arrays.asList("id", "vector"))
                .vectors(Collections.singletonList(vec))
                .roundDecimal(4)
                .searchParams(searchParams)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .ignoreGrowing(true)
                .timezone("UTC")
                .groupByFieldName("color")
                .batchSize(200)
                .externalFilterFunc(filterFunc)
                .filterTemplateValues(templateValues)
                .build();

        assertEquals("db", req.getDatabaseName());
        assertEquals("col", req.getCollectionName());
        assertEquals("cluster-1", req.getClusterId());
        assertEquals(Arrays.asList("p0", "p1"), req.getPartitionNames());
        assertEquals(IndexParam.MetricType.COSINE, req.getMetricType());
        assertEquals("vector", req.getVectorFieldName());
        assertEquals(10, req.getLimit());
        assertEquals(10, req.getTopK());
        assertEquals("pk > 3", req.getFilter());
        assertEquals(Arrays.asList("id", "vector"), req.getOutputFields());
        assertEquals(Collections.singletonList(vec), req.getVectors());
        assertEquals(4, req.getRoundDecimal());
        assertEquals(searchParams, req.getSearchParams());
        assertEquals(ConsistencyLevel.STRONG, req.getConsistencyLevel());
        assertTrue(req.isIgnoreGrowing());
        assertEquals("UTC", req.getTimezone());
        assertEquals("color", req.getGroupByFieldName());
        assertEquals(200, req.getBatchSize());
        assertEquals(filterFunc, req.getExternalFilterFunc());
        assertEquals(templateValues, req.getFilterTemplateValues());
    }

    @Test
    void builderDefaults() {
        SearchIteratorReqV2 req = SearchIteratorReqV2.builder().build();

        assertNull(req.getDatabaseName());
        assertNull(req.getCollectionName());
        assertNull(req.getClusterId());
        assertTrue(req.getPartitionNames().isEmpty());
        assertEquals(IndexParam.MetricType.INVALID, req.getMetricType());
        assertNull(req.getVectorFieldName());
        assertEquals(Constant.UNLIMITED, req.getTopK());
        assertEquals(Constant.UNLIMITED_L, req.getLimit());
        assertEquals("", req.getFilter());
        assertTrue(req.getOutputFields().isEmpty());
        assertTrue(req.getVectors().isEmpty());
        assertEquals(-1, req.getRoundDecimal());
        assertTrue(req.getSearchParams().isEmpty());
        assertNull(req.getConsistencyLevel());
        assertEquals(false, req.isIgnoreGrowing());
        assertEquals("", req.getTimezone());
        assertEquals("", req.getGroupByFieldName());
        assertEquals(1000, req.getBatchSize());
        assertNull(req.getExternalFilterFunc());
        assertTrue(req.getFilterTemplateValues().isEmpty());
    }

    @Test
    void topKAndLimitRemainInSync() {
        SearchIteratorReqV2 req = SearchIteratorReqV2.builder().topK(5).build();
        assertEquals(5, req.getTopK());
        assertEquals(5L, req.getLimit());

        req.setTopK(7);
        assertEquals(7, req.getTopK());
        assertEquals(7L, req.getLimit());

        req.setLimit(9);
        assertEquals(9, req.getLimit());
        assertEquals(9, req.getTopK());
    }

    @Test
    void settersUpdateFields() {
        SearchIteratorReqV2 req = SearchIteratorReqV2.builder().build();

        req.setDatabaseName("db2");
        req.setCollectionName("col2");
        req.setClusterId("cluster-2");
        req.setPartitionNames(Collections.singletonList("p2"));
        req.setMetricType(IndexParam.MetricType.IP);
        req.setVectorFieldName("vector2");
        req.setFilter("id in [1]");
        req.setOutputFields(Collections.singletonList("id"));
        req.setVectors(Collections.singletonList(new FloatVec(new float[]{1f})));
        req.setRoundDecimal(2);
        req.setSearchParams(Collections.singletonMap("nprobe", 1));
        req.setConsistencyLevel(ConsistencyLevel.EVENTUALLY);
        req.setIgnoreGrowing(true);
        req.setGroupByFieldName("g");
        req.setBatchSize(50);
        req.setExternalFilterFunc(list -> list);

        assertEquals("db2", req.getDatabaseName());
        assertEquals("col2", req.getCollectionName());
        assertEquals("cluster-2", req.getClusterId());
        assertEquals(1, req.getPartitionNames().size());
        assertEquals(IndexParam.MetricType.IP, req.getMetricType());
        assertEquals("vector2", req.getVectorFieldName());
        assertEquals("id in [1]", req.getFilter());
        assertEquals(1, req.getOutputFields().size());
        assertEquals(1, req.getVectors().size());
        assertEquals(2, req.getRoundDecimal());
        assertEquals(1, req.getSearchParams().get("nprobe"));
        assertEquals(ConsistencyLevel.EVENTUALLY, req.getConsistencyLevel());
        assertTrue(req.isIgnoreGrowing());
        assertEquals("g", req.getGroupByFieldName());
        assertEquals(50, req.getBatchSize());
        assertNotNull(req.getExternalFilterFunc());
    }

    @Test
    void toStringContainsFields() {
        SearchIteratorReqV2 req = SearchIteratorReqV2.builder().collectionName("col").build();
        assertNotNull(req.toString());
    }
}
