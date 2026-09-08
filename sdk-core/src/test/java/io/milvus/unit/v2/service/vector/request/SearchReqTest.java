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

import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.FunctionChain;
import io.milvus.v2.service.vector.request.FunctionScore;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.SearchReq.SearchReqBuilder;
import io.milvus.v2.service.vector.request.aggregation.AggDirection;
import io.milvus.v2.service.vector.request.aggregation.OrderByField;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.highlighter.LexicalHighlighter;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class SearchReqTest {

    @Test
    void builderSetsAllFields() {
        FloatVec vec = new FloatVec(Arrays.asList(1.0f, 2.0f));
        OrderByField orderBy = OrderByField.builder().fieldName("pk").direction(AggDirection.DESC).build();
        Map<String, Object> searchParams = new HashMap<>();
        searchParams.put("nprobe", 10);
        Map<String, Object> templateValues = new HashMap<>();
        templateValues.put("age", 3);
        CreateCollectionReq.Function ranker = CreateCollectionReq.Function.builder()
                .name("rank")
                .description("desc")
                .build();
        FunctionScore functionScore = FunctionScore.builder()
                .addFunction(CreateCollectionReq.Function.builder().name("f").build())
                .build();
        FunctionChain functionChain = FunctionChain.builder().name("chain").build();
        LexicalHighlighter highlighter = LexicalHighlighter.builder().fragmentSize(10).build();

        SearchReq req = SearchReq.builder()
                .databaseName("db")
                .collectionName("col")
                .clusterId("cluster-1")
                .partitionNames(Arrays.asList("p0", "p1"))
                .annsField("vector")
                .metricType(IndexParam.MetricType.COSINE)
                .filter("pk > 3")
                .outputFields(Arrays.asList("id", "vector"))
                .data(Collections.singletonList(vec))
                .ids(Collections.singletonList(1L))
                .offset(5)
                .limit(10)
                .roundDecimal(4)
                .searchParams(searchParams)
                .guaranteeTimestamp(100L)
                .gracefulTime(2000L)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .ignoreGrowing(true)
                .timezone("UTC")
                .orderByFields(Collections.singletonList(orderBy))
                .groupByFieldName("color")
                .groupSize(3)
                .strictGroupSize(true)
                .ranker(ranker)
                .functionScore(functionScore)
                .functionChains(Collections.singletonList(functionChain))
                .filterTemplateValues(templateValues)
                .highlighter(highlighter)
                .build();

        assertEquals("db", req.getDatabaseName());
        assertEquals("col", req.getCollectionName());
        assertEquals("cluster-1", req.getClusterId());
        assertEquals(Arrays.asList("p0", "p1"), req.getPartitionNames());
        assertEquals("vector", req.getAnnsField());
        assertEquals(IndexParam.MetricType.COSINE, req.getMetricType());
        assertEquals("pk > 3", req.getFilter());
        assertEquals(Arrays.asList("id", "vector"), req.getOutputFields());
        assertEquals(Collections.singletonList(vec), req.getData());
        assertEquals(Collections.singletonList(1L), req.getIds());
        assertEquals(5, req.getOffset());
        assertEquals(10, req.getLimit());
        assertEquals(10, req.getTopK());
        assertEquals(4, req.getRoundDecimal());
        assertEquals(searchParams, req.getSearchParams());
        assertEquals(100L, req.getGuaranteeTimestamp());
        assertEquals(2000L, req.getGracefulTime());
        assertEquals(ConsistencyLevel.STRONG, req.getConsistencyLevel());
        assertTrue(req.isIgnoreGrowing());
        assertEquals("UTC", req.getTimezone());
        assertEquals(Collections.singletonList(orderBy), req.getOrderByFields());
        assertEquals("color", req.getGroupByFieldName());
        assertEquals(3, req.getGroupSize());
        assertEquals(true, req.getStrictGroupSize());
        assertEquals(ranker, req.getRanker());
        assertEquals(functionScore, req.getFunctionScore());
        assertEquals(Collections.singletonList(functionChain), req.getFunctionChains());
        assertEquals(templateValues, req.getFilterTemplateValues());
        assertEquals(highlighter, req.getHighlighter());
    }

    @Test
    void builderDefaults() {
        SearchReq req = SearchReq.builder().build();

        assertNull(req.getDatabaseName());
        assertNull(req.getCollectionName());
        assertNull(req.getClusterId());
        assertTrue(req.getPartitionNames().isEmpty());
        assertEquals("", req.getAnnsField());
        assertNull(req.getMetricType());
        assertEquals(0, req.getTopK());
        assertNull(req.getFilter());
        assertTrue(req.getOutputFields().isEmpty());
        assertTrue(req.getData().isEmpty());
        assertTrue(req.getIds().isEmpty());
        assertEquals(0, req.getOffset());
        assertEquals(0L, req.getLimit());
        assertEquals(-1, req.getRoundDecimal());
        assertTrue(req.getSearchParams().isEmpty());
        assertEquals(0, req.getGuaranteeTimestamp());
        assertEquals(5000L, req.getGracefulTime());
        assertNull(req.getConsistencyLevel());
        assertEquals(false, req.isIgnoreGrowing());
        assertEquals("", req.getTimezone());
        assertTrue(req.getOrderByFields().isEmpty());
        assertNull(req.getGroupByFieldName());
        assertNull(req.getGroupSize());
        assertNull(req.getStrictGroupSize());
        assertNull(req.getRanker());
        assertNull(req.getFunctionScore());
        assertNull(req.getFunctionChains());
        assertTrue(req.getFilterTemplateValues().isEmpty());
        assertNull(req.getHighlighter());
        assertNull(req.getSearchAggregation());
    }

    @Test
    void topKAndLimitRemainInSync() {
        SearchReq req = SearchReq.builder().topK(8).build();
        assertEquals(8, req.getTopK());
        assertEquals(8L, req.getLimit());

        req.setTopK(12);
        assertEquals(12, req.getTopK());
        assertEquals(12L, req.getLimit());

        req.setLimit(20);
        assertEquals(20, req.getLimit());
        assertEquals(20, req.getTopK());

        SearchReq viaLimit = SearchReq.builder().limit(15).build();
        assertEquals(15, viaLimit.getLimit());
        assertEquals(15, viaLimit.getTopK());
    }

    @Test
    void settersUpdateFields() {
        SearchReq req = SearchReq.builder().build();

        req.setDatabaseName("db2");
        req.setCollectionName("col2");
        req.setClusterId("cluster-2");
        req.setPartitionNames(Collections.singletonList("p2"));
        req.setAnnsField("vector2");
        req.setMetricType(IndexParam.MetricType.IP);
        req.setFilter("id in [1]");
        req.setOutputFields(Collections.singletonList("id"));
        req.setData(Collections.singletonList(new FloatVec(new float[]{1f, 2f})));
        req.setOffset(1);
        req.setRoundDecimal(2);
        req.setSearchParams(Collections.singletonMap("nprobe", 1));
        req.setGuaranteeTimestamp(7L);
        req.setGracefulTime(8L);
        req.setConsistencyLevel(ConsistencyLevel.EVENTUALLY);
        req.setIgnoreGrowing(true);
        req.setOrderByFields(Collections.singletonList(OrderByField.builder().fieldName("id").build()));
        req.setGroupByFieldName("g");
        req.setGroupSize(2);
        req.setStrictGroupSize(false);
        req.setRanker(CreateCollectionReq.Function.builder().name("r").build());
        req.setFunctionScore(FunctionScore.builder().build());
        req.setFunctionChains(Collections.singletonList(FunctionChain.builder().build()));
        req.setFilterTemplateValues(Collections.singletonMap("city", Arrays.asList("a")));
        req.setSearchAggregation(null);

        assertEquals("db2", req.getDatabaseName());
        assertEquals("col2", req.getCollectionName());
        assertEquals("cluster-2", req.getClusterId());
        assertEquals(1, req.getPartitionNames().size());
        assertEquals("vector2", req.getAnnsField());
        assertEquals(IndexParam.MetricType.IP, req.getMetricType());
        assertEquals("id in [1]", req.getFilter());
        assertEquals(1, req.getOutputFields().size());
        assertEquals(1, req.getData().size());
        assertEquals(1, req.getOffset());
        assertEquals(2, req.getRoundDecimal());
        assertEquals(1, req.getSearchParams().get("nprobe"));
        assertEquals(7L, req.getGuaranteeTimestamp());
        assertEquals(8L, req.getGracefulTime());
        assertEquals(ConsistencyLevel.EVENTUALLY, req.getConsistencyLevel());
        assertTrue(req.isIgnoreGrowing());
        assertEquals(1, req.getOrderByFields().size());
        assertEquals("g", req.getGroupByFieldName());
        assertEquals(2, req.getGroupSize());
        assertEquals(false, req.getStrictGroupSize());
        assertNotNull(req.getRanker());
        assertNotNull(req.getFunctionScore());
        assertEquals(1, req.getFunctionChains().size());
        assertEquals(1, req.getFilterTemplateValues().size());
    }

    @Test
    void addFunctionChainBuildsListAndRejectsNull() {
        SearchReqBuilder builder = SearchReq.builder();
        FunctionChain chain = FunctionChain.builder().name("c1").build();
        builder.addFunctionChain(chain);
        builder.addFunctionChain(FunctionChain.builder().name("c2").build());

        SearchReq req = builder.build();
        assertEquals(2, req.getFunctionChains().size());
        assertEquals("c1", req.getFunctionChains().get(0).getName());

        assertThrows(MilvusClientException.class,
                () -> SearchReq.builder().addFunctionChain(null));
    }

    @Test
    void toStringContainsFields() {
        SearchReq req = SearchReq.builder().collectionName("col").build();
        assertNotNull(req.toString());
    }
}
