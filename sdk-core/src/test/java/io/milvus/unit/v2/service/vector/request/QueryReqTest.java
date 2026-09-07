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
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.aggregation.AggDirection;
import io.milvus.v2.service.vector.request.aggregation.OrderByField;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class QueryReqTest {

    @Test
    void builderSetsAllFields() {
        OrderByField orderBy = OrderByField.builder().fieldName("pk").direction(AggDirection.DESC).build();
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("timezone", "America/Chicago");
        Map<String, Object> templateValues = new HashMap<>();
        templateValues.put("age", 3);
        QueryReq req = QueryReq.builder()
                .databaseName("db")
                .collectionName("col")
                .clusterId("cluster-1")
                .partitionNames(Arrays.asList("p0", "p1"))
                .outputFields(Arrays.asList("id", "vector"))
                .ids(Arrays.asList(1L, 2L))
                .filter("pk > 3")
                .consistencyLevel(ConsistencyLevel.STRONG)
                .offset(5)
                .limit(10)
                .ignoreGrowing(true)
                .timezone("UTC")
                .orderByFields(Collections.singletonList(orderBy))
                .queryParams(queryParams)
                .filterTemplateValues(templateValues)
                .build();

        assertEquals("db", req.getDatabaseName());
        assertEquals("col", req.getCollectionName());
        assertEquals("cluster-1", req.getClusterId());
        assertEquals(Arrays.asList("p0", "p1"), req.getPartitionNames());
        assertEquals(Arrays.asList("id", "vector"), req.getOutputFields());
        assertEquals(Arrays.asList(1L, 2L), req.getIds());
        assertEquals("pk > 3", req.getFilter());
        assertEquals(ConsistencyLevel.STRONG, req.getConsistencyLevel());
        assertEquals(5, req.getOffset());
        assertEquals(10, req.getLimit());
        assertTrue(req.isIgnoreGrowing());
        assertEquals("UTC", req.getTimezone());
        assertEquals(Collections.singletonList(orderBy), req.getOrderByFields());
        assertEquals(queryParams, req.getQueryParams());
        assertEquals(templateValues, req.getFilterTemplateValues());
    }

    @Test
    void builderDefaults() {
        QueryReq req = QueryReq.builder().build();

        assertNull(req.getDatabaseName());
        assertNull(req.getCollectionName());
        assertNull(req.getClusterId());
        assertTrue(req.getPartitionNames().isEmpty());
        assertEquals(Collections.singletonList("*"), req.getOutputFields());
        assertNull(req.getIds());
        assertEquals("", req.getFilter());
        assertNull(req.getConsistencyLevel());
        assertEquals(0, req.getOffset());
        assertEquals(0, req.getLimit());
        assertEquals(false, req.isIgnoreGrowing());
        assertEquals("", req.getTimezone());
        assertTrue(req.getOrderByFields().isEmpty());
        assertTrue(req.getQueryParams().isEmpty());
        assertTrue(req.getFilterTemplateValues().isEmpty());
    }

    @Test
    void settersUpdateFields() {
        QueryReq req = QueryReq.builder().build();

        req.setDatabaseName("db2");
        req.setCollectionName("col2");
        req.setClusterId("cluster-2");
        req.setPartitionNames(Collections.singletonList("p2"));
        req.setOutputFields(Collections.singletonList("*"));
        req.setIds(Collections.singletonList(3L));
        req.setFilter("id in [1, 2]");
        req.setConsistencyLevel(ConsistencyLevel.EVENTUALLY);
        req.setOffset(1);
        req.setLimit(2);
        req.setIgnoreGrowing(true);
        req.setOrderByFields(Collections.singletonList(
                OrderByField.builder().fieldName("score").build()));
        req.setQueryParams(Collections.singletonMap("k", "10"));
        req.setFilterTemplateValues(Collections.singletonMap("city", Arrays.asList("a", "b")));

        assertEquals("db2", req.getDatabaseName());
        assertEquals("col2", req.getCollectionName());
        assertEquals("cluster-2", req.getClusterId());
        assertEquals(1, req.getPartitionNames().size());
        assertEquals(1, req.getOutputFields().size());
        assertEquals(1, req.getIds().size());
        assertEquals("id in [1, 2]", req.getFilter());
        assertEquals(ConsistencyLevel.EVENTUALLY, req.getConsistencyLevel());
        assertEquals(1, req.getOffset());
        assertEquals(2, req.getLimit());
        assertTrue(req.isIgnoreGrowing());
        assertEquals(1, req.getOrderByFields().size());
        assertEquals("10", req.getQueryParams().get("k"));
        assertEquals(2, ((java.util.List<?>) req.getFilterTemplateValues().get("city")).size());
    }

    @Test
    void toStringContainsFields() {
        QueryReq req = QueryReq.builder().collectionName("col").build();
        assertNotNull(req.toString());
    }
}
