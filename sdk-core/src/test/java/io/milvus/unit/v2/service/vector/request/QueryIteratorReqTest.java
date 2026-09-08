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

import io.milvus.orm.iterator.QueryIteratorCursor;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.vector.request.QueryIteratorReq;

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
class QueryIteratorReqTest {

    @Test
    void builderSetsAllFields() {
        QueryIteratorCursor cursor = QueryIteratorCursor.builder()
                .sessionTs(123L)
                .intPk(99L)
                .build();
        Map<String, Object> templateValues = new HashMap<>();
        templateValues.put("age", 3);
        QueryIteratorReq req = QueryIteratorReq.builder()
                .databaseName("db")
                .collectionName("col")
                .clusterId("cluster-1")
                .partitionNames(Arrays.asList("p0", "p1"))
                .outputFields(Arrays.asList("id", "vector"))
                .expr("pk > 3")
                .consistencyLevel(ConsistencyLevel.STRONG)
                .offset(5)
                .limit(100)
                .ignoreGrowing(true)
                .timezone("UTC")
                .batchSize(500)
                .reduceStopForBest(false)
                .filterTemplateValues(templateValues)
                .cursor(cursor)
                .build();

        assertEquals("db", req.getDatabaseName());
        assertEquals("col", req.getCollectionName());
        assertEquals("cluster-1", req.getClusterId());
        assertEquals(Arrays.asList("p0", "p1"), req.getPartitionNames());
        assertEquals(Arrays.asList("id", "vector"), req.getOutputFields());
        assertEquals("pk > 3", req.getExpr());
        assertEquals(ConsistencyLevel.STRONG, req.getConsistencyLevel());
        assertEquals(5, req.getOffset());
        assertEquals(100, req.getLimit());
        assertTrue(req.isIgnoreGrowing());
        assertEquals("UTC", req.getTimezone());
        assertEquals(500, req.getBatchSize());
        assertEquals(false, req.isReduceStopForBest());
        assertEquals(templateValues, req.getFilterTemplateValues());
        assertEquals(cursor, req.getCursor());
    }

    @Test
    void builderDefaults() {
        QueryIteratorReq req = QueryIteratorReq.builder().build();

        assertNull(req.getDatabaseName());
        assertNull(req.getCollectionName());
        assertNull(req.getClusterId());
        assertTrue(req.getPartitionNames().isEmpty());
        assertTrue(req.getOutputFields().isEmpty());
        assertEquals("", req.getExpr());
        assertNull(req.getConsistencyLevel());
        assertEquals(0, req.getOffset());
        assertEquals(-1, req.getLimit());
        assertEquals(false, req.isIgnoreGrowing());
        assertEquals("", req.getTimezone());
        assertEquals(1000, req.getBatchSize());
        assertEquals(true, req.isReduceStopForBest());
        assertTrue(req.getFilterTemplateValues().isEmpty());
        assertNull(req.getCursor());
    }

    @Test
    void settersUpdateFields() {
        QueryIteratorReq req = QueryIteratorReq.builder().build();

        req.setDatabaseName("db2");
        req.setCollectionName("col2");
        req.setClusterId("cluster-2");
        req.setPartitionNames(Collections.singletonList("p2"));
        req.setOutputFields(Collections.singletonList("*"));
        req.setExpr("id in [1, 2]");
        req.setConsistencyLevel(ConsistencyLevel.EVENTUALLY);
        req.setOffset(1);
        req.setLimit(2);
        req.setIgnoreGrowing(true);
        req.setBatchSize(50);
        req.setReduceStopForBest(false);
        req.setCursor(QueryIteratorCursor.builder().sessionTs(1L).build());

        assertEquals("db2", req.getDatabaseName());
        assertEquals("col2", req.getCollectionName());
        assertEquals("cluster-2", req.getClusterId());
        assertEquals(1, req.getPartitionNames().size());
        assertEquals(1, req.getOutputFields().size());
        assertEquals("id in [1, 2]", req.getExpr());
        assertEquals(ConsistencyLevel.EVENTUALLY, req.getConsistencyLevel());
        assertEquals(1, req.getOffset());
        assertEquals(2, req.getLimit());
        assertTrue(req.isIgnoreGrowing());
        assertEquals(50, req.getBatchSize());
        assertEquals(false, req.isReduceStopForBest());
        assertNotNull(req.getCursor());
        assertEquals(1L, req.getCursor().getSessionTs());
    }

    @Test
    void toStringContainsFields() {
        QueryIteratorReq req = QueryIteratorReq.builder().collectionName("col").build();
        assertNotNull(req.toString());
    }
}
