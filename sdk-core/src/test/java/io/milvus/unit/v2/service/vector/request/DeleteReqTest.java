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
import io.milvus.v2.service.vector.request.DeleteReq;

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
class DeleteReqTest {

    @Test
    void builderSetsAllFields() {
        Map<String, Object> templateValues = new HashMap<>();
        templateValues.put("age", 3);
        DeleteReq req = DeleteReq.builder()
                .databaseName("db")
                .collectionName("col")
                .partitionName("p0")
                .filter("pk > {age}")
                .ids(Arrays.asList(1L, 2L))
                .filterTemplateValues(templateValues)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build();

        assertEquals("db", req.getDatabaseName());
        assertEquals("col", req.getCollectionName());
        assertEquals("p0", req.getPartitionName());
        assertEquals("pk > {age}", req.getFilter());
        assertEquals(Arrays.asList(1L, 2L), req.getIds());
        assertEquals(templateValues, req.getFilterTemplateValues());
        assertEquals(ConsistencyLevel.STRONG, req.getConsistencyLevel());
    }

    @Test
    void builderDefaults() {
        DeleteReq req = DeleteReq.builder().build();

        assertEquals("", req.getDatabaseName());
        assertNull(req.getCollectionName());
        assertEquals("", req.getPartitionName());
        assertNull(req.getFilter());
        assertNull(req.getIds());
        assertNotNull(req.getFilterTemplateValues());
        assertTrue(req.getFilterTemplateValues().isEmpty());
        assertNull(req.getConsistencyLevel());
    }

    @Test
    void settersUpdateFields() {
        DeleteReq req = DeleteReq.builder().build();

        req.setDatabaseName("db2");
        req.setCollectionName("col2");
        req.setPartitionName("p2");
        req.setFilter("pk == 42");
        req.setIds(Collections.singletonList(42L));
        req.setFilterTemplateValues(Collections.singletonMap("age", 3L));
        req.setConsistencyLevel(ConsistencyLevel.BOUNDED);

        assertEquals("db2", req.getDatabaseName());
        assertEquals("col2", req.getCollectionName());
        assertEquals("p2", req.getPartitionName());
        assertEquals("pk == 42", req.getFilter());
        assertEquals(Collections.singletonList(42L), req.getIds());
        assertEquals(3L, req.getFilterTemplateValues().get("age"));
        assertEquals(ConsistencyLevel.BOUNDED, req.getConsistencyLevel());
    }

    @Test
    void toStringContainsFields() {
        DeleteReq req = DeleteReq.builder().collectionName("col").build();
        assertNotNull(req.toString());
    }
}
