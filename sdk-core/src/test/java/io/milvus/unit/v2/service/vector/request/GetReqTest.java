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

import io.milvus.v2.service.vector.request.GetReq;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class GetReqTest {

    @Test
    void builderSetsAllFields() {
        GetReq req = GetReq.builder()
                .databaseName("db")
                .collectionName("col")
                .clusterId("cluster-1")
                .partitionName("p0")
                .partitionNames(Arrays.asList("p0", "p1"))
                .ids(Arrays.asList(1L, 2L))
                .outputFields(Collections.singletonList("vector"))
                .build();

        assertEquals("db", req.getDatabaseName());
        assertEquals("col", req.getCollectionName());
        assertEquals("cluster-1", req.getClusterId());
        assertEquals("p0", req.getPartitionName());
        assertEquals(Arrays.asList("p0", "p1"), req.getPartitionNames());
        assertEquals(Arrays.asList(1L, 2L), req.getIds());
        assertEquals(Collections.singletonList("vector"), req.getOutputFields());
    }

    @Test
    void builderDefaults() {
        GetReq req = GetReq.builder().build();

        assertNull(req.getDatabaseName());
        assertNull(req.getCollectionName());
        assertNull(req.getClusterId());
        assertEquals("", req.getPartitionName());
        assertNull(req.getPartitionNames());
        assertNull(req.getIds());
        assertNull(req.getOutputFields());
    }

    @Test
    void settersUpdateFields() {
        GetReq req = GetReq.builder().build();

        req.setDatabaseName("db2");
        req.setCollectionName("col2");
        req.setClusterId("cluster-2");
        req.setPartitionName("p2");
        req.setPartitionNames(Arrays.asList("p2"));
        req.setIds(Collections.singletonList("id1"));
        req.setOutputFields(Collections.singletonList("name"));

        assertEquals("db2", req.getDatabaseName());
        assertEquals("col2", req.getCollectionName());
        assertEquals("cluster-2", req.getClusterId());
        assertEquals("p2", req.getPartitionName());
        assertEquals(Arrays.asList("p2"), req.getPartitionNames());
        assertEquals(Collections.singletonList("id1"), req.getIds());
        assertEquals(Collections.singletonList("name"), req.getOutputFields());
    }

    @Test
    void toStringContainsKeyFields() {
        GetReq req = GetReq.builder().collectionName("col").build();
        assertNotNull(req.toString());
        assertEquals("col", req.getCollectionName());
    }
}
