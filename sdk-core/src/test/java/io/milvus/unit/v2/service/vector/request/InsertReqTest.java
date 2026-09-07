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

import com.google.gson.JsonObject;
import io.milvus.v2.service.vector.request.InsertReq;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class InsertReqTest {

    @Test
    void builderSetsAllFields() {
        List<JsonObject> data = Collections.singletonList(new JsonObject());
        InsertReq req = InsertReq.builder()
                .data(data)
                .databaseName("db")
                .collectionName("col")
                .partitionName("p0")
                .build();

        assertEquals(data, req.getData());
        assertEquals("db", req.getDatabaseName());
        assertEquals("col", req.getCollectionName());
        assertEquals("p0", req.getPartitionName());
    }

    @Test
    void builderDefaults() {
        InsertReq req = InsertReq.builder().build();

        assertNull(req.getData());
        assertEquals("", req.getDatabaseName());
        assertNull(req.getCollectionName());
        assertEquals("", req.getPartitionName());
    }

    @Test
    void settersUpdateFields() {
        InsertReq req = InsertReq.builder().build();

        JsonObject row = new JsonObject();
        row.addProperty("id", 1);
        req.setData(Collections.singletonList(row));
        req.setDatabaseName("db2");
        req.setCollectionName("col2");
        req.setPartitionName("p2");

        assertEquals(1, req.getData().size());
        assertEquals(1, req.getData().get(0).get("id").getAsInt());
        assertEquals("db2", req.getDatabaseName());
        assertEquals("col2", req.getCollectionName());
        assertEquals("p2", req.getPartitionName());
    }

    @Test
    void toStringContainsFields() {
        InsertReq req = InsertReq.builder().collectionName("col").build();
        assertNotNull(req.toString());
    }
}
