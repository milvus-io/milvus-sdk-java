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

package io.milvus.unit.v2.service.partition;

import io.milvus.v2.service.partition.request.ReleasePartitionsReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class ReleasePartitionsReqTest {
    @Test
    void builderBuildsAllFields() {
        List<String> partitionNames = Arrays.asList("p1", "p2");

        ReleasePartitionsReq request = ReleasePartitionsReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .partitionNames(partitionNames)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals(partitionNames, request.getPartitionNames());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        ReleasePartitionsReq request = ReleasePartitionsReq.builder().build();

        assertNull(request.getDatabaseName());
        assertNull(request.getCollectionName());
        assertNull(request.getPartitionNames());
    }

    @Test
    void settersUpdateFields() {
        ReleasePartitionsReq request = ReleasePartitionsReq.builder().build();

        request.setDatabaseName("db");
        request.setCollectionName("coll");
        request.setPartitionNames(Arrays.asList("p1"));

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals(Arrays.asList("p1"), request.getPartitionNames());
    }

    @Test
    void toStringContainsFields() {
        ReleasePartitionsReq request = ReleasePartitionsReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .partitionNames(Arrays.asList("p1"))
                .build();

        assertEquals("ReleasePartitionsReq{databaseName='db', collectionName='coll', partitionNames=[p1]}",
                request.toString());
    }
}
