/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file
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

package io.milvus.unit.v2.service.snapshot;

import io.milvus.v2.service.snapshot.request.CreateSnapshotReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class CreateSnapshotReqTest {
    @Test
    void builderBuildsAllFields() {
        CreateSnapshotReq request = CreateSnapshotReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .snapshotName("snap")
                .description("desc")
                .compactionProtectionSeconds(3600L)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals("snap", request.getSnapshotName());
        assertEquals("desc", request.getDescription());
        assertEquals(Long.valueOf(3600L), request.getCompactionProtectionSeconds());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        CreateSnapshotReq request = CreateSnapshotReq.builder().build();

        assertEquals("", request.getDatabaseName());
        assertNull(request.getCollectionName());
        assertNull(request.getSnapshotName());
        assertEquals("", request.getDescription());
        assertEquals(Long.valueOf(0L), request.getCompactionProtectionSeconds());
    }

    @Test
    void settersUpdateFields() {
        CreateSnapshotReq request = CreateSnapshotReq.builder().build();

        request.setDatabaseName("db");
        request.setCollectionName("coll");
        request.setSnapshotName("snap");
        request.setDescription("desc");
        request.setCompactionProtectionSeconds(7200L);

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals("snap", request.getSnapshotName());
        assertEquals("desc", request.getDescription());
        assertEquals(Long.valueOf(7200L), request.getCompactionProtectionSeconds());
    }

    @Test
    void compactionProtectionSecondsAcceptsNegativeValue() {
        CreateSnapshotReq request = CreateSnapshotReq.builder()
                .compactionProtectionSeconds(-1L)
                .build();

        assertEquals(Long.valueOf(-1L), request.getCompactionProtectionSeconds());
    }

    @Test
    void toStringContainsFields() {
        CreateSnapshotReq request = CreateSnapshotReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .snapshotName("snap")
                .description("desc")
                .compactionProtectionSeconds(1L)
                .build();

        String text = request.toString();
        assertEquals("CreateSnapshotReq{databaseName='db', collectionName='coll', snapshotName='snap', description='desc', compactionProtectionSeconds=1}", text);
    }
}
